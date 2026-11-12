"""Measure subscriber recovery after a primary Pod deletion in a fresh pg-lab."""
import argparse
import json
import subprocess
import time
from pathlib import Path

from drill import experiment, kubectl, primary, reconcile, sql

SCHEMA = "CREATE TABLE receipts(run_id text, seq integer, PRIMARY KEY(run_id,seq))"
READY_SLOT = ("SELECT count(*) FROM pg_replication_slots WHERE slot_name='cdc_lab' "
              "AND synced AND NOT temporary AND invalidation_reason IS NULL "
              "AND restart_lsn IS NOT NULL AND confirmed_flush_lsn IS NOT NULL")


def pg(pod, query, subscriber=False):
    args = ["exec", pod]
    if not subscriber:
        args.extend(["-c", "postgres"])
    return kubectl(*args, "--", "psql", "-XAtq", "-U", "postgres", "-d",
                   "postgres" if subscriber else "app", "-v", "ON_ERROR_STOP=1", "-c", query)


def downstream(query):
    return pg("subscriber", query, subscriber=True)


def until(description, probe, seconds):
    deadline = time.monotonic() + seconds
    last = ""
    while time.monotonic() < deadline:
        try:
            if probe():
                return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            last = str(exc.stderr or "timeout")[-500:]
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {description}. {last}")


def prepare(mode):
    # Both arms use identical publisher settings. Only subscription.failover differs.
    patch = {"spec": {"replicationSlots": {"highAvailability": {"synchronizeLogicalDecoding": True}},
                      "postgresql": {"parameters": {"hot_standby_feedback": "on",
                                      "sync_replication_slots": "on", "wal_level": "logical"}}}}
    kubectl("patch", "cluster", "lab", "--type=merge", "-p", json.dumps(patch))
    pods = json.loads(kubectl("get", "pod", "-l", "cnpg.io/cluster=lab,cnpg.io/podRole=instance", "-o", "json"))
    names = [item["metadata"]["name"] for item in pods["items"]]
    if len(names) != 3:
        raise RuntimeError("Expected three publisher instances")
    until("publisher replication settings", lambda: all(
        pg(p, "SELECT current_setting('wal_level') || '/' || current_setting('sync_replication_slots') "
              "|| '/' || current_setting('hot_standby_feedback')") == "logical/on/on"
        for p in names), 180)
    kubectl("apply", "-f", str(Path(__file__).with_name("subscriber.yaml")))
    kubectl("wait", "pod/subscriber", "--for=condition=Ready", "--timeout=180s", timeout=190)
    sql(SCHEMA)
    old = primary()
    pg(old, "ALTER ROLE app REPLICATION")
    pg(old, "CREATE PUBLICATION orders FOR TABLE public.receipts")
    downstream(SCHEMA)
    flag = "true" if mode == "failover" else "false"
    downstream("CREATE SUBSCRIPTION cdc_lab CONNECTION 'host=lab-rw port=5432 dbname=app "
               "user=app passfile=/tmp/subscriber.pgpass' PUBLICATION orders "
               f"WITH (failover={flag}, copy_data=false)")
    sql("INSERT INTO receipts VALUES ('preflight',0)")
    until("pre-fault end-to-end delivery", lambda: downstream(
        "SELECT count(*) FROM receipts WHERE run_id='preflight'") == "1", 90)
    standbys = [p for p in names if p != old]
    if mode == "failover":
        until("ready logical slots on both eligible standbys",
              lambda: all(pg(p, READY_SLOT) == "1" for p in standbys), 180)
    slots = {p: int(pg(p, READY_SLOT)) for p in standbys}
    if primary() != old or any(n != int(mode == "failover") for n in slots.values()):
        raise RuntimeError("Unexpected pre-fault topology or logical slot state")
    return slots


def measure(mode, directory, window):
    slots = prepare(mode)
    origin = time.monotonic()
    report = experiment(directory, 180, clock_origin=origin)
    journal = [json.loads(line) for line in (directory / "attempts.jsonl").read_text().splitlines()]
    ack = {r["seq"] for r in journal if r["status"] == "ack"}
    after = {r["seq"] for r in journal if r["status"] == "ack" and
             r["start_s"] >= report["observed_promotion_s"]}
    if not after:
        raise RuntimeError("Missing a confirmed post-promotion write")
    deadline = time.monotonic() + window
    first_seen = None
    while True:
        query = f"SELECT seq FROM receipts WHERE run_id='{report['run_id']}'"
        rows = {int(n) for n in downstream(query).splitlines()}
        observation_s = round(time.monotonic() - origin - report["fault_s"], 3)
        if first_seen is None and after & rows:
            first_seen = observation_s
        if ack <= rows or time.monotonic() >= deadline:
            break
        time.sleep(0.5)
    new_slot = pg(report["new_primary"], "SELECT count(*) FROM pg_replication_slots "
                  "WHERE slot_name='cdc_lab' AND invalidation_reason IS NULL")
    result = {"mode": mode, "version": pg(report["new_primary"], "SHOW server_version"),
              "pre_fault_ready_slots": slots, "usable_slot_on_new_primary": int(new_slot),
              "subscriber_enabled": downstream("SELECT subenabled FROM pg_subscription "
                                               "WHERE subname='cdc_lab'"),
              "publisher_recovery_upper_s": report["confirmed_recovery_upper_bound_s"],
              "subscriber_post_promotion_upper_s": first_seen,
              "last_observation_after_fault_s": observation_s,
              "subscriber": reconcile(journal, rows), "publisher_missing_ack": report["missing_ack"]}
    (directory / "cdc.json").write_text(json.dumps(result, indent=2), encoding="utf8")
    print(json.dumps(result))
    if report["missing_ack"]:
        raise RuntimeError("Missing publisher acknowledgments")
    if mode == "failover" and (first_seen is None or result["subscriber"]["missing_ack"] or new_slot != "1"):
        raise RuntimeError("Failover subscriber did not recover within the observation window")
    if mode == "ordinary" and (first_seen is not None or new_slot != "0"):
        raise RuntimeError("Ordinary slot control unexpectedly continued replication")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["ordinary", "failover"])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--window", type=int, default=90)
    args = parser.parse_args()
    if args.window < 1 or args.output.exists():
        parser.error("Use a positive observation window and a fresh output path")
    try:
        measure(args.mode, args.output, args.window)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(exc.stderr or str(exc)) from exc
