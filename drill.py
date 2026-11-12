"""Run a pod loss experiment against the dedicated local kind lab."""
import argparse
import json
import subprocess
import threading
import time
import uuid
from pathlib import Path

CONTEXT = "kind-pg-lab"
NAMESPACE = "pg-lab"


def kubectl(*args, timeout=25):
    return subprocess.run(
        ["kubectl", "--context", CONTEXT, "-n", NAMESPACE, *args],
        capture_output=True, text=True, timeout=timeout, check=True,
    ).stdout.strip()


def sql(query):
    return kubectl("exec", "client", "--", "psql", "-XAtq", "-v",
                   "ON_ERROR_STOP=1", "-c", query, timeout=12)


def primary():
    return kubectl("get", "cluster", "lab", "-o", "jsonpath={.status.currentPrimary}")


def reconcile(attempts, rows):
    ack = {a["seq"] for a in attempts if a["status"] == "ack"}
    unknown = {a["seq"] for a in attempts if a["status"] == "unknown"}
    return {
        "ack_count": len(ack), "missing_ack": sorted(ack - rows),
        "unknown_present": sorted(unknown & rows),
        "unknown_absent": sorted(unknown - rows),
    }


def experiment(directory, timeout, *, clock_origin=None):
    directory.mkdir(parents=True, exist_ok=False)
    run_id = uuid.uuid4().hex
    sql("CREATE TABLE IF NOT EXISTS receipts (run_id text, seq integer, "
        "PRIMARY KEY (run_id, seq))")
    old = primary()
    if not old:
        raise RuntimeError("Cluster does not have an elected primary")
    start = time.monotonic() if clock_origin is None else clock_origin
    elapsed = lambda: round(time.monotonic() - start, 3)
    attempts = []
    stop = threading.Event()
    primed = threading.Event()

    def write():
        with (directory / "attempts.jsonl").open("x", encoding="utf8") as journal:
            seq = 0
            while not stop.is_set():
                seq += 1
                record = {"seq": seq, "start_s": elapsed()}
                try:
                    sql(f"INSERT INTO receipts VALUES ('{run_id}', {seq})")
                    record["status"] = "ack"
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    record.update(status="unknown", error=str(exc.stderr or "timeout")[-600:])
                record["end_s"] = elapsed()
                journal.write(json.dumps(record) + "\n")
                journal.flush()
                attempts.append(record)
                if sum(r["status"] == "ack" for r in attempts[-3:]) == 3:
                    primed.set()
                stop.wait(0.2)

    writer = threading.Thread(target=write)
    writer.start()
    try:
        if not primed.wait(60):
            raise RuntimeError("No stable write baseline; see attempts.jsonl")
        deleted_at = elapsed()
        kubectl("delete", "pod", old, "--wait=false")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                new = primary()
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                new = ""
            if new and new != old:
                observed = elapsed()
                while time.monotonic() < deadline:
                    try:
                        if sql("SELECT pg_is_in_recovery()") == "f" and any(
                            r["status"] == "ack" and r["start_s"] >= observed
                            for r in list(attempts)
                        ):
                            break
                    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                        pass
                    time.sleep(0.3)
                else:
                    raise RuntimeError("New primary observed but writes did not recover")
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("No primary change observed before deadline")
    finally:
        stop.set()
        writer.join()
    stored = {int(v) for v in sql(f"SELECT seq FROM receipts WHERE run_id='{run_id}'").splitlines()}
    recovered = next(r for r in attempts if r["start_s"] >= observed and r["status"] == "ack")
    report = dict(run_id=run_id, old_primary=old, new_primary=new, fault_s=deleted_at,
                  observed_promotion_s=observed,
                  confirmed_recovery_upper_bound_s=round(recovered["end_s"] - deleted_at, 3),
                  **reconcile(attempts, stored))
    (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf8")
    print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New result directory")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    raise SystemExit(bool(experiment(args.output, args.timeout)["missing_ack"]))
