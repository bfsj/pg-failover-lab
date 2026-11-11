# PostgreSQL Failover Lab

Measure what happens to confirmed and ambiguous writes when a PostgreSQL primary Pod is deleted. A Python client drives an isolated three-instance [CloudNativePG](https://github.com/cloudnative-pg/cloudnative-pg) cluster through its read/write Service. Each run has a unique ID and an append-only client journal.

[CI runs and downloadable drill reports](https://github.com/bfsj/pg-failover-lab/actions/workflows/drill.yml)

## Reproduce

On Linux or macOS with Docker, kind, kubectl, Bash and Python 3.10+:

```sh
bash setup.sh async
python3 drill.py --output results-async
kind delete cluster --name pg-lab
bash setup.sh sync
python3 drill.py --output results-sync
kind delete cluster --name pg-lab
```

The setup creates a fresh kind cluster named pg-lab. The drill targets only the kind-pg-lab context, namespace pg-lab and Cluster lab. Setup waits for two streaming standbys. Sync mode requires a remote acknowledgment from one standby. To run just the transaction-accounting test, use `python -m unittest -v`.

## Reading an experiment

- `attempts.jsonl` records monotonic start/end times, sequence numbers and the psql outcome. A completed autocommit INSERT counts as acknowledged. A failed call is conservatively recorded as unknown; querying the final database resolves presence or absence.
- `report.json` identifies the old and new primary, every missing confirmed write and both sets of unknown outcomes. Missing confirmed writes fail the CI job. Each storage lookup is scoped to this run.
- Recovery time measures the interval from issuing the deletion to a confirmed write **started after observing a changed primary**. Polling, kubectl exec and client startup contribute to this upper bound. It is not the exact service outage.
- The experiment exercises graceful Pod deletion on one kind node, with persistent storage. Async replication can finish a run with zero loss. Repeat on fresh clusters before comparing distributions; a single clean run cannot establish an RPO guarantee.

## Maintenance

Changes to reconciliation should include a journal example and a regression test. For operator upgrades run both CI modes and compare the report schema. When filing an issue, include the mode, workflow URL, operator version and sanitized report. The upstream operator handles replication and election. This repository owns the workload, injection, journal and verifier.
