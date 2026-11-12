# PostgreSQL Failover Lab

## CDC subscriber recovery

**PostgreSQL 18.6 单次配对实验**。两组主库都恢复了写入，客户端确认过的事务在新主库上全部查回。下游表现如下，时间从请求删除旧主库 Pod 开始计。

| 逻辑槽 | 主库确认新写入的时间上界 | 下游观察到切主后写入的时间上界 | 下游查回的确认事务 |
|---|---:|---:|---:|
| [普通槽](examples/cdc-ordinary.json) | 23.464 s | 到 113.933 s 仍未观察到 | 3/4 |
| [已同步的 failover slot](examples/cdc-failover.json) | 22.932 s | 28.329 s | 6/6 |

普通槽组在新主库上找不到原槽，订阅仍显示 enabled，下游未收到序号 33。另一组在故障前确认两台备库的槽均已就绪，切换后下游自动追平。[两个作业和原始记录](https://github.com/bfsj/pg-failover-lab/actions/runs/43160032142)

This experiment gives the old primary Pod five seconds of termination grace in both arms. With the default grace period and an active subscriber, the original test observed the old primary still terminating at the 180-second deadline. [Initial run](https://github.com/bfsj/pg-failover-lab/actions/runs/43159209406)

On a fresh kind lab, run `bash setup.sh async` and then `python cdc.py failover --output results-cdc`. Repeat on a newly created cluster with `ordinary` to run the control. Both arms enable the same publisher settings and let a separate PostgreSQL 18 subscriber follow the lab-rw Service. Only the subscription's failover flag differs. The test verifies delivery before injection and checks logical slot readiness on both eligible standbys.

After deleting the primary Pod, the verifier compares downstream rows with the client journal. The report measures the upper bound until a write started after promotion appears downstream. Subscriber polling starts after the publisher verifier finishes. An ordinary slot that disappears on promotion is recorded as an observed interruption, with the final polling time and missing sequence numbers. Use the [CDC workflow](https://github.com/bfsj/pg-failover-lab/actions/workflows/cdc.yml) to reproduce both arms.

Measure what happens to confirmed and ambiguous writes when a PostgreSQL primary Pod is deleted. A Python client drives an isolated three-instance [CloudNativePG](https://github.com/cloudnative-pg/cloudnative-pg) cluster through its read/write Service. Each run has a unique ID and an append-only client journal.

[CI runs and downloadable drill reports](https://github.com/bfsj/pg-failover-lab/actions/workflows/drill.yml)

## 首次真实演练

在同一配置的两套单节点 kind 集群中删除主库 Pod，比较同步和异步复制。表中为观察到新主库确认写入的时间上界，包含客户端采样开销。每种模式各跑一次。

| 模式 | 恢复写入上界 | 客户端确认写入 | 恢复后缺失的确认写入 | 结果未知且恢复后未查到 |
|---|---:|---:|---:|---:|
| [异步](examples/async.json) | 4.554 s | 5 | 0 | 3 |
| [同步](examples/sync.json) | 5.542 s | 4 | 0 | 2 |

[原始演练记录](https://github.com/bfsj/pg-failover-lab/actions/runs/43040624513) · [在 Agent 项目中回放这次主库变更](https://github.com/bfsj/kube-incident-replay/blob/main/examples/pg-failover.json)

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
