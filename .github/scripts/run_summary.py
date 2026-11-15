from pathlib import Path
import json
import os

report = json.loads(Path("results-cdc/cdc.json").read_text())
downstream = report["subscriber"]
observed = report["subscriber_post_promotion_upper_s"]
with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf8") as out:
    print(f"### Logical replication · {report['mode']}", file=out)
    print(f"- Publisher recovery upper bound: {report['publisher_recovery_upper_s']} s", file=out)
    progress = f"{observed} s" if observed is not None else "no new write observed by cutoff"
    print(f"- Subscriber recovery upper bound: {progress}", file=out)
    print(f"- Last observation after fault: {report['last_observation_after_fault_s']} s", file=out)
    print(f"- Missing acknowledged writes downstream: {len(downstream['missing_ack'])} "
          f"of {downstream['ack_count']}", file=out)
    print(f"- Usable slot on promoted primary: {report['usable_slot_on_new_primary']}", file=out)
