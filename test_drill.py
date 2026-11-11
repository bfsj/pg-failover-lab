import unittest
from drill import reconcile


class ReconciliationTests(unittest.TestCase):
    def test_ack_loss_and_ambiguous_commits_are_separate(self):
        attempts = [dict(seq=i, status=s) for i, s in
                    enumerate(["ack", "ack", "unknown", "unknown"], 1)]
        self.assertEqual(reconcile(attempts, {1, 3}), {
            "ack_count": 2, "missing_ack": [2], "unknown_present": [3],
            "unknown_absent": [4],
        })
