from __future__ import annotations

import re
import unittest
from pathlib import Path


RUNNER = Path(__file__).resolve().parents[1] / "webbynode-bench"


class NetworkMethodologyV4Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = RUNNER.read_text(encoding="utf-8")
        match = re.search(
            r"run_iperf_with_retry\(\) \{(?P<body>.*?)\n\}",
            cls.source,
            flags=re.DOTALL,
        )
        if not match:
            raise AssertionError("run_iperf_with_retry function not found")
        cls.function = match.group("body")

    def test_v4_identity_and_fixed_exact_byte_payload(self) -> None:
        self.assertIn('MEASUREMENT_EPOCH="v4"', self.source)
        self.assertIn('NETWORK_PROTOCOL_VERSION="4.0"', self.source)
        self.assertIn('NETWORK_ENDPOINT_PAYLOAD_BYTES="90000000"', self.source)
        self.assertIn('-n "$NETWORK_ENDPOINT_PAYLOAD_BYTES"', self.function)

    def test_command_has_one_stream_and_no_bitrate_or_duration_cap(self) -> None:
        self.assertIn('-P "$IPERF_PARALLEL"', self.function)
        self.assertNotRegex(self.function, r"(?m)^\s*-b\s")
        self.assertNotRegex(self.function, r"(?m)^\s*-t\s")
        self.assertNotIn("IPERF_TARGET_BPS", self.source)
        self.assertNotIn("IPERF_DURATION=", self.source)

    def test_normal_and_reverse_commands_share_fixed_byte_construction(self) -> None:
        self.assertIn('if [[ "$reverse_flag" == "reverse" ]]', self.function)
        self.assertIn("cmd+=( -R )", self.function)
        self.assertLess(
            self.function.index('-n "$NETWORK_ENDPOINT_PAYLOAD_BYTES"'),
            self.function.index('if [[ "$reverse_flag" == "reverse" ]]'),
        )
        self.assertIn("cmd+=( -J )", self.function)

    def test_external_timeout_is_failure_only(self) -> None:
        self.assertIn('IPERF_SAFETY_TIMEOUT_SECONDS="120"', self.source)
        self.assertIn('safety_timed_out=1', self.function)
        self.assertIn('"failed_timeout"', self.function)
        success_branch = self.function[
            self.function.index('if [[ "$rc" == "0" ]'):self.function.index(
                'classified="$(classify_iperf_error_reason'
            )
        ]
        self.assertNotIn("safety_timed_out", success_branch)

    def test_budget_constants_and_provider_neutral_network_path_are_unchanged(self) -> None:
        for declaration in (
            'NETWORK_CLUSTER_LIMIT_BYTES="1000000000"',
            'NETWORK_RUN_LIMIT_BYTES="333333333"',
            'NETWORK_GUARD_STOP_BYTES="320000000"',
            'NETWORK_PLANNED_PAYLOAD_BYTES="270000000"',
        ):
            self.assertIn(declaration, self.source)
        self.assertIn('"guard_stop_bytes": integer(network_guard_stop_bytes)', self.source)
        network_body = re.search(
            r"run_network\(\) \{(?P<body>.*?)\n\}",
            self.source,
            flags=re.DOTALL,
        ).group("body")
        self.assertNotIn("$PROVIDER", network_body)
        self.assertNotRegex(network_body, r"exoscale|vultr|aws|azure|linode")


if __name__ == "__main__":
    unittest.main()
