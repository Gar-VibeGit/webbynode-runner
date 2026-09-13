from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
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
        self.assertIn('IPERF_BLOCK_BYTES="100000"', self.source)
        self.assertIn('-l "$IPERF_BLOCK_BYTES"', self.function)
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
            self.function.index('-l "$IPERF_BLOCK_BYTES"'),
            self.function.index('if [[ "$reverse_flag" == "reverse" ]]'),
        )
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

    def test_fake_iperf_exact_bytes_survive_full_network_json_path(self) -> None:
        bash = self._find_bash()
        with tempfile.TemporaryDirectory() as temp_name:
            temp_dir = Path(temp_name)
            fake_bin = temp_dir / "bin"
            run_dir = temp_dir / "run"
            fake_bin.mkdir()
            run_dir.mkdir()

            command_log = temp_dir / "iperf-commands.tsv"
            fake_iperf = fake_bin / "iperf3"
            fake_iperf.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    requested_bytes=""
                    block_bytes="131072"
                    reverse="0"
                    printf '%s\\t' "$@" >> "$FAKE_IPERF_COMMAND_LOG"
                    printf '\\n' >> "$FAKE_IPERF_COMMAND_LOG"
                    while (( $# > 0 )); do
                      case "$1" in
                        -n) requested_bytes="$2"; shift 2 ;;
                        -l) block_bytes="$2"; shift 2 ;;
                        -R) reverse="1"; shift ;;
                        *) shift ;;
                      esac
                    done
                    transferred_bytes=$((
                      ((requested_bytes + block_bytes - 1) / block_bytes) * block_bytes
                    ))
                    bits_per_second=$((transferred_bytes * 8))
                    cat <<JSON
                    {
                      "start": {
                        "connected": [{"remote_host": "127.0.0.1"}],
                        "test_start": {
                          "bytes": ${requested_bytes},
                          "blksize": ${block_bytes},
                          "num_streams": 1,
                          "reverse": ${reverse}
                        }
                      },
                      "end": {
                        "sum_sent": {
                          "bytes": ${transferred_bytes},
                          "seconds": 1.0,
                          "bits_per_second": ${bits_per_second}
                        },
                        "sum_received": {
                          "bytes": ${transferred_bytes},
                          "seconds": 1.0,
                          "bits_per_second": ${bits_per_second}
                        }
                      }
                    }
                    JSON
                    """
                ),
                encoding="utf-8",
                newline="\n",
            )
            self._make_executable(fake_iperf)

            harness = temp_dir / "exercise-network-path.sh"
            harness.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -Eeuo pipefail
                    export WEBBYNODE_BENCH_SOURCE_ONLY=1
                    source "$1"
                    trap - ERR
                    fake_bin_path="$2"
                    command_log_path="$3"
                    run_dir_path="$4"
                    export PATH="/usr/bin:$PATH"
                    if command -v cygpath >/dev/null 2>&1; then
                      fake_bin_path="$(cygpath -u "$fake_bin_path")"
                      command_log_path="$(cygpath -u "$command_log_path")"
                      run_dir_path="$(cygpath -u "$run_dir_path")"
                    fi
                    export PATH="$fake_bin_path:$PATH"
                    export FAKE_IPERF_COMMAND_LOG="$command_log_path"
                    RUN_DIR="$run_dir_path"
                    python_executable_path="$5"

                    update_network_egress_counter() { return 1; }
                    log() { :; }
                    python3() { "$python_executable_path" "$@"; }

                    RUN_ID="test-region-plan-2026-09-13-001"
                    PROVIDER="Test"
                    PROVIDER_SLUG="test"
                    REGION="Region"
                    REGION_SLUG="region"
                    PLAN="Plan"
                    PLAN_SLUG="plan"
                    PRICE="1"
                    CPU_MODEL="test"
                    VCPU_COUNT="1"
                    RAM_GB="1"
                    OS_NAME="test"
                    KERNEL_VERSION="test"
                    ARCH="x86_64"
                    HOSTNAME_FQDN="test-host"
                    STARTED_AT_UTC="2026-09-13T00:00:00Z"
                    FINISHED_AT_UTC="2026-09-13T00:00:01Z"
                    US_ENDPOINT="127.0.0.1"
                    EU_ENDPOINT="127.0.0.1"
                    ASIA_ENDPOINT="127.0.0.1"
                    NETWORK_EGRESS_INTERFACE="eth0"
                    NETWORK_EGRESS_MEASURED_BYTES="270000000"
                    NETWORK_APPLICATION_BYTES_SENT="270000000"
                    NETWORK_BUDGET_STATUS="within_budget"

                    RUN_LOG="$RUN_DIR/run.log"
                    RUN_MD="$RUN_DIR/run.md"
                    RAW_SYSBENCH_LOG="$RUN_DIR/raw.sysbench.log"
                    RAW_FIO_LOG="$RUN_DIR/raw.fio.log"
                    RAW_IPERF_US_OUT_LOG="$RUN_DIR/raw.iperf-us-east-outbound.log"
                    RAW_IPERF_US_IN_LOG="$RUN_DIR/raw.iperf-us-east-inbound.log"
                    RAW_IPERF_EU_OUT_LOG="$RUN_DIR/raw.iperf-eu-west-outbound.log"
                    RAW_IPERF_EU_IN_LOG="$RUN_DIR/raw.iperf-eu-west-inbound.log"
                    RAW_IPERF_ASIA_OUT_LOG="$RUN_DIR/raw.iperf-asia-outbound.log"
                    RAW_IPERF_ASIA_IN_LOG="$RUN_DIR/raw.iperf-asia-inbound.log"
                    RUN_JSON="$RUN_DIR/run.json"
                    TELEMETRY_LOG="$RUN_DIR/telemetry.log"
                    AGENT_SIM_JSON="$RUN_DIR/agent-sim.json"
                    RAW_AGENT_SIM_LOG="$RUN_DIR/raw.agent-sim.log"

                    : > "$RUN_LOG"
                    : > "$RUN_MD"
                    : > "$RAW_SYSBENCH_LOG"
                    : > "$RAW_FIO_LOG"

                    run_iperf_with_retry "$US_ENDPOINT" "$RAW_IPERF_US_OUT_LOG" "" NET_US_OUT_STATUS NET_US_OUT_RETRIES_USED us_outbound
                    capture_iperf_metrics "$RAW_IPERF_US_OUT_LOG" US_OUT_REMOTE_HOST US_OUT_SENT_BPS US_OUT_RECEIVED_BPS
                    run_iperf_with_retry "$US_ENDPOINT" "$RAW_IPERF_US_IN_LOG" reverse NET_US_IN_STATUS NET_US_IN_RETRIES_USED us_inbound
                    capture_iperf_metrics "$RAW_IPERF_US_IN_LOG" US_IN_REMOTE_HOST US_IN_SENT_BPS US_IN_RECEIVED_BPS
                    run_iperf_with_retry "$EU_ENDPOINT" "$RAW_IPERF_EU_OUT_LOG" "" NET_EU_OUT_STATUS NET_EU_OUT_RETRIES_USED eu_outbound
                    capture_iperf_metrics "$RAW_IPERF_EU_OUT_LOG" EU_OUT_REMOTE_HOST EU_OUT_SENT_BPS EU_OUT_RECEIVED_BPS
                    run_iperf_with_retry "$EU_ENDPOINT" "$RAW_IPERF_EU_IN_LOG" reverse NET_EU_IN_STATUS NET_EU_IN_RETRIES_USED eu_inbound
                    capture_iperf_metrics "$RAW_IPERF_EU_IN_LOG" EU_IN_REMOTE_HOST EU_IN_SENT_BPS EU_IN_RECEIVED_BPS
                    run_iperf_with_retry "$ASIA_ENDPOINT" "$RAW_IPERF_ASIA_OUT_LOG" "" NET_ASIA_OUT_STATUS NET_ASIA_OUT_RETRIES_USED asia_outbound
                    capture_iperf_metrics "$RAW_IPERF_ASIA_OUT_LOG" ASIA_OUT_REMOTE_HOST ASIA_OUT_SENT_BPS ASIA_OUT_RECEIVED_BPS
                    run_iperf_with_retry "$ASIA_ENDPOINT" "$RAW_IPERF_ASIA_IN_LOG" reverse NET_ASIA_IN_STATUS NET_ASIA_IN_RETRIES_USED asia_inbound
                    capture_iperf_metrics "$RAW_IPERF_ASIA_IN_LOG" ASIA_IN_REMOTE_HOST ASIA_IN_SENT_BPS ASIA_IN_RECEIVED_BPS

                    write_artifact_provenance
                    write_run_json
                    validate_run_json
                    """
                ),
                encoding="utf-8",
                newline="\n",
            )
            self._make_executable(harness)

            completed = subprocess.run(
                [
                    bash,
                    harness.as_posix(),
                    RUNNER.as_posix(),
                    fake_bin.as_posix(),
                    command_log.as_posix(),
                    run_dir.as_posix(),
                    Path(sys.executable).as_posix(),
                ],
                cwd=RUNNER.parent,
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(
                completed.returncode,
                0,
                msg=f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}",
            )
            self.assertIn("run.json validation passed", completed.stdout)
            self.assertTrue(
                command_log.is_file(),
                msg=f"fake iperf was not invoked\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}",
            )

            commands = [
                line.rstrip("\t").split("\t")
                for line in command_log.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(commands), 6)
            for index, command in enumerate(commands):
                self.assertEqual(command[command.index("-n") + 1], "90000000")
                self.assertEqual(command[command.index("-l") + 1], "100000")
                self.assertEqual(command[command.index("-P") + 1], "1")
                self.assertEqual("-R" in command, index % 2 == 1)
                self.assertNotIn("-b", command)
                self.assertNotIn("-t", command)

            requested_bytes = 90_000_000
            block_bytes = 100_000
            self.assertEqual(requested_bytes % block_bytes, 0)
            run_data = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
            network = run_data["results"]["network"]
            for endpoint in ("us_east", "eu_west", "asia"):
                for direction in ("outbound", "inbound"):
                    measurement = network[endpoint][direction]
                    self.assertEqual(measurement["final_outcome"], "ok")
                    self.assertEqual(measurement["requested_application_bytes"], requested_bytes)
                    self.assertEqual(measurement["parallel_streams"], 1)
                    self.assertEqual(measurement["application_bytes_sent"], requested_bytes)

    @staticmethod
    def _make_executable(path: Path) -> None:
        path.chmod(path.stat().st_mode | 0o111)

    @staticmethod
    def _find_bash() -> str:
        candidates = []
        if os.name == "nt":
            candidates.extend(
                [
                    Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
                    / "Git"
                    / "bin"
                    / "bash.exe",
                    Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
                    / "Git"
                    / "usr"
                    / "bin"
                    / "bash.exe",
                ]
            )
        discovered = shutil.which("bash")
        if discovered:
            candidates.append(Path(discovered))
        for candidate in candidates:
            if candidate.is_file():
                return str(candidate)
        raise unittest.SkipTest("bash is required to exercise the runner path")


if __name__ == "__main__":
    unittest.main()
