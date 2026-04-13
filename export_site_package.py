#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

EXPORT_VERSION = "2.0"
DEFAULT_METHODOLOGY_VERSION = "1.1"
DEFAULT_ALLOWED_EXPORT_STATUSES = ["ok"]

CANONICAL_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "us_east": {
        "slug": "us-east-iperf",
        "display_name": "US East iPerf Endpoint",
        "aliases": {
            "us_east",
            "us-east",
            "us-east-iperf",
            "us-east.webbynode.net",
            "us-east.webbynode.com",
            "us",
            "use1",
            "us-east-1",
        },
        "artifact_stems": [
            "us-east",
            "us_east",
            "use1",
            "us-east-iperf",
            "us-east.webbynode.net",
        ],
    },
    "eu_west": {
        "slug": "eu-west-iperf",
        "display_name": "EU West iPerf Endpoint",
        "aliases": {
            "eu_west",
            "eu-west",
            "eu-west-iperf",
            "eu-west.webbynode.net",
            "eu-west.webbynode.com",
            "eu",
            "euw1",
            "nbg",
            "nuremberg",
        },
        "artifact_stems": [
            "eu-west",
            "eu_west",
            "euw1",
            "eu-west-iperf",
            "nuremberg",
            "nbg",
            "eu-west.webbynode.net",
        ],
    },
    "asia": {
        "slug": "asia-iperf",
        "display_name": "Asia iPerf Endpoint",
        "aliases": {
            "asia",
            "asia-iperf",
            "asia.webbynode.net",
            "asia.webbynode.com",
            "asia_singapore",
            "asia-singapore",
            "asia-singapore-iperf",
            "singapore",
            "sg",
            "sgp",
            "ap-southeast",
            "ap_southeast",
            "ap-southeast-1",
            "ap-southeast-iperf",
        },
        "artifact_stems": [
            "asia",
            "asia-iperf",
            "asia_singapore",
            "asia-singapore",
            "asia-sg",
            "singapore",
            "sg",
            "sgp",
            "ap-southeast",
            "ap_southeast",
            "ap-southeast-1",
            "asia.webbynode.net",
        ],
    },
}

FIXED_PUBLIC_TOP_LEVEL_FILES = ["run.json", "run.log", "run.md"]


class ExportError(RuntimeError):
    pass


@dataclass
class Config:
    dataset_repo_root: Path
    site_repo_root: Path
    source_benchmarks_rel: Path = Path("benchmarks")
    dest_data_rel: Path = Path("src/data/benchmarks")
    dest_public_rel: Path = Path("public/benchmarks")
    default_methodology_version: str = DEFAULT_METHODOLOGY_VERSION
    allowed_export_statuses: List[str] = field(default_factory=lambda: list(DEFAULT_ALLOWED_EXPORT_STATUSES))
    dry_run: bool = False
    verbose: bool = False

    @property
    def source_benchmarks_dir(self) -> Path:
        return self.dataset_repo_root / self.source_benchmarks_rel

    @property
    def dest_data_dir(self) -> Path:
        return self.site_repo_root / self.dest_data_rel

    @property
    def dest_public_dir(self) -> Path:
        return self.site_repo_root / self.dest_public_rel


@dataclass
class Counters:
    scanned_runs: int = 0
    exportable_runs: int = 0
    skipped_runs: int = 0
    clusters_generated: int = 0
    artifacts_copied: int = 0
    artifacts_removed: int = 0
    warnings: List[str] = field(default_factory=list)


@dataclass
class NormalizedRun:
    sort_tested_at: datetime
    sort_run_id: str
    record: Dict[str, Any]
    source_dir: Path
    provider_slug: str
    run_id: str
    cluster_key: str
    public_files: List[str]


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, obj: Dict[str, Any], dry_run: bool = False) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")
    tmp_path.replace(path)


def parse_iso8601(value: Any) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def to_iso_z(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def safe_int(value: Any) -> Optional[int]:
    num = safe_float(value)
    if num is None:
        return None
    return int(num)


def nested_get(obj: Any, *path: str, default: Any = None) -> Any:
    cur = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def choose(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def gbps_from_bps(value: Any) -> Optional[float]:
    num = safe_float(value)
    if num is None:
        return None
    return round(num / 1_000_000_000.0, 4)


def gbps_from_mbps(value: Any) -> Optional[float]:
    num = safe_float(value)
    if num is None:
        return None
    return round(num / 1000.0, 4)


def avg(values: Sequence[float]) -> float:
    return round(sum(values) / len(values), 4)


def stat_block(values: List[float], prefix: str) -> Dict[str, Any]:
    if not values:
        return {}
    return {
        f"{prefix}_avg": avg(values),
        f"{prefix}_min": round(min(values), 4),
        f"{prefix}_max": round(max(values), 4),
    }


# ---------------------------------------------------------------------------
# Endpoint normalization + artifact discovery
# ---------------------------------------------------------------------------

def normalize_endpoint_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    for canonical_key, meta in CANONICAL_ENDPOINTS.items():
        if text == canonical_key:
            return canonical_key
        if text in meta["aliases"]:
            return canonical_key
    # Looser contains-based fallback for evolved runner keys/hosts.
    squashed = text.replace("_", "-")
    if "us" in squashed and "east" in squashed:
        return "us_east"
    if "eu" in squashed and "west" in squashed:
        return "eu_west"
    if any(token in squashed for token in ["asia", "singapore", "ap-southeast", "sgp", "sg"]):
        return "asia"
    return None


def canonical_network_template() -> Dict[str, Dict[str, Optional[Any]]]:
    return {
        endpoint_key: {
            "outbound_gbps": None,
            "inbound_gbps": None,
            "outbound_status": None,
            "inbound_status": None,
        }
        for endpoint_key in CANONICAL_ENDPOINTS
    }


def _artifact_candidates_for_endpoint(endpoint_key: str, direction: str) -> List[str]:
    assert direction in {"outbound", "inbound"}
    short_direction = "out" if direction == "outbound" else "in"
    candidates: List[str] = []
    for stem in CANONICAL_ENDPOINTS[endpoint_key]["artifact_stems"]:
        normalized = stem.replace(".", "-")
        dotted = stem.replace("-", ".")
        underscored = stem.replace("-", "_")
        variants = {stem, normalized, dotted, underscored}
        for variant in variants:
            candidates.extend(
                [
                    f"raw.iperf-{variant}-{direction}.log",
                    f"raw.iperf-{variant}-{short_direction}.log",
                    f"raw.iperf.{variant}.{direction}.log",
                    f"raw.iperf.{variant}.{short_direction}.log",
                ]
            )
    # deterministic de-dupe
    seen = set()
    ordered = []
    for item in candidates:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def discover_public_files(source_dir: Path) -> List[str]:
    names: List[str] = []
    for fixed in FIXED_PUBLIC_TOP_LEVEL_FILES:
        if (source_dir / fixed).exists():
            names.append(fixed)

    # Copy all raw proof logs, including Asia and future endpoint logs, but not cmd txt files.
    for path in sorted(source_dir.iterdir()):
        if not path.is_file():
            continue
        if path.name.startswith("raw.") and path.suffix == ".log":
            names.append(path.name)

    seen = set()
    ordered = []
    for name in names:
        if name not in seen:
            ordered.append(name)
            seen.add(name)
    return ordered


def build_public_paths(provider_slug: str, run_id: str, source_dir: Path) -> Dict[str, Optional[str]]:
    artifact_base = f"/benchmarks/{provider_slug}/{run_id}/"
    public_files = set(discover_public_files(source_dir))
    paths: Dict[str, Optional[str]] = {
        "artifact_base": artifact_base,
        "run_json": artifact_base + "run.json" if "run.json" in public_files else None,
        "run_log": artifact_base + "run.log" if "run.log" in public_files else None,
        "raw_sysbench_log": artifact_base + "raw.sysbench.log" if "raw.sysbench.log" in public_files else None,
        "raw_fio_log": artifact_base + "raw.fio.log" if "raw.fio.log" in public_files else None,
        "run_md": artifact_base + "run.md" if "run.md" in public_files else None,
    }

    for endpoint_key in CANONICAL_ENDPOINTS:
        for direction in ("outbound", "inbound"):
            found = None
            for candidate in _artifact_candidates_for_endpoint(endpoint_key, direction):
                if candidate in public_files:
                    found = candidate
                    break
            suffix = "out" if direction == "outbound" else "in"
            resolved = artifact_base + found if found else None
            paths[f"raw_iperf_{endpoint_key}_{suffix}_log"] = resolved

            # Backward-compatible aliases expected by older site components.
            if endpoint_key == "us_east":
                paths[f"raw_iperf_us_{suffix}_log"] = resolved
            elif endpoint_key == "eu_west":
                paths[f"raw_iperf_eu_{suffix}_log"] = resolved
            elif endpoint_key == "asia":
                paths[f"raw_iperf_asia_{suffix}_log"] = resolved

            for tool in ("ping", "mtr"):
                tool_found = None
                for stem in CANONICAL_ENDPOINTS[endpoint_key]["artifact_stems"]:
                    candidates = [
                        f"raw.{tool}-{stem}.log",
                        f"raw.{tool}.{stem}.log",
                        f"raw.{tool}-{stem.replace('.', '-')}.log",
                        f"raw.{tool}.{stem.replace('-', '.')}.log",
                    ]
                    for candidate in candidates:
                        if candidate in public_files:
                            tool_found = candidate
                            break
                    if tool_found:
                        break
                tool_resolved = artifact_base + tool_found if tool_found else None
                paths[f"raw_{tool}_{endpoint_key}_log"] = tool_resolved

                # Backward-compatible aliases expected by older site components.
                if endpoint_key == "us_east":
                    paths[f"raw_{tool}_us_log"] = tool_resolved
                elif endpoint_key == "eu_west":
                    paths[f"raw_{tool}_eu_log"] = tool_resolved
                elif endpoint_key == "asia":
                    paths[f"raw_{tool}_asia_log"] = tool_resolved

    return paths


# ---------------------------------------------------------------------------
# Run.json normalization
# ---------------------------------------------------------------------------

def normalize_status(raw: Dict[str, Any]) -> Optional[str]:
    direct = choose(
        raw.get("status"),
        raw.get("global_status"),
        nested_get(raw, "metadata", "status"),
        nested_get(raw, "results", "status"),
    )
    if direct:
        return str(direct).strip().lower()

    statuses: List[str] = []

    def collect(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            statuses.append(value.strip().lower())

    collect(nested_get(raw, "results", "cpu", "status"))
    collect(nested_get(raw, "results", "storage", "status"))

    results_network = nested_get(raw, "results", "network", default={}) or {}
    if isinstance(results_network, dict):
        for endpoint in results_network.values():
            if not isinstance(endpoint, dict):
                continue
            for direction in ("outbound", "inbound"):
                direction_obj = endpoint.get(direction)
                if isinstance(direction_obj, dict):
                    collect(direction_obj.get("final_outcome"))
                    collect(nested_get(direction_obj, "result", "status"))

    benchmarks_network = nested_get(raw, "benchmarks", "network", default=[])
    if isinstance(benchmarks_network, list):
        for item in benchmarks_network:
            if isinstance(item, dict):
                collect(item.get("status"))

    if statuses:
        if any(s in {"failed", "failed_error", "parse_error", "missing", "error"} for s in statuses):
            return "error"
        if any(s == "failed_busy" for s in statuses):
            return "partial"
        if all(s in {"ok", "not_run"} for s in statuses):
            return "ok"
        return "error"

    # Older canonical benchmark-bundle schema without explicit statuses.
    benchmarks = raw.get("benchmarks")
    if isinstance(benchmarks, dict):
        cpu_ok = safe_float(nested_get(benchmarks, "cpu", "events_per_second")) is not None
        storage_ok = (
            safe_float(nested_get(benchmarks, "storage", "read_iops")) is not None
            or safe_float(nested_get(benchmarks, "storage", "write_iops")) is not None
        )
        network = benchmarks.get("network")
        network_ok = isinstance(network, list) and len(network) > 0
        if cpu_ok and storage_ok and network_ok:
            return "ok"
        if cpu_ok or storage_ok or network_ok:
            return "partial"

    return None


def normalize_identity(raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    provider_slug = choose(nested_get(raw, "provider", "slug"), nested_get(raw, "metadata", "provider_slug"))
    provider_name = choose(
        nested_get(raw, "provider", "name"),
        nested_get(raw, "provider", "display_name"),
        nested_get(raw, "metadata", "provider_name"),
        provider_slug,
    )
    region_slug = choose(
        nested_get(raw, "region", "slug"),
        nested_get(raw, "metadata", "region_slug"),
        nested_get(raw, "provider", "region"),
    )
    region_name = choose(
        nested_get(raw, "region", "name"),
        nested_get(raw, "metadata", "region_name"),
        region_slug,
    )
    plan_slug = choose(
        nested_get(raw, "plan", "slug"),
        nested_get(raw, "metadata", "plan_slug"),
        nested_get(raw, "provider", "plan"),
    )
    plan_name = choose(
        nested_get(raw, "plan", "name"),
        nested_get(raw, "metadata", "plan_name"),
        plan_slug,
    )
    run_id = raw.get("run_id")
    return run_id, provider_slug, provider_name, region_slug, region_name, plan_slug, plan_name


def normalize_tested_at(raw: Dict[str, Any]) -> Optional[datetime]:
    return parse_iso8601(
        choose(
            nested_get(raw, "metadata", "tested_at"),
            nested_get(raw, "timestamps", "finished_at_utc"),
            nested_get(raw, "timestamps", "started_at_utc"),
            raw.get("tested_at"),
        )
    )


def normalize_methodology_version(raw: Dict[str, Any], cfg: Config) -> str:
    return str(
        choose(
            raw.get("methodology_version"),
            nested_get(raw, "metadata", "methodology_version"),
            cfg.default_methodology_version,
        )
    )


def normalize_schema_version(raw: Dict[str, Any]) -> Optional[str]:
    return choose(raw.get("schema_version"), raw.get("version"))




def normalize_measurement_epoch(raw: Dict[str, Any], normalized_network: Optional[Dict[str, Dict[str, Optional[Any]]]] = None) -> str:
    explicit = choose(
        raw.get("measurement_epoch"),
        nested_get(raw, "metadata", "measurement_epoch"),
        nested_get(raw, "provider", "measurement_epoch"),
    )
    if explicit:
        return str(explicit).strip()

    network = normalized_network or {}
    asia = network.get("asia") if isinstance(network, dict) else None
    if isinstance(asia, dict):
        if any(
            asia.get(key) not in (None, "", {}, [])
            for key in ("outbound_gbps", "inbound_gbps", "outbound_status", "inbound_status")
        ):
            return "v2"

    results_network = nested_get(raw, "results", "network", default={}) or {}
    if isinstance(results_network, dict):
        for raw_endpoint_key, endpoint_obj in results_network.items():
            canonical_key = normalize_endpoint_key(raw_endpoint_key)
            if canonical_key == "asia":
                return "v2"
            if isinstance(endpoint_obj, dict):
                if normalize_endpoint_key(nested_get(endpoint_obj, "endpoint", "slug")) == "asia":
                    return "v2"
                if normalize_endpoint_key(nested_get(endpoint_obj, "endpoint", "host")) == "asia":
                    return "v2"

    benchmark_network = nested_get(raw, "benchmarks", "network", default=[]) or []
    if isinstance(benchmark_network, list):
        for item in benchmark_network:
            if not isinstance(item, dict):
                continue
            canonical_key = normalize_endpoint_key(
                choose(item.get("target_slug"), item.get("target_name"), item.get("endpoint"))
            )
            if canonical_key == "asia":
                return "v2"

    return "v1"

def normalize_pricing(raw: Dict[str, Any]) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    monthly = choose(
        nested_get(raw, "pricing", "monthly_usd"),
        nested_get(raw, "metadata", "cost", "monthly_usd"),
        nested_get(raw, "pricing", "monthly"),
        nested_get(raw, "metadata", "cost", "amount"),
    )
    currency = choose(
        nested_get(raw, "pricing", "currency"),
        nested_get(raw, "metadata", "cost", "currency"),
        "USD",
    )
    billing_period = choose(
        nested_get(raw, "pricing", "billing_period"),
        nested_get(raw, "metadata", "cost", "billing_period"),
        nested_get(raw, "metadata", "cost", "interval"),
        "monthly",
    )
    return safe_float(monthly), currency, billing_period


def normalize_environment(raw: Dict[str, Any]) -> Dict[str, Any]:
    ram_gb = choose(nested_get(raw, "environment", "ram_gb"), None)
    if ram_gb is None:
        ram_mb = safe_float(nested_get(raw, "environment", "ram_mb"))
        ram_gb = round(ram_mb / 1024.0, 2) if ram_mb is not None else None

    return {
        "cpu_model": choose(nested_get(raw, "environment", "cpu_model"), None),
        "vcpu_count": choose(
            safe_int(nested_get(raw, "environment", "vcpu_count")),
            safe_int(nested_get(raw, "environment", "cpu_cores")),
        ),
        "ram_gb": safe_float(ram_gb),
        "os": choose(nested_get(raw, "environment", "os"), None),
        "kernel": choose(nested_get(raw, "environment", "kernel"), None),
        "arch": choose(nested_get(raw, "environment", "arch"), None),
        "virtualization": choose(
            nested_get(raw, "environment", "virtualization"),
            nested_get(raw, "environment", "virtualization_type"),
            None,
        ),
    }


def normalize_cpu(raw: Dict[str, Any]) -> Dict[str, Any]:
    cpu = choose(nested_get(raw, "results", "cpu"), nested_get(raw, "benchmarks", "cpu"), raw.get("cpu"), {}) or {}
    latency = nested_get(cpu, "latency_ms", default={}) or {}
    return {
        "events_per_second": safe_float(choose(cpu.get("events_per_second"), nested_get(cpu, "sysbench", "events_per_second"))),
        "latency_avg_ms": safe_float(choose(nested_get(latency, "avg"), cpu.get("latency_avg_ms"))),
        "latency_p95_ms": safe_float(choose(nested_get(latency, "p95"), cpu.get("latency_p95_ms"))),
        "latency_p99_ms": safe_float(choose(nested_get(latency, "p99"), cpu.get("latency_p99_ms"))),
    }


def normalize_storage(raw: Dict[str, Any]) -> Dict[str, Any]:
    storage_root = choose(nested_get(raw, "results", "storage"), nested_get(raw, "benchmarks", "storage"), raw.get("storage"), {}) or {}
    fio = choose(storage_root.get("fio"), storage_root) or {}

    return {
        "read_iops": safe_float(choose(nested_get(fio, "read", "iops"), fio.get("read_iops"), fio.get("iops"))),
        "write_iops": safe_float(choose(nested_get(fio, "write", "iops"), fio.get("write_iops"))),
        "read_p95_ms": safe_float(choose(nested_get(fio, "latency_ms", "read_p95"), fio.get("read_p95_ms"))),
        "read_p99_ms": safe_float(choose(nested_get(fio, "latency_ms", "read_p99"), fio.get("read_p99_ms"))),
        "write_p95_ms": safe_float(choose(nested_get(fio, "latency_ms", "write_p95"), fio.get("write_p95_ms"))),
        "write_p99_ms": safe_float(choose(nested_get(fio, "latency_ms", "write_p99"), fio.get("write_p99_ms"))),
        "block_size": choose(nested_get(storage_root, "profile", "block_size"), nested_get(storage_root, "profile", "bs"), fio.get("block_size"), "4k"),
        "io_depth": choose(safe_int(nested_get(storage_root, "profile", "io_depth")), safe_int(fio.get("io_depth"))),
        "num_jobs": choose(safe_int(nested_get(storage_root, "profile", "num_jobs")), safe_int(fio.get("num_jobs"))),
        "runtime_seconds": choose(safe_int(nested_get(storage_root, "profile", "runtime_seconds")), safe_int(fio.get("runtime_seconds"))),
        "selected_size": choose(nested_get(storage_root, "profile", "size"), fio.get("selected_size")),
    }


def _extract_gbps_from_current_runner(direction_obj: Dict[str, Any], direction: str) -> Optional[float]:
    result = nested_get(direction_obj, "result", default={}) or {}
    # Prefer the perspective that matches the local VPS direction.
    if direction == "outbound":
        return choose(
            safe_float(result.get("throughput_gbps")),
            gbps_from_bps(result.get("sent_bps")),
            gbps_from_bps(result.get("received_bps")),
            gbps_from_mbps(result.get("mbps")),
        )
    return choose(
        safe_float(result.get("throughput_gbps")),
        gbps_from_bps(result.get("received_bps")),
        gbps_from_bps(result.get("sent_bps")),
        gbps_from_mbps(result.get("mbps")),
    )


def normalize_network(raw: Dict[str, Any]) -> Dict[str, Dict[str, Optional[Any]]]:
    network = canonical_network_template()

    # Current runner flat results.network shape.
    results_network = nested_get(raw, "results", "network", default={}) or {}
    if isinstance(results_network, dict) and results_network:
        for raw_endpoint_key, endpoint_obj in results_network.items():
            canonical_key = normalize_endpoint_key(raw_endpoint_key)
            if canonical_key is None and isinstance(endpoint_obj, dict):
                canonical_key = normalize_endpoint_key(nested_get(endpoint_obj, "endpoint", "slug"))
                if canonical_key is None:
                    canonical_key = normalize_endpoint_key(nested_get(endpoint_obj, "endpoint", "host"))
            if canonical_key is None or not isinstance(endpoint_obj, dict):
                continue

            for direction in ("outbound", "inbound"):
                direction_obj = endpoint_obj.get(direction)
                if not isinstance(direction_obj, dict):
                    continue
                network[canonical_key][f"{direction}_gbps"] = safe_float(_extract_gbps_from_current_runner(direction_obj, direction))
                network[canonical_key][f"{direction}_status"] = choose(
                    direction_obj.get("final_outcome"),
                    nested_get(direction_obj, "result", "status"),
                )

    # Older/frozen benchmarks.network list shape.
    benchmark_list = nested_get(raw, "benchmarks", "network", default=[])
    if isinstance(benchmark_list, list):
        for item in benchmark_list:
            if not isinstance(item, dict):
                continue
            canonical_key = normalize_endpoint_key(choose(item.get("target_slug"), item.get("target_name"), item.get("endpoint")))
            direction = str(item.get("direction") or "").strip().lower()
            if canonical_key is None or direction not in {"outbound", "inbound"}:
                continue
            gbps = choose(
                safe_float(item.get("throughput_gbps")),
                gbps_from_bps(item.get("throughput_bps")),
                gbps_from_mbps(item.get("throughput_mbps")),
            )
            if network[canonical_key][f"{direction}_gbps"] is None:
                network[canonical_key][f"{direction}_gbps"] = safe_float(gbps)
            if network[canonical_key][f"{direction}_status"] is None:
                network[canonical_key][f"{direction}_status"] = choose(item.get("status"), "ok" if gbps is not None else None)

    # Older/simple fallback.
    older = raw.get("network") or {}
    if isinstance(older, dict):
        if network["us_east"]["outbound_gbps"] is None:
            network["us_east"]["outbound_gbps"] = safe_float(
                choose(older.get("us_east_outbound_gbps"), gbps_from_mbps(older.get("us_endpoint_mbps")))
            )
        if network["eu_west"]["outbound_gbps"] is None:
            network["eu_west"]["outbound_gbps"] = safe_float(
                choose(older.get("eu_west_outbound_gbps"), gbps_from_mbps(older.get("eu_endpoint_mbps")))
            )
        if network["asia"]["outbound_gbps"] is None:
            network["asia"]["outbound_gbps"] = safe_float(
                choose(
                    older.get("asia_outbound_gbps"),
                    older.get("asia_endpoint_gbps"),
                    gbps_from_mbps(older.get("asia_endpoint_mbps")),
                )
            )

    return network


# ---------------------------------------------------------------------------
# Export eligibility and normalization
# ---------------------------------------------------------------------------

def required_export_artifacts_exist(source_dir: Path) -> Tuple[bool, List[str]]:
    missing: List[str] = []
    for name in ["run.json", "run.log", "raw.sysbench.log", "raw.fio.log"]:
        if not (source_dir / name).exists():
            missing.append(name)
    return len(missing) == 0, missing


def normalize_run(raw: Dict[str, Any], source_dir: Path, cfg: Config) -> NormalizedRun:
    run_id, provider_slug, provider_name, region_slug, region_name, plan_slug, plan_name = normalize_identity(raw)
    if not run_id:
        raise ExportError("missing run_id")
    if not provider_slug or not region_slug or not plan_slug:
        raise ExportError("missing provider/region/plan slug")

    status = normalize_status(raw)
    if not status:
        raise ExportError("missing status")
    if status not in cfg.allowed_export_statuses:
        raise ExportError(f"status={status!r} not exportable")

    tested_dt = normalize_tested_at(raw)
    if tested_dt is None:
        raise ExportError("invalid or missing tested_at")

    required_ok, missing_artifacts = required_export_artifacts_exist(source_dir)
    if not required_ok:
        raise ExportError(f"missing required artifacts: {', '.join(missing_artifacts)}")

    methodology_version = normalize_methodology_version(raw, cfg)
    schema_version = normalize_schema_version(raw)
    price_monthly_usd, currency, billing_period = normalize_pricing(raw)
    environment = normalize_environment(raw)
    cpu = normalize_cpu(raw)
    storage = normalize_storage(raw)
    network = normalize_network(raw)
    measurement_epoch = normalize_measurement_epoch(raw, network)

    has_any_metric = any(
        value is not None
        for value in [
            cpu.get("events_per_second"),
            storage.get("read_iops"),
            storage.get("write_iops"),
            *[network[k][f"{direction}_gbps"] for k in CANONICAL_ENDPOINTS for direction in ("outbound", "inbound")],
        ]
    )
    if not has_any_metric:
        raise ExportError("benchmark metrics not parseable enough for site use")

    cluster_key = f"{provider_slug}::{region_slug}::{plan_slug}"
    public_files = discover_public_files(source_dir)
    public_paths = build_public_paths(provider_slug, run_id, source_dir)

    record = {
        "run_id": run_id,
        "benchmark_page_slug": run_id,
        "status": status,
        "provider_slug": provider_slug,
        "provider_name": provider_name,
        "region_slug": region_slug,
        "region_name": region_name,
        "plan_slug": plan_slug,
        "plan_name": plan_name,
        "cluster_key": cluster_key,
        "cluster_sequence": None,
        "cluster_size": None,
        "tested_at": to_iso_z(tested_dt),
        "methodology_version": methodology_version,
        "schema_version": schema_version,
        "measurement_epoch": measurement_epoch,
        "price_monthly_usd": price_monthly_usd,
        "currency": currency,
        "billing_period": billing_period,
        "environment": environment,
        "cpu": cpu,
        "storage": storage,
        "network": network,
        "public_paths": public_paths,
    }

    return NormalizedRun(
        sort_tested_at=tested_dt,
        sort_run_id=run_id,
        record=record,
        source_dir=source_dir,
        provider_slug=provider_slug,
        run_id=run_id,
        cluster_key=cluster_key,
        public_files=public_files,
    )


# ---------------------------------------------------------------------------
# Dataset/cluster output generation
# ---------------------------------------------------------------------------

def compute_cluster_assignments(runs: List[NormalizedRun]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[NormalizedRun]] = defaultdict(list)
    for run in runs:
        grouped[run.cluster_key].append(run)

    cluster_summaries: Dict[str, Dict[str, Any]] = {}
    for cluster_key, items in grouped.items():
        items.sort(key=lambda x: (x.sort_tested_at, x.sort_run_id))
        for idx, item in enumerate(items, start=1):
            item.record["cluster_sequence"] = idx
            item.record["cluster_size"] = len(items)

        sample = items[0].record

        def metric(selector):
            values: List[float] = []
            for item in items:
                value = selector(item.record)
                if value is not None:
                    values.append(float(value))
            return values

        cluster_summary = {
            "cluster_key": cluster_key,
            "provider_slug": sample["provider_slug"],
            "provider_name": sample["provider_name"],
            "region_slug": sample["region_slug"],
            "region_name": sample["region_name"],
            "plan_slug": sample["plan_slug"],
            "plan_name": sample["plan_name"],
            "cluster_size": len(items),
            "measurement_epoch": sample.get("measurement_epoch"),
            "run_ids": [item.run_id for item in items],
            "latest_tested_at": to_iso_z(max(item.sort_tested_at for item in items)),
            "cpu": stat_block(metric(lambda r: nested_get(r, "cpu", "events_per_second")), "events_per_second"),
            "storage": {
                **stat_block(metric(lambda r: nested_get(r, "storage", "read_iops")), "read_iops"),
                **stat_block(metric(lambda r: nested_get(r, "storage", "write_iops")), "write_iops"),
            },
            "network": {},
        }

        for endpoint_key in CANONICAL_ENDPOINTS:
            cluster_summary["network"][endpoint_key] = {
                **stat_block(metric(lambda r, e=endpoint_key: nested_get(r, "network", e, "outbound_gbps")), "outbound_gbps"),
                **stat_block(metric(lambda r, e=endpoint_key: nested_get(r, "network", e, "inbound_gbps")), "inbound_gbps"),
            }

        cluster_summaries[cluster_key] = cluster_summary

    return cluster_summaries


def build_dataset_index(runs: List[NormalizedRun]) -> Dict[str, Any]:
    methodology_versions = sorted({r.record["methodology_version"] for r in runs if r.record.get("methodology_version")})
    schema_versions = sorted({r.record["schema_version"] for r in runs if r.record.get("schema_version")})
    providers = {r.record["provider_slug"] for r in runs}
    regions = {r.record["region_slug"] for r in runs}
    plans = {(r.record["provider_slug"], r.record["plan_slug"]) for r in runs}
    clusters = {r.cluster_key for r in runs}
    latest = max((r.sort_tested_at for r in runs), default=None)
    return {
        "generated_at": now_iso(),
        "export_version": EXPORT_VERSION,
        "source_repo": "webbynode-dataset",
        "methodology_versions_present": methodology_versions,
        "schema_versions_present": schema_versions,
        "summary": {
            "total_runs": len(runs),
            "providers_tested": len(providers),
            "regions_tested": len(regions),
            "plans_tested": len(plans),
            "clusters_completed": len(clusters),
            "latest_tested_at": to_iso_z(latest),
        },
        "runs": [r.record for r in sorted(runs, key=lambda x: (x.sort_tested_at, x.sort_run_id))],
    }


def build_cluster_summary(cluster_summaries: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "generated_at": now_iso(),
        "export_version": EXPORT_VERSION,
        "source_repo": "webbynode-dataset",
        "clusters": [cluster_summaries[key] for key in sorted(cluster_summaries.keys())],
    }


# ---------------------------------------------------------------------------
# File sync
# ---------------------------------------------------------------------------

def copy_file_if_changed(src: Path, dst: Path, dry_run: bool = False) -> bool:
    if not src.exists():
        return False
    if dst.exists() and src.read_bytes() == dst.read_bytes():
        return False
    if not dry_run:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    return True


def sync_public_artifacts(runs: List[NormalizedRun], cfg: Config, counters: Counters) -> None:
    desired_files: set[Path] = set()
    desired_dirs: set[Path] = set()

    for run in runs:
        dst_run_dir = cfg.dest_public_dir / run.provider_slug / run.run_id
        desired_dirs.add(dst_run_dir)
        for name in run.public_files:
            src = run.source_dir / name
            if not src.exists():
                continue
            dst = dst_run_dir / name
            desired_files.add(dst)
            if copy_file_if_changed(src, dst, dry_run=cfg.dry_run):
                counters.artifacts_copied += 1

    if cfg.dest_public_dir.exists():
        for path in sorted(cfg.dest_public_dir.rglob("*"), reverse=True):
            if path.is_file() and path not in desired_files:
                if not cfg.dry_run:
                    path.unlink()
                counters.artifacts_removed += 1
            elif path.is_dir() and path != cfg.dest_public_dir:
                try:
                    next(path.iterdir())
                except StopIteration:
                    if path not in desired_dirs:
                        if not cfg.dry_run:
                            path.rmdir()


# ---------------------------------------------------------------------------
# Config / execution
# ---------------------------------------------------------------------------

def load_config(args: argparse.Namespace) -> Config:
    cfg_dict: Dict[str, Any] = {}
    if args.config:
        cfg_path = Path(args.config).expanduser().resolve()
        cfg_dict = load_json(cfg_path)

    def val(name: str, default: Any = None) -> Any:
        cli_value = getattr(args, name)
        return cli_value if cli_value is not None else cfg_dict.get(name, default)

    dataset_repo_root = Path(val("dataset_repo_root")).expanduser().resolve()
    site_repo_root = Path(val("site_repo_root")).expanduser().resolve()

    return Config(
        dataset_repo_root=dataset_repo_root,
        site_repo_root=site_repo_root,
        source_benchmarks_rel=Path(val("source_benchmarks_rel", "benchmarks")),
        dest_data_rel=Path(val("dest_data_rel", "src/data/benchmarks")),
        dest_public_rel=Path(val("dest_public_rel", "public/benchmarks")),
        default_methodology_version=str(val("default_methodology_version", DEFAULT_METHODOLOGY_VERSION)),
        allowed_export_statuses=list(val("allowed_export_statuses", DEFAULT_ALLOWED_EXPORT_STATUSES)),
        dry_run=bool(args.dry_run),
        verbose=bool(args.verbose),
    )


def verify_paths(cfg: Config) -> None:
    if not cfg.dataset_repo_root.exists():
        raise ExportError(f"dataset_repo_root does not exist: {cfg.dataset_repo_root}")
    if not cfg.site_repo_root.exists():
        raise ExportError(f"site_repo_root does not exist: {cfg.site_repo_root}")
    if not cfg.source_benchmarks_dir.exists():
        raise ExportError(f"source benchmarks dir does not exist: {cfg.source_benchmarks_dir}")
    if not cfg.dest_data_dir.exists():
        raise ExportError(f"destination data dir does not exist: {cfg.dest_data_dir}")
    if not cfg.dest_public_dir.exists():
        raise ExportError(f"destination public dir does not exist: {cfg.dest_public_dir}")


def scan_runs(cfg: Config, counters: Counters) -> List[NormalizedRun]:
    runs: List[NormalizedRun] = []
    run_json_paths = sorted(cfg.source_benchmarks_dir.glob("*/*/run.json"))
    counters.scanned_runs = len(run_json_paths)

    for run_json_path in run_json_paths:
        source_dir = run_json_path.parent
        try:
            raw = load_json(run_json_path)
            runs.append(normalize_run(raw, source_dir, cfg))
        except ExportError as exc:
            counters.skipped_runs += 1
            counters.warnings.append(f"SKIP {source_dir.name} : {exc}")
        except Exception as exc:
            counters.skipped_runs += 1
            counters.warnings.append(f"SKIP {source_dir.name} : unexpected error: {exc}")

    runs.sort(key=lambda x: (x.sort_tested_at, x.sort_run_id))
    counters.exportable_runs = len(runs)
    return runs


def run_export(cfg: Config) -> Counters:
    verify_paths(cfg)
    counters = Counters()
    runs = scan_runs(cfg, counters)
    cluster_summaries = compute_cluster_assignments(runs)
    counters.clusters_generated = len(cluster_summaries)

    dataset_index = build_dataset_index(runs)
    cluster_summary = build_cluster_summary(cluster_summaries)

    sync_public_artifacts(runs, cfg, counters)
    write_json(cfg.dest_data_dir / "dataset-index.json", dataset_index, dry_run=cfg.dry_run)
    write_json(cfg.dest_data_dir / "cluster-summary.json", cluster_summary, dry_run=cfg.dry_run)

    return counters


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Webbynode benchmark artifacts into the site repo package.")
    parser.add_argument("--config", help="Path to JSON config file.")
    parser.add_argument("--dataset-repo-root", dest="dataset_repo_root", help="Path to webbynode-dataset repo root.")
    parser.add_argument("--site-repo-root", dest="site_repo_root", help="Path to webbynode.com repo root.")
    parser.add_argument("--source-benchmarks-rel", dest="source_benchmarks_rel", help="Benchmark source path relative to dataset repo root.")
    parser.add_argument("--dest-data-rel", dest="dest_data_rel", help="Data output path relative to site repo root.")
    parser.add_argument("--dest-public-rel", dest="dest_public_rel", help="Public artifacts output path relative to site repo root.")
    parser.add_argument("--default-methodology-version", dest="default_methodology_version", help="Fallback methodology version.")
    parser.add_argument("--allowed-export-statuses", dest="allowed_export_statuses", nargs="+", help="List of exportable statuses.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and compute exports without writing files.")
    parser.add_argument("--verbose", action="store_true", help="Print skip warnings.")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args)
        counters = run_export(cfg)
    except ExportError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"UNEXPECTED ERROR: {exc}", file=sys.stderr)
        return 3

    print(f"Scanned runs: {counters.scanned_runs}")
    print(f"Exportable runs: {counters.exportable_runs}")
    print(f"Skipped runs: {counters.skipped_runs}")
    print(f"Clusters generated: {counters.clusters_generated}")
    print(f"Artifacts copied: {counters.artifacts_copied}")
    print(f"Artifacts removed: {counters.artifacts_removed}")
    if args.verbose and counters.warnings:
        for line in counters.warnings:
            print(line)
    elif counters.warnings:
        print(f"Warnings: {len(counters.warnings)} (use --verbose to print)")
    print("dataset-index.json written" + (" [dry-run skipped]" if args.dry_run else ""))
    print("cluster-summary.json written" + (" [dry-run skipped]" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
