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
from typing import Any, Dict, Iterable, List, Optional, Tuple

EXPORT_VERSION = "1.0"
DEFAULT_METHODOLOGY_VERSION = "1.1"
DEFAULT_ALLOWED_EXPORT_STATUSES = ["ok"]

PUBLIC_ARTIFACT_FILENAMES = [
    "run.json",
    "run.log",
    "run.md",
    "raw.sysbench.log",
    "raw.fio.log",
    "raw.iperf-us-east-outbound.log",
    "raw.iperf-us-east-inbound.log",
    "raw.iperf-eu-west-outbound.log",
    "raw.iperf-eu-west-inbound.log",
    "raw.ping-us-east.log",
    "raw.ping-eu-west.log",
    "raw.mtr-us-east.log",
    "raw.mtr-eu-west.log",
]


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


def nested_get(obj: Dict[str, Any], *path: str, default: Any = None) -> Any:
    cur: Any = obj
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


def normalize_status(raw: Dict[str, Any]) -> Optional[str]:
    direct = choose(
        raw.get("status"),
        raw.get("global_status"),
        nested_get(raw, "metadata", "status"),
        nested_get(raw, "results", "status"),
    )
    if direct:
        return str(direct)

    statuses: List[str] = []
    for path in [
        ("results", "cpu", "status"),
        ("results", "storage", "status"),
        ("results", "network", "us_east", "outbound", "result", "status"),
        ("results", "network", "us_east", "outbound", "final_outcome"),
        ("results", "network", "us_east", "inbound", "result", "status"),
        ("results", "network", "us_east", "inbound", "final_outcome"),
        ("results", "network", "eu_west", "outbound", "result", "status"),
        ("results", "network", "eu_west", "outbound", "final_outcome"),
        ("results", "network", "eu_west", "inbound", "result", "status"),
        ("results", "network", "eu_west", "inbound", "final_outcome"),
    ]:
        val = nested_get(raw, *path)
        if isinstance(val, str) and val.strip():
            statuses.append(val.strip())

    if statuses:
        lowered = [s.lower() for s in statuses]
        if any(s in {"failed", "failed_error", "parse_error", "missing", "error"} for s in lowered):
            return "error"
        if any(s == "failed_busy" for s in lowered):
            return "partial"
        if all(s in {"ok", "not_run"} for s in lowered):
            return "ok"
        return "error"

    # Older/current benchmark-bundle schema with top-level `benchmarks` and no explicit statuses.
    benchmarks = raw.get("benchmarks")
    if isinstance(benchmarks, dict):
        cpu_ok = isinstance(benchmarks.get("cpu"), dict) and safe_float(nested_get(benchmarks, "cpu", "events_per_second")) is not None
        storage_ok = isinstance(benchmarks.get("storage"), dict) and (
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
    provider_name = choose(nested_get(raw, "provider", "name"), nested_get(raw, "provider", "display_name"), nested_get(raw, "metadata", "provider_name"), provider_slug)
    region_slug = choose(nested_get(raw, "region", "slug"), nested_get(raw, "metadata", "region_slug"), nested_get(raw, "provider", "region"))
    region_name = choose(nested_get(raw, "region", "name"), nested_get(raw, "metadata", "region_name"), region_slug)
    plan_slug = choose(nested_get(raw, "plan", "slug"), nested_get(raw, "metadata", "plan_slug"), nested_get(raw, "provider", "plan"))
    plan_name = choose(nested_get(raw, "plan", "name"), nested_get(raw, "metadata", "plan_name"), plan_slug)
    run_id = raw.get("run_id")
    return run_id, provider_slug, provider_name, region_slug, region_name, plan_slug, plan_name


def normalize_tested_at(raw: Dict[str, Any]) -> Optional[datetime]:
    return (
        parse_iso8601(choose(
            nested_get(raw, "metadata", "tested_at"),
            nested_get(raw, "timestamps", "finished_at_utc"),
            nested_get(raw, "timestamps", "started_at_utc"),
            raw.get("tested_at"),
        ))
    )


def normalize_methodology_version(raw: Dict[str, Any], cfg: Config) -> str:
    return str(choose(raw.get("methodology_version"), nested_get(raw, "metadata", "methodology_version"), cfg.default_methodology_version))


def normalize_schema_version(raw: Dict[str, Any]) -> Optional[str]:
    return choose(raw.get("schema_version"), raw.get("version"))


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
        "vcpu_count": choose(safe_int(nested_get(raw, "environment", "vcpu_count")), safe_int(nested_get(raw, "environment", "cpu_cores"))),
        "ram_gb": safe_float(ram_gb),
        "os": choose(nested_get(raw, "environment", "os"), None),
        "kernel": choose(nested_get(raw, "environment", "kernel"), None),
        "arch": choose(nested_get(raw, "environment", "arch"), None),
        "virtualization": choose(nested_get(raw, "environment", "virtualization"), None),
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
    read_iops = choose(nested_get(fio, "read", "iops"), fio.get("read_iops"), fio.get("iops"))
    write_iops = choose(nested_get(fio, "write", "iops"), fio.get("write_iops"))
    return {
        "read_iops": safe_float(read_iops),
        "write_iops": safe_float(write_iops),
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


def _normalize_network_result(result: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    if not isinstance(result, dict):
        return None, None

    inner = result.get("result") if isinstance(result.get("result"), dict) else {}

    throughput = choose(
        result.get("throughput_gbps"),
        inner.get("throughput_gbps"),
        gbps_from_bps(result.get("received_bps")),
        gbps_from_bps(result.get("sent_bps")),
        gbps_from_bps(inner.get("received_bps")),
        gbps_from_bps(inner.get("sent_bps")),
        gbps_from_mbps(result.get("mbps")),
        gbps_from_mbps(inner.get("mbps")),
    )
    status = choose(
        inner.get("status"),
        result.get("status"),
        result.get("final_outcome"),
        inner.get("final_outcome"),
    )
    return safe_float(throughput), status


def normalize_network(raw: Dict[str, Any]) -> Dict[str, Any]:
    network = {
        "us_east": {
            "outbound_gbps": None,
            "inbound_gbps": None,
            "outbound_status": None,
            "inbound_status": None,
        },
        "eu_west": {
            "outbound_gbps": None,
            "inbound_gbps": None,
            "outbound_status": None,
            "inbound_status": None,
        },
    }

    results_network = nested_get(raw, "results", "network", default={}) or {}
    if isinstance(results_network, dict) and ("us_east" in results_network or "eu_west" in results_network):
        for endpoint_key in ("us_east", "eu_west"):
            endpoint = results_network.get(endpoint_key) or {}
            out_gbps, out_status = _normalize_network_result(endpoint.get("outbound") if isinstance(endpoint, dict) else {})
            in_gbps, in_status = _normalize_network_result(endpoint.get("inbound") if isinstance(endpoint, dict) else {})
            network[endpoint_key]["outbound_gbps"] = out_gbps
            network[endpoint_key]["inbound_gbps"] = in_gbps
            network[endpoint_key]["outbound_status"] = out_status
            network[endpoint_key]["inbound_status"] = in_status
        return network

    benchmark_list = nested_get(raw, "benchmarks", "network", default=[]) or []
    if isinstance(benchmark_list, list) and benchmark_list:
        target_map = {
            "us-east-iperf": "us_east",
            "eu-west-iperf": "eu_west",
        }
        for item in benchmark_list:
            if not isinstance(item, dict):
                continue
            endpoint_key = target_map.get(str(item.get("target_slug") or "").strip().lower())
            direction = str(item.get("direction") or "").strip().lower()
            if endpoint_key not in network or direction not in {"outbound", "inbound"}:
                continue
            gbps = safe_float(item.get("throughput_gbps"))
            if gbps is None:
                gbps = gbps_from_bps(item.get("throughput_bps"))
            network[endpoint_key][f"{direction}_gbps"] = gbps
            network[endpoint_key][f"{direction}_status"] = "ok" if gbps is not None else None
        return network

    # Older/simple shape fallback.
    older = raw.get("network") or {}
    network["us_east"]["outbound_gbps"] = safe_float(choose(older.get("us_east_outbound_gbps"), gbps_from_mbps(older.get("us_endpoint_mbps"))))
    network["eu_west"]["outbound_gbps"] = safe_float(choose(older.get("eu_west_outbound_gbps"), gbps_from_mbps(older.get("eu_endpoint_mbps"))))
    return network


def build_public_paths(provider_slug: str, run_id: str, source_dir: Path) -> Dict[str, Optional[str]]:
    artifact_base = f"/benchmarks/{provider_slug}/{run_id}/"
    paths: Dict[str, Optional[str]] = {
        "artifact_base": artifact_base,
        "run_json": artifact_base + "run.json",
        "run_log": artifact_base + "run.log",
        "raw_sysbench_log": artifact_base + "raw.sysbench.log",
        "raw_fio_log": artifact_base + "raw.fio.log",
        "raw_iperf_us_out_log": artifact_base + "raw.iperf-us-east-outbound.log",
        "raw_iperf_us_in_log": artifact_base + "raw.iperf-us-east-inbound.log",
        "raw_iperf_eu_out_log": artifact_base + "raw.iperf-eu-west-outbound.log",
        "raw_iperf_eu_in_log": artifact_base + "raw.iperf-eu-west-inbound.log",
        "run_md": artifact_base + "run.md",
    }
    # Null out entries for files that do not exist.
    file_map = {
        "run_json": "run.json",
        "run_log": "run.log",
        "raw_sysbench_log": "raw.sysbench.log",
        "raw_fio_log": "raw.fio.log",
        "raw_iperf_us_out_log": "raw.iperf-us-east-outbound.log",
        "raw_iperf_us_in_log": "raw.iperf-us-east-inbound.log",
        "raw_iperf_eu_out_log": "raw.iperf-eu-west-outbound.log",
        "raw_iperf_eu_in_log": "raw.iperf-eu-west-inbound.log",
        "run_md": "run.md",
    }
    for key, name in file_map.items():
        if not (source_dir / name).exists():
            paths[key] = None
    return paths


def required_export_artifacts_exist(source_dir: Path) -> Tuple[bool, List[str]]:
    missing = []
    required = ["run.json", "run.log", "raw.sysbench.log", "raw.fio.log"]
    for name in required:
        if not (source_dir / name).exists():
            missing.append(name)
    return (len(missing) == 0), missing


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

    if cpu["events_per_second"] is None and storage["read_iops"] is None and storage["write_iops"] is None:
        raise ExportError("benchmark metrics not parseable enough for site use")

    cluster_key = f"{provider_slug}::{region_slug}::{plan_slug}"
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
    )


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

        def nums(selector):
            values = []
            for i in items:
                val = selector(i.record)
                if val is not None:
                    values.append(float(val))
            return values

        def stat_block(values: List[float], prefix: str) -> Dict[str, Any]:
            if not values:
                return {}
            return {
                f"{prefix}_avg": round(sum(values) / len(values), 4),
                f"{prefix}_min": round(min(values), 4),
                f"{prefix}_max": round(max(values), 4),
            }

        cluster_summary = {
            "cluster_key": cluster_key,
            "provider_slug": sample["provider_slug"],
            "provider_name": sample["provider_name"],
            "region_slug": sample["region_slug"],
            "region_name": sample["region_name"],
            "plan_slug": sample["plan_slug"],
            "plan_name": sample["plan_name"],
            "cluster_size": len(items),
            "run_ids": [i.run_id for i in items],
            "latest_tested_at": to_iso_z(max(i.sort_tested_at for i in items)),
            "cpu": stat_block(nums(lambda r: nested_get(r, "cpu", "events_per_second")), "events_per_second"),
            "storage": {
                **stat_block(nums(lambda r: nested_get(r, "storage", "read_iops")), "read_iops"),
                **stat_block(nums(lambda r: nested_get(r, "storage", "write_iops")), "write_iops"),
            },
            "network": {
                "us_east": {
                    **stat_block(nums(lambda r: nested_get(r, "network", "us_east", "outbound_gbps")), "outbound_gbps"),
                    **stat_block(nums(lambda r: nested_get(r, "network", "us_east", "inbound_gbps")), "inbound_gbps"),
                },
                "eu_west": {
                    **stat_block(nums(lambda r: nested_get(r, "network", "eu_west", "outbound_gbps")), "outbound_gbps"),
                    **stat_block(nums(lambda r: nested_get(r, "network", "eu_west", "inbound_gbps")), "inbound_gbps"),
                },
            },
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
        "clusters": [cluster_summaries[k] for k in sorted(cluster_summaries.keys())],
    }


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
        for name in PUBLIC_ARTIFACT_FILENAMES:
            src = run.source_dir / name
            if src.exists():
                dst = dst_run_dir / name
                desired_files.add(dst)
                changed = copy_file_if_changed(src, dst, dry_run=cfg.dry_run)
                if changed:
                    counters.artifacts_copied += 1

    # Remove stale files.
    if cfg.dest_public_dir.exists():
        for path in sorted(cfg.dest_public_dir.rglob("*"), reverse=True):
            if path.is_file() and path not in desired_files:
                if not cfg.dry_run:
                    path.unlink()
                counters.artifacts_removed += 1
            elif path.is_dir() and path != cfg.dest_public_dir:
                # Remove empty dirs not in desired_dirs.
                try:
                    next(path.iterdir())
                except StopIteration:
                    if path not in desired_dirs:
                        if not cfg.dry_run:
                            path.rmdir()


def load_config(args: argparse.Namespace) -> Config:
    cfg_dict: Dict[str, Any] = {}
    if args.config:
        cfg_path = Path(args.config).expanduser().resolve()
        cfg_dict = load_json(cfg_path)

    def val(name: str, default: Any = None) -> Any:
        return getattr(args, name) if getattr(args, name) is not None else cfg_dict.get(name, default)

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
            normalized = normalize_run(raw, source_dir, cfg)
            runs.append(normalized)
        except ExportError as exc:
            counters.skipped_runs += 1
            counters.warnings.append(f"SKIP {source_dir.name} : {exc}")
        except Exception as exc:  # pragma: no cover
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
    except Exception as exc:  # pragma: no cover
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
