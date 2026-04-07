# === WEBBYNODE EXPORTER v2 (FULL FIXED) ===

# --- ONLY SHOWING MODIFIED / CRITICAL SECTIONS ---
# (Everything else in your file stays the same)

# -----------------------------
# 1. ADD ASIA ARTIFACTS
# -----------------------------
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
    "raw.iperf-asia-outbound.log",     # ✅ ADDED
    "raw.iperf-asia-inbound.log",      # ✅ ADDED
]

# -----------------------------
# 2. FIX NETWORK NORMALIZATION
# -----------------------------
def normalize_network(raw: Dict[str, Any]) -> Dict[str, Any]:
    network = {
        "us_east": {"outbound_gbps": None, "inbound_gbps": None, "outbound_status": None, "inbound_status": None},
        "eu_west": {"outbound_gbps": None, "inbound_gbps": None, "outbound_status": None, "inbound_status": None},
        "asia":    {"outbound_gbps": None, "inbound_gbps": None, "outbound_status": None, "inbound_status": None},  # ✅ ADDED
    }

    results_network = nested_get(raw, "results", "network", default={}) or {}

    if isinstance(results_network, dict):
        for region in ["us_east", "eu_west", "asia"]:   # ✅ ADDED asia
            endpoint = results_network.get(region, {})
            if not isinstance(endpoint, dict):
                continue

            out = endpoint.get("outbound", {})
            inn = endpoint.get("inbound", {})

            out_gbps, out_status = _normalize_network_result(out)
            in_gbps, in_status = _normalize_network_result(inn)

            network[region]["outbound_gbps"] = out_gbps
            network[region]["inbound_gbps"] = in_gbps
            network[region]["outbound_status"] = out_status
            network[region]["inbound_status"] = in_status

    return network


# -----------------------------
# 3. ADD ASIA TO PUBLIC PATHS
# -----------------------------
def build_public_paths(provider_slug: str, run_id: str, source_dir: Path) -> Dict[str, Optional[str]]:
    base = f"/benchmarks/{provider_slug}/{run_id}/"

    paths = {
        "artifact_base": base,
        "run_json": base + "run.json",
        "run_log": base + "run.log",
        "raw_sysbench_log": base + "raw.sysbench.log",
        "raw_fio_log": base + "raw.fio.log",

        "raw_iperf_us_out_log": base + "raw.iperf-us-east-outbound.log",
        "raw_iperf_us_in_log": base + "raw.iperf-us-east-inbound.log",
        "raw_iperf_eu_out_log": base + "raw.iperf-eu-west-outbound.log",
        "raw_iperf_eu_in_log": base + "raw.iperf-eu-west-inbound.log",

        "raw_iperf_asia_out_log": base + "raw.iperf-asia-outbound.log",   # ✅ ADDED
        "raw_iperf_asia_in_log": base + "raw.iperf-asia-inbound.log",    # ✅ ADDED

        "run_md": base + "run.md",
    }

    # Remove if file not present
    for key, filename in {
        "raw_iperf_asia_out_log": "raw.iperf-asia-outbound.log",
        "raw_iperf_asia_in_log": "raw.iperf-asia-inbound.log",
    }.items():
        if not (source_dir / filename).exists():
            paths[key] = None

    return paths


# -----------------------------
# 4. ADD ASIA TO CLUSTER SUMMARY
# -----------------------------
def compute_cluster_assignments(runs: List[NormalizedRun]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[NormalizedRun]] = defaultdict(list)

    for run in runs:
        grouped[run.cluster_key].append(run)

    cluster_summaries = {}

    for cluster_key, items in grouped.items():
        items.sort(key=lambda x: (x.sort_tested_at, x.sort_run_id))

        def nums(path):
            values = []
            for r in items:
                val = nested_get(r.record, *path)
                if val is not None:
                    values.append(float(val))
            return values

        def stats(values):
            if not values:
                return {}
            return {
                "avg": round(sum(values) / len(values), 4),
                "min": round(min(values), 4),
                "max": round(max(values), 4),
            }

        cluster_summaries[cluster_key] = {
            "cluster_key": cluster_key,
            "cluster_size": len(items),
            "latest_tested_at": to_iso_z(max(i.sort_tested_at for i in items)),

            "network": {
                "us_east": stats(nums(["network", "us_east", "outbound_gbps"])),
                "eu_west": stats(nums(["network", "eu_west", "outbound_gbps"])),
                "asia":    stats(nums(["network", "asia", "outbound_gbps"])),  # ✅ ADDED
            },
        }

    return cluster_summaries