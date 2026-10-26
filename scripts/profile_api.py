#!/usr/bin/env python3
r"""Summarize Photon API profiling JSONL without conflating errors or workloads.

Example:
  python3 scripts/profile_api.py --baseline /tmp/before.jsonl \
      --candidate /tmp/after.jsonl --json-output /tmp/profile.json \
      --markdown-output /tmp/profile.md --csv-output /tmp/profile.csv

Repeat --baseline/--candidate for additional runs. Companion .out files are read
for full-suite failures; explicit --baseline-test-log/--candidate-test-log paths
can be supplied as well. Profiles themselves must use the API span schema, not
SQL EXPLAIN measurements. No request bodies or SQL text are copied to reports.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


TARGET_SPEEDUP = 10.0
DEFAULT_RPC_SOURCE = Path(__file__).resolve().parents[1] / "src/api/rpc_server.rs"
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def rpc_methods(source: str) -> dict[str, str]:
    """Extract direct registrations, including aliases pointing to one method."""
    tokens = re.compile(r'"(?:\\.|[^"\\])*"|//[^\n]*|/\*.*?\*/', re.S)
    clean = tokens.sub(lambda match: match[0] if match[0].startswith('"') else " ", source)
    calls = list(re.finditer(r'register_async_method\s*\(\s*"([^"\\]+)"', clean))
    result = {}
    for index, call in enumerate(calls):
        end = calls[index + 1].start() if index + 1 < len(calls) else len(clean)
        body = re.sub(r'"(?:\\.|[^"\\])*"', '""', clean[call.end():end])
        implementations = set(re.findall(r"\.\s*(get_[a-z0-9_]+|liveness|readiness)\s*\(", body))
        if len(implementations) != 1 or call[1] in result:
            raise ValueError(f"Cannot uniquely map RPC registration {call[1]!r}")
        result[call[1]] = implementations.pop()
    if not result:
        raise ValueError("RPC source contains no direct async method registrations")
    return result


def backend_for(record: dict) -> str:
    detected = set(re.findall(r"(?:DatabaseBackend__)?(Sqlite|Postgres)\b", record["test"]))
    explicit = record.get("backend")
    aliases = {"sqlite": "Sqlite", "postgres": "Postgres", "postgresql": "Postgres", "pg": "Postgres"}
    if explicit is not None:
        normalized = aliases.get(str(explicit).lower())
        if normalized is None or (detected and detected != {normalized}):
            raise ValueError(f"Unknown or conflicting backend {explicit!r} for {record['test']!r}")
        return normalized
    return detected.pop() if len(detected) == 1 else "Unknown"


def load_profiles(paths: list[Path]) -> tuple[list[dict], list[dict]]:
    if len({path.resolve() for path in paths}) != len(paths):
        raise ValueError("Duplicate profile paths would count the same samples more than once")
    samples, inputs = [], []
    for path in paths:
        digest = hashlib.sha256()
        count = 0
        with path.open("rb") as stream:
            for line_number, raw in enumerate(stream, 1):
                digest.update(raw)
                if not raw.strip():
                    continue
                try:
                    row = json.loads(raw)
                    if not isinstance(row, dict):
                        raise ValueError("Expected an API profile object")
                    for key in ["method", "test"]:
                        if not isinstance(row.get(key), str) or not row[key]:
                            raise ValueError(f"Missing API span field {key!r}")
                    if not isinstance(row.get("success"), bool):
                        raise ValueError("API span success must be a Boolean")
                    for key in ["elapsed_ns", "database_queries", "database_elapsed_ns"]:
                        value = row.get(key)
                        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                            raise ValueError(f"API span {key} must be a nonnegative integer")
                    samples.append({
                        key: row[key]
                        for key in ["method", "test", "success", "elapsed_ns", "database_queries", "database_elapsed_ns"]
                    } | {"backend": backend_for(row), "source": str(path)})
                    count += 1
                except (ValueError, TypeError, UnicodeError) as error:
                    raise ValueError(f"{path}:{line_number}: {error}") from error
        inputs.append({"path": str(path), "sha256": digest.hexdigest(), "records": count})
    return samples, inputs


def distribution(values: list[int], divisor: float = 1.0) -> dict | None:
    if not values:
        return None
    ordered = sorted(values)
    return {
        "median": statistics.median(ordered) / divisor,
        "p95": ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)] / divisor,
        "min": ordered[0] / divisor,
        "max": ordered[-1] / divisor,
        "total": sum(ordered) / divisor,
    }


def sample_stats(samples: list[dict]) -> dict:
    def outcome(success: bool) -> dict:
        rows = [sample for sample in samples if sample["success"] is success]
        return {
            "samples": len(rows),
            "elapsed_ms": distribution([row["elapsed_ns"] for row in rows], 1_000_000),
            "database_query_count": distribution([row["database_queries"] for row in rows]),
            "database_elapsed_ms": distribution([row["database_elapsed_ns"] for row in rows], 1_000_000),
        }
    return {"successful": outcome(True), "errors": outcome(False)}


def load_test_logs(paths: list[Path]) -> list[dict]:
    """Keep full-suite failures separate from successful API span measurements."""
    logs = []
    for path in paths:
        text = ANSI_ESCAPE.sub("", path.read_text())
        summaries = []
        for match in re.finditer(
            r"test result:\s*(ok|FAILED)\.\s*(\d+) passed;\s*(\d+) failed;\s*(\d+) ignored(?:;\s*(\d+) measured;\s*(\d+) filtered out)?", text
        ):
            summaries.append(dict(status=match[1], passed=int(match[2]), failed=int(match[3]), ignored=int(match[4]), measured=int(match[5] or 0), filtered_out=int(match[6] or 0)))
        names = list(dict.fromkeys(re.findall(r"^test (.+?) \.\.\. FAILED\s*$", text, re.M)))
        blocks = {}
        for match in re.finditer(r"^---- (.+?) stdout ----\s*\n(.*?)(?=^---- |^failures:|^test result:|\Z)", text, re.M | re.S):
            blocks[match[1]] = match[2].strip()
            if match[1] not in names:
                names.append(match[1])
        failures = []
        for name in names:
            message = blocks.get(name, "")
            if name.startswith("merkle_tree_deserialization::") and "error sending request" in message:
                category = "network_dependency"
            elif name.startswith("open_api_tests::") and "NotFound" in message:
                category = "external_tool_dependency"
            else:
                category = "unclassified"
            failures.append({"test": name, "category": category, "detail": message[:1000]})
        logs.append({
            "path": str(path), "summaries": summaries, "failures": failures,
            "unidentified_failures": max(0, sum(row["failed"] for row in summaries) - len(failures)),
            "status": "failed" if failures or any(row["status"] == "FAILED" or row["failed"] for row in summaries) else "passed" if summaries else "unknown",
        })
    return logs


def floor_status(stats: dict, floor_us: float) -> str:
    latency = stats["successful"]["elapsed_ms"]
    if latency is None:
        return "no_successful_samples"
    return "below_reference_threshold" if latency["median"] <= floor_us / 1000 else "above_reference_threshold"


def build_report(
    baseline: list[dict], candidate: list[dict] | None, mapping: dict[str, str],
    baseline_logs: list[dict] | None = None, candidate_logs: list[dict] | None = None,
    min_samples: int = 3, floor_us: float = 1000,
) -> dict:
    known = set(mapping.values())
    aliases = {method: sorted(rpc for rpc, implementation in mapping.items() if implementation == method) for method in known}
    cohorts = {"baseline": baseline, "candidate": candidate}
    logs = {"baseline": baseline_logs or [], "candidate": candidate_logs or []}
    failed = {name: {failure["test"] for log in entries for failure in log["failures"]} for name, entries in logs.items()}
    groups = {}
    method_samples = {}
    for name, samples in cohorts.items():
        grouped = defaultdict(list)
        by_method = defaultdict(list)
        for sample in samples or []:
            grouped[(sample["backend"], sample["test"], sample["method"])].append(sample)
            by_method[(sample["backend"], sample["method"])].append(sample)
        groups[name] = grouped
        method_samples[name] = by_method
    comparisons = []
    for backend, test, method in sorted(set(groups["baseline"]) | set(groups["candidate"])):
        before = sample_stats(groups["baseline"][(backend, test, method)])
        after = sample_stats(groups["candidate"][(backend, test, method)])
        count_match = before["successful"]["samples"] == after["successful"]["samples"]
        errors_match = before["errors"]["samples"] == after["errors"]["samples"]
        reasons = []
        if candidate is None:
            reasons.append("candidate_not_provided")
        if method not in known:
            reasons.append("unknown_method")
        if backend == "Unknown":
            reasons.append("unknown_backend")
        if not count_match:
            reasons.append("successful_sample_count_mismatch")
        if not errors_match:
            reasons.append("error_sample_count_mismatch")
        if min(before["successful"]["samples"], after["successful"]["samples"]) < min_samples:
            reasons.append("insufficient_successful_samples")
        if test in failed["baseline"] or test in failed["candidate"]:
            reasons.append("test_failed_in_suite")
        before_latency, after_latency = before["successful"]["elapsed_ms"], after["successful"]["elapsed_ms"]
        if before_latency and after_latency and min(before_latency["median"], after_latency["median"]) <= 0:
            reasons.append("zero_median_duration")
        comparable = not reasons
        speedup = before_latency["median"] / after_latency["median"] if comparable else None
        comparisons.append({
            "backend": backend, "test": test, "method": method, "rpc_names": aliases.get(method, []),
            "baseline": before, "candidate": after if candidate is not None else None,
            "successful_sample_counts_match": count_match, "error_sample_counts_match": errors_match,
            "comparable": comparable, "incomparable_reasons": reasons,
            "median_speedup": speedup, "measured_10x": comparable and speedup >= TARGET_SPEEDUP,
            "latency_floor_status": floor_status(before, floor_us),
        })
    backends = sorted({row["backend"] for samples in cohorts.values() for row in samples or []} - {"Unknown"})
    method_rows = []
    coverage = {}
    for name, samples in cohorts.items():
        if samples is None:
            coverage[name] = None
            continue
        successes = {row["method"] for row in samples if row["success"]}
        pairs = {(row["backend"], row["method"]) for row in samples if row["success"]}
        coverage[name] = {
            "records": len(samples), "successful_calls": sum(row["success"] for row in samples),
            "error_calls": sum(not row["success"] for row in samples),
            "successful_implementations": len(successes & known),
            "missing_implementations": sorted(known - successes),
            "missing_rpc_names": sorted(rpc for rpc, method in mapping.items() if method not in successes),
            "missing_backend_implementations": [{"backend": backend, "method": method, "rpc_names": aliases[method]} for backend in backends for method in sorted(known) if (backend, method) not in pairs],
            "unknown_methods": sorted({row["method"] for row in samples} - known),
            "unknown_backend_records": sum(row["backend"] == "Unknown" for row in samples),
        }
    for backend in backends:
        for method in sorted(known):
            before = sample_stats(method_samples["baseline"][(backend, method)])
            after = sample_stats(method_samples["candidate"][(backend, method)])
            rows = [row for row in comparisons if row["backend"] == backend and row["method"] == method]
            comparable = [row for row in rows if row["comparable"]]
            met = [row for row in comparable if row["measured_10x"]]
            if candidate is None:
                status = "candidate_not_provided"
            elif not rows or len(comparable) != len(rows):
                status = "incomplete_comparison"
            elif len(met) == len(rows):
                status = "met_in_all_recorded_groups"
            elif met:
                status = "met_in_some_groups"
            else:
                status = "below_target"
            method_rows.append({
                "backend": backend, "method": method, "rpc_names": aliases[method],
                "baseline": before, "candidate": after if candidate is not None else None,
                "recorded_groups": len(rows), "comparable_groups": len(comparable),
                "measured_10x_groups": len(met), "target_status": status,
                "latency_floor_status": floor_status(before, floor_us),
            })
    return {
        "schema_version": 1,
        "config": {"target_speedup": TARGET_SPEEDUP, "minimum_successful_samples": min_samples, "latency_floor_reference_us": floor_us},
        "registry": {"rpc_names": len(mapping), "implementations": len(known), "mapping": mapping, "backends": backends},
        "coverage": coverage, "test_runs": logs, "methods": method_rows, "groups": comparisons,
        "measured_10x_groups": sum(row["measured_10x"] for row in comparisons),
        "limitations": [
            "Only successful API span latencies are compared; errors are reported separately.",
            "Counts must match within each test, method and backend, including error counts; at least the configured successful sample count is required.",
            "Matching counts do not prove identical request parameters, cache state, build flags or fixtures; profiles do not record that provenance.",
            "Method-level aggregates can mix request shapes; 10x flags come only from comparable test/method/backend groups.",
            "Aliases share implementation samples; registry coverage does not establish separate HTTP alias dispatch measurements.",
            "The latency-floor threshold is a reporting reference, not a demonstrated physical lower bound.",
            "SQL-only experiment speedups are not included, and API span times exclude external HTTP client transport unless included by the profiler.",
            "A complete API trace does not mean the full test suite passed; test log failures are retained separately.",
        ],
    }


def _metric(stats: dict | None, metric: str, field: str = "median") -> str:
    value = stats and stats["successful"][metric]
    return f"{value[field]:.4f}" if value is not None else "—"


def markdown_report(report: dict) -> str:
    registry = report["registry"]
    lines = ["# Photon API profile", "", f"Registry: {registry['rpc_names']} RPC names / {registry['implementations']} implementations. Target: 10× per comparable successful group.", ""]
    for name, coverage in report["coverage"].items():
        if coverage is not None:
            lines.append(f"- {name}: {coverage['records']} calls, {coverage['successful_calls']} successful, {coverage['error_calls']} errors; {coverage['successful_implementations']}/{registry['implementations']} implementations have successful samples.")
    lines.extend(["", "## Full test-run status", ""])
    for name, logs in report["test_runs"].items():
        if not logs:
            lines.append(f"- {name}: no test log supplied or inferred; full-suite status unknown.")
        for log in logs:
            totals = {key: sum(row[key] for row in log["summaries"]) for key in ["passed", "failed", "ignored", "filtered_out"]}
            lines.append(f"- {name}: **{log['status']}** — {totals['passed']} passed, {totals['failed']} failed, {totals['ignored']} ignored, {totals['filtered_out']} filtered out (`{log['path']}`).")
            for failure in log["failures"]:
                lines.append(f"  - `{failure['test']}` — {failure['category']}")
            if log["unidentified_failures"]:
                lines.append(f"  - {log['unidentified_failures']} failure(s) have no individual details in this log.")
    lines.extend(["", "## Method metrics", "", "Aggregates below describe the recorded fixture mix. Target status uses the per-test comparisons, not ratios of these mixed aggregates.", "", "| RPC names | Backend | Baseline OK/errors | Candidate OK/errors | Baseline p50 / p95 ms | Candidate p50 / p95 ms | DB query median before / after | 10× groups | Status / floor |", "|---|---|---:|---:|---:|---:|---:|---:|---|"])
    for row in report["methods"]:
        before, after = row["baseline"], row["candidate"]
        counts = f"{after['successful']['samples']}/{after['errors']['samples']}" if after else "—"
        names = ", ".join(row["rpc_names"])
        lines.append(f"| {names} | {row['backend']} | {before['successful']['samples']}/{before['errors']['samples']} | {counts} | {_metric(before,'elapsed_ms')} / {_metric(before,'elapsed_ms','p95')} | {_metric(after,'elapsed_ms')} / {_metric(after,'elapsed_ms','p95')} | {_metric(before,'database_query_count')} / {_metric(after,'database_query_count')} | {row['measured_10x_groups']}/{row['recorded_groups']} | {row['target_status']}; {row['latency_floor_status']} |")
    lines.extend(["", "## Coverage gaps", ""])
    for name, coverage in report["coverage"].items():
        if coverage is None:
            continue
        lines.append(f"- {name} missing RPC names: {', '.join(coverage['missing_rpc_names']) or 'none'}.")
        for pair in coverage["missing_backend_implementations"]:
            lines.append(f"  - {pair['backend']}: {', '.join(pair['rpc_names'])}")
        if coverage["unknown_methods"] or coverage["unknown_backend_records"]:
            lines.append(f"  - Unknown methods: {coverage['unknown_methods']}; unknown-backend records: {coverage['unknown_backend_records']}.")
    lines.extend(["", "## Per-test comparisons", "", "| Backend | Test | Method | Successful samples before / after | Errors before / after | Comparable | Median speedup | Measured 10× | Reason |", "|---|---|---|---:|---:|---|---:|---|---|"])
    for row in report["groups"]:
        before, after = row["baseline"], row["candidate"]
        count = after["successful"]["samples"] if after else 0
        errors = after["errors"]["samples"] if after else 0
        gain = f"{row['median_speedup']:.3f}×" if row["median_speedup"] is not None else "—"
        lines.append(f"| {row['backend']} | {row['test']} | {row['method']} | {before['successful']['samples']} / {count} | {before['errors']['samples']} / {errors} | {row['comparable']} | {gain} | {row['measured_10x']} | {', '.join(row['incomparable_reasons'])} |")
    lines.extend(["", "## Interpretation limits", ""] + [f"- {note}" for note in report["limitations"]])
    return "\n".join(lines) + "\n"


def write_csv(path: Path, report: dict) -> None:
    metrics = ["elapsed_ms", "database_query_count", "database_elapsed_ms"]
    statistics_names = ["median", "p95", "min", "max", "total"]
    fields = ["backend", "test", "method", "rpc_names", "baseline_success", "candidate_success", "baseline_errors", "candidate_errors"]
    fields += [f"{cohort}_{outcome}_{metric}_{statistic}" for cohort in ["baseline", "candidate"] for outcome in ["successful", "errors"] for metric in metrics for statistic in statistics_names]
    fields += ["comparable", "median_speedup", "measured_10x", "latency_floor_status", "incomparable_reasons"]
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in report["groups"]:
            before, after = row["baseline"], row["candidate"]
            output = {
                "backend": row["backend"], "test": row["test"], "method": row["method"], "rpc_names": "|".join(row["rpc_names"]),
                "baseline_success": before["successful"]["samples"], "candidate_success": after["successful"]["samples"] if after else 0,
                "baseline_errors": before["errors"]["samples"], "candidate_errors": after["errors"]["samples"] if after else 0,
                **{key: row[key] for key in ["comparable", "median_speedup", "measured_10x", "latency_floor_status"]},
                "incomparable_reasons": "|".join(row["incomparable_reasons"]),
            }
            for cohort, values in [("baseline", before), ("candidate", after)]:
                for outcome in ["successful", "errors"]:
                    for metric in metrics:
                        stats = values and values[outcome][metric]
                        for statistic in statistics_names:
                            output[f"{cohort}_{outcome}_{metric}_{statistic}"] = stats[statistic] if stats else None
            writer.writerow(output)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, action="append", required=True)
    parser.add_argument("--candidate", type=Path, action="append", default=[])
    parser.add_argument("--rpc-source", type=Path, default=DEFAULT_RPC_SOURCE)
    parser.add_argument("--baseline-test-log", type=Path, action="append", default=[])
    parser.add_argument("--candidate-test-log", type=Path, action="append", default=[])
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--csv-output", type=Path)
    parser.add_argument("--min-samples", type=int, default=3)
    parser.add_argument("--latency-floor-us", type=float, default=1000)
    args = parser.parse_args(argv)
    if args.min_samples < 1 or not math.isfinite(args.latency_floor_us) or args.latency_floor_us < 0:
        parser.error("--min-samples must be positive and --latency-floor-us must be finite and nonnegative")
    try:
        before, before_inputs = load_profiles(args.baseline)
        after, after_inputs = load_profiles(args.candidate) if args.candidate else (None, [])
        before_logs = list(dict.fromkeys(args.baseline_test_log + [path.with_suffix('.out') for path in args.baseline if path.with_suffix('.out').is_file()]))
        after_logs = list(dict.fromkeys(args.candidate_test_log + [path.with_suffix('.out') for path in args.candidate if path.with_suffix('.out').is_file()]))
        mapping = rpc_methods(args.rpc_source.read_text())
        report = build_report(before, after, mapping, load_test_logs(before_logs), load_test_logs(after_logs), args.min_samples, args.latency_floor_us)
        report["inputs"] = {"baseline": before_inputs, "candidate": after_inputs, "rpc_source": str(args.rpc_source), "rpc_source_sha256": hashlib.sha256(args.rpc_source.read_bytes()).hexdigest()}
        outputs = [path for path in [args.json_output, args.markdown_output, args.csv_output] if path is not None]
        inputs = {path.resolve() for path in args.baseline + args.candidate + before_logs + after_logs + [args.rpc_source]}
        if len({path.resolve() for path in outputs}) != len(outputs) or any(path.resolve() in inputs for path in outputs):
            raise ValueError("Output paths must be distinct and must not overwrite input files")
        for path in outputs:
            path.parent.mkdir(parents=True, exist_ok=True)
        if args.json_output:
            args.json_output.write_text(json.dumps(report, indent=2) + "\n")
        if args.markdown_output:
            args.markdown_output.write_text(markdown_report(report))
        if args.csv_output:
            write_csv(args.csv_output, report)
        if not outputs:
            print(markdown_report(report), end="")
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    sys.exit(main())
