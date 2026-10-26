import json
import tempfile
import unittest
from pathlib import Path

import profile_api as profile


MAPPING = {"getFoo": "get_foo", "getFooAlias": "get_foo", "liveness": "liveness"}


def sample(elapsed_ns, success=True, test="fixture::DatabaseBackend__Postgres", method="get_foo"):
    return dict(method=method, test=test, backend="Postgres", success=success, elapsed_ns=elapsed_ns,
                database_queries=2, database_elapsed_ns=elapsed_ns // 2)


class ProfileApiTests(unittest.TestCase):
    def test_registry_tracks_aliases_and_ignores_commented_registrations(self):
        source = '''// module.register_async_method("bogus", |p,c| api.get_bogus()).unwrap();
        module.register_async_method("getFoo", |p,c| async { api.get_foo(p).await });
        module.register_async_method("getFooAlias", |p,c| async { c.as_ref().get_foo(p).await });
        module.register_async_method("liveness", |p,c| async { api.liveness().await });'''
        self.assertEqual(profile.rpc_methods(source), MAPPING)

    def test_backend_detection_does_not_mix_unknown_or_conflicting_backends(self):
        self.assertEqual(profile.backend_for({"test": "x::backend_1_DatabaseBackend__Sqlite"}), "Sqlite")
        self.assertEqual(profile.backend_for({"test": "x"}), "Unknown")
        with self.assertRaises(ValueError):
            profile.backend_for({"test": "DatabaseBackend__Sqlite", "backend": "Postgres"})

    def test_only_comparable_successful_groups_can_claim_tenfold(self):
        baseline = [sample(100_000_000)] * 3 + [sample(1, False)]
        candidate = [sample(10_000_000)] * 3 + [sample(1_000_000_000, False)]
        report = profile.build_report(baseline, candidate, MAPPING)
        group = report["groups"][0]
        self.assertEqual((group["median_speedup"], group["measured_10x"], group["comparable"]), (10.0, True, True))
        self.assertEqual((group["baseline"]["successful"]["samples"], group["baseline"]["errors"]["samples"]), (3, 1))
        self.assertEqual(group["candidate"]["errors"]["elapsed_ms"]["median"], 1000.0)

    def test_changed_sample_or_error_counts_invalidate_comparison(self):
        for candidate in [[sample(1)] * 4, [sample(1)] * 3 + [sample(1, False)]]:
            report = profile.build_report([sample(100)] * 3, candidate, MAPPING)
            group = report["groups"][0]
            self.assertEqual((group["comparable"], group["measured_10x"], group["median_speedup"]), (False, False, None))

    def test_workload_mix_cannot_create_a_spurious_aggregate_gain(self):
        before = [sample(1_000_000, test="fast")] * 3 + [sample(100_000_000, test="slow")] * 30
        after = [sample(1_000_000, test="fast")] * 30 + [sample(100_000_000, test="slow")] * 3
        report = profile.build_report(before, after, MAPPING)
        self.assertEqual(report["measured_10x_groups"], 0)
        self.assertTrue(all(not row["comparable"] for row in report["groups"]))

    def test_zero_duration_missing_candidate_and_failed_test_never_claim_gain(self):
        cases = [
            profile.build_report([sample(10)] * 3, [sample(0)] * 3, MAPPING),
            profile.build_report([sample(10)] * 3, None, MAPPING),
            profile.build_report([sample(100)] * 3, [sample(1)] * 3, MAPPING,
                                 candidate_logs=[{"failures": [{"test": "fixture::DatabaseBackend__Postgres"}]}]),
        ]
        for report in cases:
            self.assertEqual(report["measured_10x_groups"], 0)
            self.assertFalse(report["groups"][0]["comparable"])

    def test_nearest_rank_percentile_and_reference_floor_are_explicit(self):
        self.assertEqual(profile.distribution(list(range(1, 21)))["p95"], 19)
        report = profile.build_report([sample(10_000)] * 3, None, MAPPING, floor_us=100)
        self.assertEqual(report["groups"][0]["latency_floor_status"], "below_reference_threshold")
        self.assertEqual(report["coverage"]["baseline"]["missing_rpc_names"], ["liveness"])

    def test_loader_rejects_sql_only_or_negative_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.jsonl"
            for row in [{"Execution Time": 1.0}, sample(-1)]:
                path.write_text(json.dumps(row) + "\n")
                with self.assertRaises(ValueError):
                    profile.load_profiles([path])

    def test_duplicate_input_paths_do_not_inflate_sample_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.jsonl"
            path.write_text(json.dumps(sample(1)) + "\n")
            with self.assertRaises(ValueError):
                profile.load_profiles([path, path])

    def test_full_test_log_failures_are_retained(self):
        text = '''test merkle_tree_deserialization::bad ... FAILED
test open_api_tests::bad ... FAILED
---- merkle_tree_deserialization::bad stdout ----
RPC error sending request to localhost
---- open_api_tests::bad stdout ----
Os { kind: NotFound }
test result: FAILED. 70 passed; 3 failed; 4 ignored;
'''
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.out"
            path.write_text(text)
            log = profile.load_test_logs([path])[0]
        self.assertEqual((log["status"], log["summaries"][0]["failed"]), ("failed", 3))
        self.assertEqual([row["category"] for row in log["failures"]], ["network_dependency", "external_tool_dependency"])

    def test_cli_reads_repeated_inputs_writes_all_formats_and_infers_test_log(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "rpc.rs"
            source.write_text('module.register_async_method("getFoo", |p,c| api.get_foo(p));')
            output, markdown, csv = root / "out.json", root / "out.md", root / "out.csv"
            args = ["--rpc-source", str(source), "--json-output", str(output), "--markdown-output", str(markdown), "--csv-output", str(csv)]
            for index in range(3):
                before, after = root / f"before-{index}.jsonl", root / f"after-{index}.jsonl"
                before.write_text(json.dumps(sample(100_000_000)) + "\n")
                after.write_text(json.dumps(sample(10_000_000)) + "\n")
                args += ["--baseline", str(before), "--candidate", str(after)]
            (root / 'before-0.out').write_text("test result: FAILED. 70 passed; 3 failed; 4 ignored;\n")
            self.assertEqual(profile.main(args), 0)
            report = json.loads(output.read_text())
            self.assertEqual((report["coverage"]["baseline"]["records"], report["measured_10x_groups"]), (3, 1))
            self.assertEqual(report["test_runs"]["baseline"][0]["status"], "failed")
            self.assertIn("70 passed, 3 failed", markdown.read_text())
            self.assertIn("measured_10x", csv.read_text())


if __name__ == "__main__":
    unittest.main()
