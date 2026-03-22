#!/usr/bin/env python3

import argparse
import multiprocessing
import platform
import re
import subprocess
import sys
from pathlib import Path


NUM_TEST_RUNS = 3
PERF_THRESHOLD = 1.5
TIMEOUT_SECS = 120

ASYNC_TIMING_TESTS = [
    "ping_pong_equal_async",
    "ping_pong_unequal_async",
    "super_light_async",
    "super_super_light_async",
    "recursive_fibonacci_async",
    "math_operations_in_tight_for_loop_async",
    "math_operations_in_tight_for_loop_fewer_tasks_async",
    "math_operations_in_tight_for_loop_fan_in_async",
    "math_operations_in_tight_for_loop_reduction_tree_async",
    "mandelbrot_chunked_async",
    "spin_between_run_calls_async",
]

ASYNC_CORRECTNESS_ONLY_TESTS = [
    "simple_test_async",
    "simple_run_deps_test",
    "strict_diamond_deps_async",
    "strict_graph_deps_small_async",
    "strict_graph_deps_med_async",
    "strict_graph_deps_large_async",
]

IMPLEMENTATIONS = [
    "[Serial]",
    "[Parallel + Always Spawn]",
    "[Parallel + Thread Pool + Spin]",
    "[Parallel + Thread Pool + Sleep]",
]

AUTHORS = ["STUDENT", "REFERENCE"]


def detect_reference_binary() -> str:
    if platform.system() == "Darwin":
        if platform.machine() == "arm64":
            return "runtasks_ref_osx_arm"
        return "runtasks_ref_osx_x86"
    if platform.machine() == "aarch64":
        return "runtasks_ref_linux_arm"
    return "runtasks_ref_linux"


def parse_runtimes(output: str, is_reference: bool) -> dict[str, float]:
    runtimes = {}
    for line in output.splitlines():
        match = re.match(r"\[(.*)\]:\s+\[(\d+\.\d+)\] ms", line)
        if match is None:
            continue
        impl = match.group(1)
        runtime = float(match.group(2))
        prefix = "REFERENCE" if is_reference else "STUDENT"
        runtimes[f"{prefix} [{impl}]"] = runtime
    return runtimes


def run_once(cmd: list[str], is_reference: bool, timeout_secs: int) -> dict[str, float]:
    try:
        proc = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout_secs,
        )
    except subprocess.TimeoutExpired as exc:
        print(f"TIMEOUT: {' '.join(cmd)}", file=sys.stderr)
        if exc.stdout:
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        return {}
    except subprocess.CalledProcessError as exc:
        print(f"FAILED: {' '.join(cmd)}", file=sys.stderr)
        if exc.stdout:
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        return {}

    return parse_runtimes(proc.stdout, is_reference=is_reference)


def pretty_print(test_name: str, runtimes: dict[str, float], show_all_impls: bool) -> None:
    print(f"Results for: {test_name}")
    print(f"{'Implementation':<40}{'Student':<12}{'Reference':<12}{'Ratio':<8}Perf?")

    impls = IMPLEMENTATIONS if show_all_impls else ["[Parallel + Thread Pool + Sleep]"]
    for impl in impls:
        student_key = f"{AUTHORS[0]} {impl}"
        ref_key = f"{AUTHORS[1]} {impl}"
        student_time = runtimes.get(student_key)
        ref_time = runtimes.get(ref_key)

        if student_time is None or ref_time is None:
            print(f"{impl:<40}{'Missing':<12}{'Missing':<12}{'-':<8}N/A")
            continue

        ratio = student_time / ref_time if ref_time > 0 else float("inf")
        ok = ratio < PERF_THRESHOLD
        verdict = "OK" if ok else "SLOW"
        print(f"{impl:<40}{student_time:<12.3f}{ref_time:<12.3f}{ratio:<8.2f}{verdict}")


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    default_threads = multiprocessing.cpu_count()

    parser = argparse.ArgumentParser(description="Run async-only task system timing harness")
    parser.add_argument(
        "-n",
        "--num_threads",
        type=int,
        default=default_threads,
        help=f"Max number of threads for runtasks (default: {default_threads})",
    )
    parser.add_argument(
        "-t",
        "--tests",
        nargs="+",
        default=ASYNC_TIMING_TESTS,
        help="Subset of async tests to run",
    )
    parser.add_argument(
        "-r",
        "--runs",
        type=int,
        default=NUM_TEST_RUNS,
        help=f"Number of repeated runs per test (default: {NUM_TEST_RUNS})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=TIMEOUT_SECS,
        help=f"Per-process timeout in seconds (default: {TIMEOUT_SECS})",
    )
    parser.add_argument(
        "--student-only",
        action="store_true",
        help="Do not compare against reference binary",
    )
    parser.add_argument(
        "--include-correctness-only",
        action="store_true",
        help="Also include async correctness/debug tests that the reference binary may not support",
    )
    parser.add_argument(
        "--all-impls",
        action="store_true",
        help="Print all implementations instead of only Parallel + Thread Pool + Sleep",
    )
    args = parser.parse_args()

    selected_tests = list(args.tests)
    if args.include_correctness_only:
        for test_name in ASYNC_CORRECTNESS_ONLY_TESTS:
            if test_name not in selected_tests:
                selected_tests.append(test_name)

    print("======================================================================")
    print(f"Running async-only harness... ({len(selected_tests)} total tests)")
    print(f"  - Task system configured to use at most {args.num_threads} threads")
    print(f"  - Repeats per test: {args.runs}")
    print("======================================================================")

    subprocess.run(["make"], cwd=script_dir, check=True, stdout=subprocess.DEVNULL)

    student_bin = script_dir / "runtasks"
    ref_bin = script_dir / detect_reference_binary()

    all_ok = True

    for test_name in selected_tests:
        print("======================================================================")
        print(f"Executing test: {test_name}")

        collected: dict[str, list[float]] = {}
        commands = [([str(student_bin), "-n", str(args.num_threads), "-i", "1", test_name], False)]
        reference_supported = test_name not in ASYNC_CORRECTNESS_ONLY_TESTS
        if not args.student_only and reference_supported:
            commands.insert(0, ([str(ref_bin), "-n", str(args.num_threads), "-i", "1", test_name], True))

        for _ in range(args.runs):
            for cmd, is_reference in commands:
                runtimes = run_once(cmd, is_reference=is_reference, timeout_secs=args.timeout)
                if not runtimes:
                    all_ok = False
                    continue
                for key, value in runtimes.items():
                    collected.setdefault(key, []).append(value)

        mins = {key: min(values) for key, values in collected.items() if values}

        if args.student_only or not reference_supported:
            print(f"Results for: {test_name}")
            if not args.student_only and not reference_supported:
                print("Reference: skipped for this test (not supported by reference binary)")
            impls = IMPLEMENTATIONS if args.all_impls else ["[Parallel + Thread Pool + Sleep]"]
            for impl in impls:
                key = f"STUDENT {impl}"
                if key in mins:
                    print(f"{impl:<40}{mins[key]:.3f} ms")
                else:
                    print(f"{impl:<40}Missing")
                    all_ok = False
        else:
            pretty_print(test_name, mins, show_all_impls=args.all_impls)
            sleep_student = mins.get("STUDENT [Parallel + Thread Pool + Sleep]")
            sleep_ref = mins.get("REFERENCE [Parallel + Thread Pool + Sleep]")
            if sleep_student is None or sleep_ref is None:
                all_ok = False

    print("======================================================================")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
