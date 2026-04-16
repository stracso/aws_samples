#!/usr/bin/env python3
"""
Performance comparison: AsyncCursor vs AioCursor in PyAthena.

AsyncCursor  – uses concurrent.futures.ThreadPoolExecutor (sync connect())
AioCursor    – uses native asyncio (async aio_connect())

Usage:
    python async_vs_aio_perf_test.py \
        --table <database.table_name> \
        --s3-staging-dir s3://bucket/path/ \
        --region us-west-2 \
        [--iterations 3] \
        [--max-workers 5] \
        [--row-limit 10000] \
        [--profile <aws_profile>] \
        [--work-group <workgroup>]

Docs: https://pyathena.dev/v3.30.0/aio.html
      https://pyathena.dev/v3.30.0/cursor.html
"""

import argparse
import asyncio
import json
import statistics
import time
from concurrent.futures import as_completed
from dataclasses import dataclass, field

from pyathena import connect, aio_connect
from pyathena.async_cursor import AsyncCursor
from pyathena.aio.cursor import AioCursor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class QueryMetrics:
    """Holds timing and Athena-reported metrics for a single query run."""
    wall_clock_s: float = 0.0
    engine_execution_ms: int = 0
    total_execution_ms: int = 0
    query_queue_ms: int = 0
    query_planning_ms: int = 0
    service_processing_ms: int = 0
    data_scanned_bytes: int = 0
    row_count: int = 0


@dataclass
class BenchmarkResult:
    """Aggregated results for one cursor type across all iterations."""
    cursor_type: str
    runs: list = field(default_factory=list)

    def summary(self) -> dict:
        walls = [r.wall_clock_s for r in self.runs]
        engines = [r.engine_execution_ms for r in self.runs]
        totals = [r.total_execution_ms for r in self.runs]
        scanned = [r.data_scanned_bytes for r in self.runs]
        rows = [r.row_count for r in self.runs]
        return {
            "cursor_type": self.cursor_type,
            "iterations": len(self.runs),
            "wall_clock_s": {
                "mean": round(statistics.mean(walls), 3),
                "median": round(statistics.median(walls), 3),
                "stdev": round(statistics.stdev(walls), 3) if len(walls) > 1 else 0,
                "min": round(min(walls), 3),
                "max": round(max(walls), 3),
            },
            "engine_execution_ms": {
                "mean": round(statistics.mean(engines)),
                "median": round(statistics.median(engines)),
            },
            "total_execution_ms": {
                "mean": round(statistics.mean(totals)),
                "median": round(statistics.median(totals)),
            },
            "avg_data_scanned_bytes": round(statistics.mean(scanned)),
            "avg_row_count": round(statistics.mean(rows)),
        }


def _build_query(table: str, row_limit: int | None) -> str:
    q = f"SELECT * FROM {table}"
    if row_limit:
        q += f" LIMIT {row_limit}"
    return q


def _connect_kwargs(args: argparse.Namespace) -> dict:
    """Build common connection kwargs from CLI args."""
    kwargs = {"region_name": args.region}
    if args.s3_staging_dir:
        kwargs["s3_staging_dir"] = args.s3_staging_dir
    if args.profile:
        kwargs["profile_name"] = args.profile
    if args.work_group:
        kwargs["work_group"] = args.work_group
    return kwargs


# ---------------------------------------------------------------------------
# AsyncCursor benchmark (thread-pool based)
# ---------------------------------------------------------------------------

def bench_async_cursor_single(args: argparse.Namespace, query: str) -> QueryMetrics:
    """Run a single query with AsyncCursor and return metrics."""
    conn_kwargs = _connect_kwargs(args)
    conn = connect(**conn_kwargs)
    cursor = conn.cursor(AsyncCursor, max_workers=args.max_workers)

    t0 = time.perf_counter()
    query_id, future = cursor.execute(query)
    result_set = future.result()
    rows = result_set.fetchall()
    wall = time.perf_counter() - t0

    m = QueryMetrics(
        wall_clock_s=wall,
        engine_execution_ms=result_set.engine_execution_time_in_millis or 0,
        total_execution_ms=result_set.total_execution_time_in_millis or 0,
        query_queue_ms=result_set.query_queue_time_in_millis or 0,
        query_planning_ms=result_set.query_planning_time_in_millis or 0,
        service_processing_ms=result_set.service_processing_time_in_millis or 0,
        data_scanned_bytes=result_set.data_scanned_in_bytes or 0,
        row_count=len(rows),
    )
    conn.close()
    return m


def bench_async_cursor_concurrent(args: argparse.Namespace, query: str) -> list[QueryMetrics]:
    """Fire N queries concurrently via AsyncCursor and return all metrics."""
    n = args.concurrent_queries
    conn_kwargs = _connect_kwargs(args)
    conn = connect(**conn_kwargs)
    cursor = conn.cursor(AsyncCursor, max_workers=args.max_workers)

    futures = {}
    t0 = time.perf_counter()
    for i in range(n):
        qid, fut = cursor.execute(query)
        futures[fut] = (qid, time.perf_counter())

    metrics = []
    for fut in as_completed(futures):
        _, submit_t = futures[fut]
        rs = fut.result()
        wall = time.perf_counter() - submit_t
        rows = rs.fetchall()
        metrics.append(QueryMetrics(
            wall_clock_s=wall,
            engine_execution_ms=rs.engine_execution_time_in_millis or 0,
            total_execution_ms=rs.total_execution_time_in_millis or 0,
            query_queue_ms=rs.query_queue_time_in_millis or 0,
            query_planning_ms=rs.query_planning_time_in_millis or 0,
            service_processing_ms=rs.service_processing_time_in_millis or 0,
            data_scanned_bytes=rs.data_scanned_in_bytes or 0,
            row_count=len(rows),
        ))
    conn.close()
    total_wall = time.perf_counter() - t0
    print(f"    AsyncCursor concurrent ({n} queries) total wall: {total_wall:.3f}s")
    return metrics


# ---------------------------------------------------------------------------
# AioCursor benchmark (native asyncio)
# ---------------------------------------------------------------------------

async def bench_aio_cursor_single(args: argparse.Namespace, query: str) -> QueryMetrics:
    """Run a single query with AioCursor and return metrics."""
    conn_kwargs = _connect_kwargs(args)
    async with await aio_connect(**conn_kwargs) as conn:
        async with conn.cursor() as cursor:
            t0 = time.perf_counter()
            await cursor.execute(query)
            rows = await cursor.fetchall()
            wall = time.perf_counter() - t0

            return QueryMetrics(
                wall_clock_s=wall,
                engine_execution_ms=cursor.engine_execution_time_in_millis or 0,
                total_execution_ms=cursor.total_execution_time_in_millis or 0,
                query_queue_ms=cursor.query_queue_time_in_millis or 0,
                query_planning_ms=cursor.query_planning_time_in_millis or 0,
                service_processing_ms=cursor.service_processing_time_in_millis or 0,
                data_scanned_bytes=cursor.data_scanned_in_bytes or 0,
                row_count=len(rows),
            )


async def _aio_single_query(conn, query: str) -> QueryMetrics:
    """Helper: execute one query on an existing aio connection."""
    async with conn.cursor() as cursor:
        t0 = time.perf_counter()
        await cursor.execute(query)
        rows = await cursor.fetchall()
        wall = time.perf_counter() - t0
        return QueryMetrics(
            wall_clock_s=wall,
            engine_execution_ms=cursor.engine_execution_time_in_millis or 0,
            total_execution_ms=cursor.total_execution_time_in_millis or 0,
            query_queue_ms=cursor.query_queue_time_in_millis or 0,
            query_planning_ms=cursor.query_planning_time_in_millis or 0,
            service_processing_ms=cursor.service_processing_time_in_millis or 0,
            data_scanned_bytes=cursor.data_scanned_in_bytes or 0,
            row_count=len(rows),
        )


async def bench_aio_cursor_concurrent(args: argparse.Namespace, query: str) -> list[QueryMetrics]:
    """Fire N queries concurrently via AioCursor using asyncio.gather."""
    n = args.concurrent_queries
    conn_kwargs = _connect_kwargs(args)
    async with await aio_connect(**conn_kwargs) as conn:
        t0 = time.perf_counter()
        tasks = [_aio_single_query(conn, query) for _ in range(n)]
        metrics = await asyncio.gather(*tasks)
        total_wall = time.perf_counter() - t0
        print(f"    AioCursor concurrent ({n} queries) total wall: {total_wall:.3f}s")
        return list(metrics)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def print_comparison(async_result: BenchmarkResult, aio_result: BenchmarkResult):
    a = async_result.summary()
    b = aio_result.summary()

    print("\n" + "=" * 72)
    print("PERFORMANCE COMPARISON: AsyncCursor vs AioCursor")
    print("=" * 72)

    for label, key in [
        ("Wall Clock (s)", "wall_clock_s"),
        ("Engine Execution (ms)", "engine_execution_ms"),
        ("Total Execution (ms)", "total_execution_ms"),
    ]:
        print(f"\n  {label}:")
        av = a[key]
        bv = b[key]
        print(f"    {'':20s} {'AsyncCursor':>14s} {'AioCursor':>14s}")
        for stat in ["mean", "median"]:
            print(f"    {stat:20s} {av[stat]:>14} {bv[stat]:>14}")
        if "stdev" in av:
            print(f"    {'stdev':20s} {av['stdev']:>14} {bv['stdev']:>14}")

    print(f"\n  Avg Data Scanned (bytes):")
    print(f"    AsyncCursor: {a['avg_data_scanned_bytes']:,}")
    print(f"    AioCursor:   {b['avg_data_scanned_bytes']:,}")

    print(f"\n  Avg Row Count:")
    print(f"    AsyncCursor: {a['avg_row_count']:,}")
    print(f"    AioCursor:   {b['avg_row_count']:,}")

    # Wall-clock delta
    delta = a["wall_clock_s"]["mean"] - b["wall_clock_s"]["mean"]
    if a["wall_clock_s"]["mean"] > 0:
        pct = (delta / a["wall_clock_s"]["mean"]) * 100
    else:
        pct = 0
    faster = "AioCursor" if delta > 0 else "AsyncCursor"
    print(f"\n  Wall-clock delta (mean): {abs(delta):.3f}s  →  {faster} is ~{abs(pct):.1f}% faster")
    print("=" * 72)


async def run_benchmarks(args: argparse.Namespace):
    query = _build_query(args.table, args.row_limit)
    print(f"Query: {query}")
    print(f"Iterations: {args.iterations}  |  Max workers: {args.max_workers}")
    if args.concurrent_queries > 1:
        print(f"Concurrent queries per iteration: {args.concurrent_queries}")
    print()

    # --- Sequential single-query benchmarks ---
    async_result = BenchmarkResult(cursor_type="AsyncCursor")
    aio_result = BenchmarkResult(cursor_type="AioCursor")

    for i in range(1, args.iterations + 1):
        print(f"--- Iteration {i}/{args.iterations} ---")

        # AsyncCursor (sync call)
        print("  Running AsyncCursor …")
        m = bench_async_cursor_single(args, query)
        async_result.runs.append(m)
        print(f"    wall={m.wall_clock_s:.3f}s  engine={m.engine_execution_ms}ms  rows={m.row_count}")

        # AioCursor (native async)
        print("  Running AioCursor …")
        m = await bench_aio_cursor_single(args, query)
        aio_result.runs.append(m)
        print(f"    wall={m.wall_clock_s:.3f}s  engine={m.engine_execution_ms}ms  rows={m.row_count}")

    print_comparison(async_result, aio_result)

    # --- Concurrent multi-query benchmark (if requested) ---
    if args.concurrent_queries > 1:
        print(f"\n{'=' * 72}")
        print(f"CONCURRENT BENCHMARK ({args.concurrent_queries} parallel queries)")
        print(f"{'=' * 72}\n")

        async_conc = BenchmarkResult(cursor_type="AsyncCursor (concurrent)")
        aio_conc = BenchmarkResult(cursor_type="AioCursor (concurrent)")

        for i in range(1, args.iterations + 1):
            print(f"--- Iteration {i}/{args.iterations} ---")

            print("  Running AsyncCursor concurrent …")
            ms = bench_async_cursor_concurrent(args, query)
            async_conc.runs.extend(ms)

            print("  Running AioCursor concurrent …")
            ms = await bench_aio_cursor_concurrent(args, query)
            aio_conc.runs.extend(ms)

        print_comparison(async_conc, aio_conc)

    # Dump raw JSON for further analysis
    output = {
        "sequential": {
            "async_cursor": async_result.summary(),
            "aio_cursor": aio_result.summary(),
        },
    }
    if args.concurrent_queries > 1:
        output["concurrent"] = {
            "async_cursor": async_conc.summary(),
            "aio_cursor": aio_conc.summary(),
        }
    print(f"\n--- Raw JSON ---\n{json.dumps(output, indent=2)}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark AsyncCursor vs AioCursor in PyAthena"
    )
    p.add_argument("--table", required=True,
                   help="Fully qualified Athena table (database.table)")
    p.add_argument("--s3-staging-dir", default=None,
                   help="S3 staging dir for query results")
    p.add_argument("--region", default="us-west-2",
                   help="AWS region (default: us-west-2)")
    p.add_argument("--profile", default=None,
                   help="AWS profile name")
    p.add_argument("--work-group", default=None,
                   help="Athena workgroup")
    p.add_argument("--iterations", type=int, default=3,
                   help="Number of iterations per cursor type (default: 3)")
    p.add_argument("--max-workers", type=int, default=5,
                   help="Thread pool size for AsyncCursor (default: 5)")
    p.add_argument("--row-limit", type=int, default=None,
                   help="LIMIT clause for the query (default: no limit)")
    p.add_argument("--concurrent-queries", type=int, default=1,
                   help="Number of concurrent queries per iteration (default: 1, >1 enables concurrency test)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_benchmarks(args))
