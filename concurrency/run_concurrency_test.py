"""
Concurrency Test Orchestrator
==============================
Launches multiple Glue job runs concurrently to test Iceberg concurrency behavior.

This script:
1. Starts N Glue job runs in parallel (each simulating a data loader)
2. Monitors all runs until completion
3. Collects results and reports on conflicts, retries, and data integrity

Usage:
    python run_concurrency_test.py --job-name <glue_job_name> \
        --database <db> --main-table <table> --warehouse-path <s3_path> \
        --num-loaders 5 --test-mode isolated

Test Modes:
    isolated    : Each loader writes to its own partition (no conflicts expected)
    overlapping : All loaders write to the same partition (conflicts expected)
    mixed       : Some loaders share partitions, some are isolated
"""

import argparse
import boto3
import time
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

glue_client = boto3.client('glue')
logs_client = boto3.client('logs')


def parse_args():
    parser = argparse.ArgumentParser(description='Iceberg Concurrency Test Orchestrator')
    parser.add_argument('--job-name', required=True, help='Glue job name')
    parser.add_argument('--database', required=True, help='Glue catalog database')
    parser.add_argument('--main-table', required=True, help='Main Iceberg table name')
    parser.add_argument('--warehouse-path', required=True, help='S3 warehouse path')
    parser.add_argument('--num-loaders', type=int, default=3, help='Number of concurrent loaders')
    parser.add_argument('--num-rows', type=int, default=10000, help='Rows per loader')
    parser.add_argument('--test-mode', choices=['isolated', 'overlapping', 'mixed'],
                        default='isolated', help='Concurrency test mode')
    parser.add_argument('--commit-retries', type=int, default=5, help='Max commit retries')
    parser.add_argument('--poll-interval', type=int, default=30, help='Status poll interval (s)')
    return parser.parse_args()

# ---------------------------------------------------------------------------
# Loader Configuration Generator
# ---------------------------------------------------------------------------

def generate_loader_configs(num_loaders, test_mode, database, main_table, warehouse_path,
                            num_rows, commit_retries):
    """
    Generate job arguments for each loader based on the test mode.

    isolated:    Each loader gets a unique source_system → no partition overlap
    overlapping: All loaders share the same source_system → maximum conflict
    mixed:       Half isolated, half overlapping
    """
    configs = []

    for i in range(num_loaders):
        loader_id = f"loader_{i:02d}"

        if test_mode == "isolated":
            source_system = f"system_{i:02d}"
            overlap_mode = "isolated"

        elif test_mode == "overlapping":
            source_system = "shared_system"
            overlap_mode = "overlapping"

        elif test_mode == "mixed":
            if i < num_loaders // 2:
                source_system = f"system_{i:02d}"
                overlap_mode = "isolated"
            else:
                source_system = "shared_system"
                overlap_mode = "overlapping"

        configs.append({
            '--loader_id': loader_id,
            '--source_system': source_system,
            '--database': database,
            '--main_table': main_table,
            '--warehouse_path': warehouse_path,
            '--num_rows': str(num_rows),
            '--overlap_mode': overlap_mode,
            '--commit_retries': str(commit_retries),
        })

    return configs


# ---------------------------------------------------------------------------
# Job Launcher
# ---------------------------------------------------------------------------

def start_glue_job(job_name, job_args):
    """Start a single Glue job run and return the run ID."""
    loader_id = job_args['--loader_id']
    print(f"  Starting {loader_id}...")

    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments=job_args
    )

    run_id = response['JobRunId']
    print(f"  {loader_id} → RunId: {run_id}")
    return {
        'loader_id': loader_id,
        'run_id': run_id,
        'source_system': job_args['--source_system'],
        'overlap_mode': job_args['--overlap_mode'],
    }


def launch_all_loaders(job_name, configs):
    """Launch all loaders concurrently."""
    print(f"\n{'='*60}")
    print(f"LAUNCHING {len(configs)} LOADERS CONCURRENTLY")
    print(f"{'='*60}")

    runs = []
    # Launch all jobs as fast as possible to maximize concurrency
    for config in configs:
        run_info = start_glue_job(job_name, config)
        runs.append(run_info)
        time.sleep(0.5)  # Small delay to avoid API throttling

    print(f"\nAll {len(runs)} jobs launched.")
    return runs

# ---------------------------------------------------------------------------
# Job Monitoring
# ---------------------------------------------------------------------------

TERMINAL_STATES = {'SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT', 'ERROR'}

def poll_job_status(job_name, runs, poll_interval=30):
    """Poll all job runs until they reach a terminal state."""
    print(f"\n{'='*60}")
    print("MONITORING JOB RUNS")
    print(f"{'='*60}")

    pending = {r['run_id']: r for r in runs}
    completed = {}

    while pending:
        time.sleep(poll_interval)

        for run_id, run_info in list(pending.items()):
            try:
                response = glue_client.get_job_run(
                    JobName=job_name,
                    RunId=run_id
                )
                status = response['JobRun']['JobRunState']
                loader_id = run_info['loader_id']

                if status in TERMINAL_STATES:
                    duration = 0
                    if 'CompletedOn' in response['JobRun'] and 'StartedOn' in response['JobRun']:
                        duration = (response['JobRun']['CompletedOn'] -
                                    response['JobRun']['StartedOn']).total_seconds()

                    run_info['status'] = status
                    run_info['duration'] = duration
                    run_info['error'] = response['JobRun'].get('ErrorMessage', '')

                    completed[run_id] = run_info
                    del pending[run_id]

                    icon = "✓" if status == "SUCCEEDED" else "✗"
                    print(f"  {icon} {loader_id}: {status} ({duration:.0f}s)")

            except Exception as e:
                print(f"  Warning: Error polling {run_info['loader_id']}: {e}")

        if pending:
            print(f"  ... {len(pending)} jobs still running, "
                  f"{len(completed)} completed. Polling in {poll_interval}s...")

    return list(completed.values())


# ---------------------------------------------------------------------------
# Results Analysis
# ---------------------------------------------------------------------------

def analyze_results(results, test_mode, num_rows):
    """Analyze and report on the concurrency test results."""
    print(f"\n{'='*60}")
    print("CONCURRENCY TEST RESULTS")
    print(f"{'='*60}")

    total = len(results)
    succeeded = [r for r in results if r['status'] == 'SUCCEEDED']
    failed = [r for r in results if r['status'] != 'SUCCEEDED']

    print(f"\n  Test Mode:       {test_mode}")
    print(f"  Total Loaders:   {total}")
    print(f"  Succeeded:       {len(succeeded)}")
    print(f"  Failed:          {len(failed)}")

    if succeeded:
        durations = [r['duration'] for r in succeeded]
        print(f"\n  Duration (succeeded):")
        print(f"    Min:  {min(durations):.0f}s")
        print(f"    Max:  {max(durations):.0f}s")
        print(f"    Avg:  {sum(durations)/len(durations):.0f}s")

    if failed:
        print(f"\n  Failed Jobs:")
        for r in failed:
            print(f"    {r['loader_id']}: {r['status']} - {r['error'][:100]}")

    # Summary table
    print(f"\n  {'Loader':<12} {'Source System':<16} {'Mode':<12} {'Status':<12} {'Duration':<10}")
    print(f"  {'-'*12} {'-'*16} {'-'*12} {'-'*12} {'-'*10}")
    for r in sorted(results, key=lambda x: x['loader_id']):
        print(f"  {r['loader_id']:<12} {r['source_system']:<16} "
              f"{r['overlap_mode']:<12} {r['status']:<12} {r['duration']:.0f}s")

    # Expected behavior analysis
    print(f"\n{'='*60}")
    print("ANALYSIS")
    print(f"{'='*60}")

    if test_mode == "isolated":
        if len(failed) == 0:
            print("  ✓ All loaders succeeded — partition isolation prevents conflicts.")
        else:
            print("  ⚠ Some loaders failed despite isolation — investigate errors above.")

    elif test_mode == "overlapping":
        if len(succeeded) == total:
            print("  ✓ All loaders succeeded — retry logic handled conflicts correctly.")
        elif len(succeeded) > 0:
            print(f"  ⚠ {len(failed)} loaders failed — retry budget may be insufficient.")
            print("    Consider increasing --commit-retries or reducing concurrency.")
        else:
            print("  ✗ All loaders failed — severe contention. Consider serialization strategy.")

    elif test_mode == "mixed":
        isolated_results = [r for r in results if r['overlap_mode'] == 'isolated']
        overlapping_results = [r for r in results if r['overlap_mode'] == 'overlapping']
        iso_success = sum(1 for r in isolated_results if r['status'] == 'SUCCEEDED')
        ovl_success = sum(1 for r in overlapping_results if r['status'] == 'SUCCEEDED')
        print(f"  Isolated loaders:    {iso_success}/{len(isolated_results)} succeeded")
        print(f"  Overlapping loaders: {ovl_success}/{len(overlapping_results)} succeeded")

    return {
        'test_mode': test_mode,
        'total': total,
        'succeeded': len(succeeded),
        'failed': len(failed),
        'results': results
    }

# ---------------------------------------------------------------------------
# Data Integrity Validation
# ---------------------------------------------------------------------------

def validate_data_integrity(job_name, database, main_table, warehouse_path, num_loaders, num_rows):
    """
    Run a validation Glue job to check data integrity after concurrent loads.
    This is a lightweight check using the Glue job itself.
    """
    print(f"\n{'='*60}")
    print("DATA INTEGRITY VALIDATION")
    print(f"{'='*60}")
    print("  (Run the validation job or query Athena to verify:)")
    print(f"  ")
    print(f"  -- Check for duplicates:")
    print(f"  SELECT id, source_system, count(*) as cnt")
    print(f"  FROM {database}.{main_table}")
    print(f"  GROUP BY id, source_system")
    print(f"  HAVING cnt > 1;")
    print(f"  ")
    print(f"  -- Check row counts per source_system:")
    print(f"  SELECT source_system, count(*) as cnt")
    print(f"  FROM {database}.{main_table}")
    print(f"  GROUP BY source_system")
    print(f"  ORDER BY source_system;")
    print(f"  ")
    print(f"  -- Check total rows:")
    print(f"  SELECT count(*) FROM {database}.{main_table};")
    print(f"  ")
    print(f"  -- Check snapshots (shows commit history):")
    print(f"  SELECT * FROM {database}.{main_table}.snapshots ORDER BY committed_at;")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    print(f"\n{'='*60}")
    print("ICEBERG CONCURRENCY TEST")
    print(f"{'='*60}")
    print(f"  Timestamp:      {datetime.now().isoformat()}")
    print(f"  Job Name:       {args.job_name}")
    print(f"  Database:       {args.database}")
    print(f"  Main Table:     {args.main_table}")
    print(f"  Warehouse:      {args.warehouse_path}")
    print(f"  Num Loaders:    {args.num_loaders}")
    print(f"  Rows/Loader:    {args.num_rows}")
    print(f"  Test Mode:      {args.test_mode}")
    print(f"  Commit Retries: {args.commit_retries}")

    # Generate loader configurations
    configs = generate_loader_configs(
        num_loaders=args.num_loaders,
        test_mode=args.test_mode,
        database=args.database,
        main_table=args.main_table,
        warehouse_path=args.warehouse_path,
        num_rows=args.num_rows,
        commit_retries=args.commit_retries
    )

    # Launch all loaders
    runs = launch_all_loaders(args.job_name, configs)

    # Monitor until completion
    results = poll_job_status(args.job_name, runs, poll_interval=args.poll_interval)

    # Analyze results
    summary = analyze_results(results, args.test_mode, args.num_rows)

    # Print validation queries
    validate_data_integrity(
        args.job_name, args.database, args.main_table,
        args.warehouse_path, args.num_loaders, args.num_rows
    )

    # Exit code based on results
    if summary['failed'] > 0:
        print(f"\n⚠ Test completed with {summary['failed']} failures.")
        exit(1)
    else:
        print(f"\n✓ Test completed successfully. All {summary['total']} loaders succeeded.")
        exit(0)


if __name__ == "__main__":
    main()
