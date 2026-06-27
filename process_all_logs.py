#!/usr/bin/env python3
"""
Process all weekly log files to build a complete nodes.json
This script processes all weekly log files in chronological order
to ensure all nodes are captured, not just the current week.

Use --region to process only a specific region's logs (e.g. cv -> nodes_cv.json).
"""

import configparser
import re
from pathlib import Path
from meshnodes import NodeDataProcessor, get_region_log_names, get_region_log_file


def resolve_log_name(region_arg):
    """Resolve a region code or log name to the canonical log name from config.ini."""
    log_name = region_arg.lower()
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        if config.has_section('region_logs'):
            for code, name in config.items('region_logs'):
                if code.lower() == log_name or name.lower() == log_name:
                    return name.lower()
    except Exception:
        pass
    return log_name


def find_region_logs(log_path, log_name=None):
    """Find historical log files (daily or weekly format), optionally for one region."""
    if log_name:
        daily_pattern = re.compile(rf'^data_log_{re.escape(log_name)}_\d{{4}}-\d{{2}}-\d{{2}}\.jsonl$')
        weekly_pattern = re.compile(rf'^data_log_{re.escape(log_name)}_\d{{4}}-W\d+\.jsonl$')
        candidates = sorted(log_path.glob(f"data_log_{log_name}_*.jsonl"))
        log_files = [
            f for f in candidates
            if daily_pattern.match(f.name) or weekly_pattern.match(f.name)
        ]
        fallback = get_region_log_file(log_name)
    else:
        daily_pattern = re.compile(r'^data_log_\d{4}-\d{2}-\d{2}\.jsonl$')
        weekly_pattern = re.compile(r'^data_log_\d{4}-W\d+\.jsonl$')
        candidates = sorted(log_path.glob("data_log_*.jsonl"))
        log_files = [
            f for f in candidates
            if (daily_pattern.match(f.name) or weekly_pattern.match(f.name))
            and not f.is_symlink()
        ]
        fallback = log_path / "data_log.jsonl"

    if not log_files:
        if fallback.exists():
            print(f"No historical log files found in {log_path}")
            print(f"Falling back to processing {fallback.name} symlink")
            log_files = [fallback]
        else:
            print(f"No log files found in {log_path}")
            if log_name:
                print(
                    f"Expected: data_log_{log_name}_YYYY-MM-DD.jsonl, "
                    f"data_log_{log_name}_YYYY-W##.jsonl, or {fallback.name}"
                )

    return log_files


def process_all_weekly_logs(log_dir="mqtt_logs", output_file="nodes.json", region=None):
    """Process all log files in chronological order"""
    log_path = Path(log_dir)

    if not log_path.exists():
        print(f"Log directory not found: {log_dir}")
        return

    log_name = resolve_log_name(region) if region else None
    log_files = find_region_logs(log_path, log_name)

    if not log_files:
        return

    region_label = f" ({log_name})" if log_name else ""
    print(f"Found {len(log_files)} log files to process{region_label}:")
    for log_file in log_files:
        print(f"  - {log_file.name}")

    # Initialize processor - we'll reuse it for all files
    # Start with first log file, but we'll process all files
    processor = NodeDataProcessor(str(log_files[0]))
    processor.output_file = output_file
    # Load existing nodes if output file exists (to preserve first_seen, etc.)
    processor._load_existing_nodes()
    # Fetch API data once at the start
    processor.fetch_api_data()

    # Process each log file sequentially
    for i, log_file in enumerate(log_files):
        print(f"\n{'='*60}")
        print(f"Processing file {i+1}/{len(log_files)}: {log_file.name}")
        print(f"{'='*60}")

        # Update log file path and reset line counter
        processor.log_file = Path(log_file)
        processor.processed_lines = 0  # Reset line counter for new file

        # Process this log file (only_new=False to process all lines)
        processor.process_log_file(only_new=False)

    # Save final nodes.json
    if processor:
        processor.save_nodes_json()
        print(f"\n✓ Successfully processed all {len(log_files)} log files")
        print(f"✓ Final {output_file} contains {len(processor.nodes)} nodes")
    else:
        print("Error: No log files were processed")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Process all weekly log files to build complete nodes.json'
    )
    parser.add_argument(
        '--log-dir',
        default='mqtt_logs',
        help='Directory containing weekly log files'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='Output JSON file (default: nodes.json, or nodes_<region>.json with --region)'
    )
    parser.add_argument(
        '--region',
        default=None,
        help='Process only this region (log name or config region code, e.g. cv or LAX)'
    )
    parser.add_argument(
        '--list-regions',
        action='store_true',
        help='List configured region log names and exit'
    )

    args = parser.parse_args()

    if args.list_regions:
        regions = get_region_log_names()
        if regions:
            print("Configured region logs:")
            for name in regions:
                print(f"  {name} -> nodes_{name}.json")
        else:
            print("No region logs configured in config.ini [region_logs]")
        return

    if args.region:
        log_name = resolve_log_name(args.region)
        output_file = args.output or f"nodes_{log_name}.json"
        process_all_weekly_logs(args.log_dir, output_file, region=args.region)
        return

    region_log_names = get_region_log_names()
    if region_log_names:
        for log_name in region_log_names:
            output_file = args.output or f"nodes_{log_name}.json"
            print(f"\n{'#'*60}")
            print(f"# Region: {log_name}")
            print(f"{'#'*60}")
            process_all_weekly_logs(args.log_dir, output_file, region=log_name)
        return

    output_file = args.output or "nodes.json"
    process_all_weekly_logs(args.log_dir, output_file)


if __name__ == "__main__":
    main()
