#!/usr/bin/env python3
"""
Process all weekly log files to build a complete nodes.json
This script processes all weekly log files in chronological order
to ensure all nodes are captured, not just the current week.
"""

import sys
from pathlib import Path
from meshnodes import NodeDataProcessor


def process_all_weekly_logs(log_dir="mqtt_logs", output_file="nodes.json"):
    """Process all weekly log files in chronological order"""
    log_path = Path(log_dir)

    if not log_path.exists():
        print(f"Log directory not found: {log_dir}")
        return

    # Find all weekly log files (data_log_YYYY-W##.jsonl)
    weekly_logs = sorted(log_path.glob("data_log_*-W*.jsonl"))

    if not weekly_logs:
        print(f"No weekly log files found in {log_dir}")
        print("Falling back to processing data_log.jsonl symlink")
        weekly_logs = [log_path / "data_log.jsonl"]

    print(f"Found {len(weekly_logs)} weekly log files to process:")
    for log_file in weekly_logs:
        print(f"  - {log_file.name}")

    # Initialize processor - we'll reuse it for all files
    # Start with first log file, but we'll process all files
    processor = NodeDataProcessor(str(weekly_logs[0]))
    processor.output_file = output_file
    # Load existing nodes if output file exists (to preserve first_seen, etc.)
    processor._load_existing_nodes()
    # Fetch API data once at the start
    processor.fetch_api_data()

    # Process each log file sequentially
    for i, log_file in enumerate(weekly_logs):
        print(f"\n{'='*60}")
        print(f"Processing file {i+1}/{len(weekly_logs)}: {log_file.name}")
        print(f"{'='*60}")

        # Update log file path and reset line counter
        processor.log_file = Path(log_file)
        processor.processed_lines = 0  # Reset line counter for new file

        # Process this log file (only_new=False to process all lines)
        processor.process_log_file(only_new=False)

    # Save final nodes.json
    if processor:
        processor.save_nodes_json()
        print(f"\n✓ Successfully processed all {len(weekly_logs)} log files")
        print(f"✓ Final nodes.json contains {len(processor.nodes)} nodes")
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
        default='nodes.json',
        help='Output JSON file'
    )

    args = parser.parse_args()

    process_all_weekly_logs(args.log_dir, args.output)


if __name__ == "__main__":
    main()
