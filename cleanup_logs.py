#!/usr/bin/env python3
"""
Cleanup Old MQTT Logs
Deletes log files older than a specified retention period instead of archiving them.
"""

import argparse
import configparser
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import re


class LogCleaner:
    def __init__(self, config_file="config.ini"):
        """Initialize log cleaner with configuration"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Set up logging
        self.log_dir = Path("mqtt_logs")
        self.log_dir.mkdir(exist_ok=True)
        self.setup_logging()

        # Load retention configuration
        # Default: 30 days if not specified
        self.retention_days = self.config.getint("cleanup", "retention_days", fallback=30)

        # Whether to also clean up archives directory
        self.cleanup_archives = self.config.getboolean("cleanup", "cleanup_archives", fallback=False)

    def setup_logging(self):
        """Set up logging"""
        log_file = self.log_dir / "cleanup_logs.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("log_cleaner")

    def parse_date_from_filename(self, filename: str) -> Optional[datetime]:
        """Parse date from filename like 'data_log_2025-01-15.jsonl' or 'data_log_<region>_2025-01-15.jsonl'

        Returns:
            datetime or None if parsing fails
        """
        # Pattern: data_log_YYYY-MM-DD.jsonl or data_log_<region>_YYYY-MM-DD.jsonl
        pattern = r'data_log_([a-z]+_)?(\d{4})-(\d{2})-(\d{2})\.jsonl'
        match = re.match(pattern, filename)
        if match:
            try:
                year = int(match.group(2))
                month = int(match.group(3))
                day = int(match.group(4))
                return datetime(year, month, day)
            except ValueError:
                return None
        return None

    def get_file_age_days(self, log_file: Path) -> int:
        """Calculate the age of a log file in days based on its filename

        For daily files, uses the date in the filename to determine age.
        For other files, uses file modification time.
        """
        # Try to parse date from filename
        file_date = self.parse_date_from_filename(log_file.name)

        if file_date:
            # Calculate age based on file date
            now = datetime.now()
            age = (now - file_date).days
            return age
        else:
            # Fall back to file modification time
            try:
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                now = datetime.now()
                age = (now - mtime).days
                return age
            except Exception as e:
                self.logger.warning(f"Could not determine age for {log_file.name}: {e}")
                return -1

    def get_old_log_files(self, retention_days: int) -> List[Path]:
        """Get all log files older than retention_days"""
        old_files = []

        # Find all daily log files (pattern: data_log_YYYY-MM-DD.jsonl or data_log_<region>_YYYY-MM-DD.jsonl)
        all_files = list(self.log_dir.glob("data_log_*-*-*.jsonl"))
        daily_files = []
        for f in all_files:
            # Filter to only match YYYY-MM-DD format (with optional region prefix, exclude symlinks and other files)
            if re.match(r'data_log_([a-z]+_)?\d{4}-\d{2}-\d{2}\.jsonl$', f.name):
                daily_files.append(f)

        # Also check for the current symlink (data_log.jsonl) - we'll skip it
        current_symlink = self.log_dir / "data_log.jsonl"

        for log_file in daily_files:
            # Skip the current symlink
            if log_file == current_symlink or log_file.is_symlink():
                continue

            age_days = self.get_file_age_days(log_file)
            if age_days > retention_days:
                old_files.append(log_file)
                self.logger.debug(f"File {log_file.name} is {age_days} days old (threshold: {retention_days})")

        return sorted(old_files)

    def cleanup_archives_dir(self, retention_days: int) -> List[Path]:
        """Get old files from archives directory based on modification time"""
        archives_dir = self.log_dir / "archives"
        if not archives_dir.exists():
            return []

        old_files = []
        now = datetime.now()

        for archive_file in archives_dir.iterdir():
            if archive_file.is_file():
                try:
                    mtime = datetime.fromtimestamp(archive_file.stat().st_mtime)
                    age = (now - mtime).days
                    if age > retention_days:
                        old_files.append(archive_file)
                except Exception as e:
                    self.logger.warning(f"Could not check age for {archive_file.name}: {e}")

        return sorted(old_files)

    def delete_files(self, files: List[Path], dry_run: bool = False) -> tuple:
        """Delete a list of files

        Returns:
            tuple: (deleted_count, failed_count, total_size_bytes)
        """
        deleted_count = 0
        failed_count = 0
        total_size = 0

        for file_path in files:
            try:
                # Get file size before deletion
                size = file_path.stat().st_size
                total_size += size

                if dry_run:
                    self.logger.info(f"[DRY RUN] Would delete: {file_path.name} ({size / 1024 / 1024:.2f} MB)")
                    deleted_count += 1
                else:
                    file_path.unlink()
                    self.logger.info(f"Deleted: {file_path.name} ({size / 1024 / 1024:.2f} MB)")
                    deleted_count += 1
            except Exception as e:
                self.logger.error(f"Failed to delete {file_path.name}: {e}")
                failed_count += 1

        return (deleted_count, failed_count, total_size)

    def cleanup(self, retention_days: int = None, dry_run: bool = False, cleanup_archives: bool = None) -> bool:
        """Main cleanup function

        Args:
            retention_days: Number of days to retain logs (overrides config)
            dry_run: If True, only report what would be deleted
            cleanup_archives: If True, also clean up archives directory (overrides config)
        """
        if retention_days is None:
            retention_days = self.retention_days

        if cleanup_archives is None:
            cleanup_archives = self.cleanup_archives

        self.logger.info(f"Starting cleanup with retention period: {retention_days} days")
        if dry_run:
            self.logger.info("DRY RUN MODE - No files will be deleted")

        # Find old log files
        old_log_files = self.get_old_log_files(retention_days)
        self.logger.info(f"Found {len(old_log_files)} old log files to delete")

        # Optionally find old archive files
        old_archive_files = []
        if cleanup_archives:
            old_archive_files = self.cleanup_archives_dir(retention_days)
            self.logger.info(f"Found {len(old_archive_files)} old archive files to delete")

        if not old_log_files and not old_archive_files:
            self.logger.info("No old files to delete")
            return True

        # Delete log files
        log_deleted, log_failed, log_size = self.delete_files(old_log_files, dry_run)

        # Delete archive files
        archive_deleted, archive_failed, archive_size = self.delete_files(old_archive_files, dry_run)

        # Summary
        total_deleted = log_deleted + archive_deleted
        total_failed = log_failed + archive_failed
        total_size = log_size + archive_size

        self.logger.info(f"Cleanup complete: {total_deleted} files deleted, {total_failed} failed")
        self.logger.info(f"Total space freed: {total_size / 1024 / 1024:.2f} MB")

        return total_failed == 0


def main():
    parser = argparse.ArgumentParser(description='Cleanup old MQTT logs')
    parser.add_argument('--retention-days', type=int, help='Number of days to retain logs (overrides config)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be deleted without actually deleting')
    parser.add_argument('--cleanup-archives', action='store_true', help='Also clean up archives directory')
    parser.add_argument('--no-cleanup-archives', action='store_true', help='Do not clean up archives directory (overrides config)')
    parser.add_argument('--config', default='config.ini', help='Config file path')

    args = parser.parse_args()

    cleaner = LogCleaner(args.config)

    # Determine cleanup_archives setting
    cleanup_archives = None
    if args.cleanup_archives:
        cleanup_archives = True
    elif args.no_cleanup_archives:
        cleanup_archives = False

    success = cleaner.cleanup(
        retention_days=args.retention_days,
        dry_run=args.dry_run,
        cleanup_archives=cleanup_archives
    )

    exit(0 if success else 1)


if __name__ == "__main__":
    main()
