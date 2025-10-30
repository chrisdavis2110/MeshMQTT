#!/usr/bin/env python3
"""
Archive and Transfer MQTT Logs
Compiles yearly log files and transfers them to a remote server via SMB or SSH
"""

import json
import subprocess
import configparser
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
import gzip
import tempfile
import os

class LogArchiver:
    def __init__(self, config_file="config.ini"):
        """Initialize log archiver with configuration"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Set up logging
        self.log_dir = Path("mqtt_logs")
        self.log_dir.mkdir(exist_ok=True)
        self.setup_logging()

        # Load transfer configuration
        self.transfer_method = self.config.get("archive", "method", fallback="ssh").lower()
        self.remote_host = self.config.get("archive", "host", fallback="")
        self.remote_path = self.config.get("archive", "path", fallback="/")
        self.remote_user = self.config.get("archive", "user", fallback="")
        self.ssh_key = self.config.get("archive", "ssh_key", fallback="")
        self.smb_user = self.config.get("archive", "smb_user", fallback="")
        self.smb_password = self.config.get("archive", "smb_password", fallback="")
        self.smb_share = self.config.get("archive", "smb_share", fallback="")

    def setup_logging(self):
        """Set up logging"""
        log_file = self.log_dir / "archive_logs.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("log_archiver")

    def get_yearly_log_files(self, year: Optional[int] = None) -> List[Path]:
        """Get all weekly log files for a given year"""
        if year is None:
            year = datetime.now().year

        # Pattern: data_log_YYYY-W##.jsonl
        log_files = sorted(
            self.log_dir.glob(f"data_log_{year}-W*.jsonl")
        )

        self.logger.info(f"Found {len(log_files)} log files for year {year}")
        return log_files

    def get_all_weekly_files(self) -> List[Path]:
        """Get all weekly log files regardless of year (for monthly filtering)."""
        return sorted(self.log_dir.glob("data_log_*W*.jsonl"))

    def file_has_entries_in_month(self, log_file: Path, year: int, month: int) -> bool:
        """Return True if the given weekly file has any entries whose timestamp is in (year, month)."""
        try:
            with open(log_file, 'r') as infile:
                for line in infile:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        ts_str = entry.get('timestamp')
                        if not ts_str:
                            continue
                        ts = datetime.fromisoformat(ts_str.replace('Z', ''))
                        if ts.year == year and ts.month == month:
                            return True
                    except Exception:
                        continue
        except FileNotFoundError:
            return False
        return False

    def find_weekly_files_for_month(self, year: int, month: int) -> List[Path]:
        """Find weekly files that contain at least one entry for the specified (year, month)."""
        candidates = self.get_all_weekly_files()
        result: List[Path] = []
        for f in candidates:
            if self.file_has_entries_in_month(f, year, month):
                result.append(f)
        self.logger.info(f"Selected {len(result)} weekly files for {year}-{month:02d}")
        return result

    def compile_logs(self, log_files: List[Path], output_file: Path, compress: bool = False) -> Path:
        """Compile multiple log files into a single file"""
        if compress:
            output_file = output_file.with_suffix(output_file.suffix + '.gz')
            open_func = gzip.open
            mode = 'wt'
        else:
            open_func = open
            mode = 'w'

        total_entries = 0
        with open_func(output_file, mode) as outfile:
            for log_file in log_files:
                if not log_file.exists():
                    self.logger.warning(f"Log file not found: {log_file}")
                    continue

                try:
                    with open(log_file, 'r') as infile:
                        for line in infile:
                            if line.strip():  # Skip empty lines
                                outfile.write(line)
                                total_entries += 1
                    self.logger.info(f"Processed {log_file.name}")
                except Exception as e:
                    self.logger.error(f"Error processing {log_file}: {e}")

        self.logger.info(f"Compiled {total_entries} entries to {output_file.name}")
        return output_file

    def compile_month(self, year: int, month: int, output_file: Path, compress: bool = True) -> Path:
        """Compile all entries from a specific year-month into one archive by filtering per-entry timestamps."""
        if compress:
            output_file = output_file.with_suffix(output_file.suffix + '.gz')
            open_func = gzip.open
            mode = 'wt'
        else:
            open_func = open
            mode = 'w'

        # Consider all weekly files (weeks can cross month boundaries)
        weekly_files = self.get_all_weekly_files()

        total_entries = 0
        with open_func(output_file, mode) as outfile:
            for log_file in weekly_files:
                try:
                    with open(log_file, 'r') as infile:
                        for line in infile:
                            if not line.strip():
                                continue
                            try:
                                entry = json.loads(line)
                                ts_str = entry.get('timestamp')
                                if not ts_str:
                                    continue
                                # Support ISO format with or without 'Z'
                                try:
                                    ts = datetime.fromisoformat(ts_str.replace('Z', ''))
                                except Exception:
                                    continue
                                if ts.year == year and ts.month == month:
                                    outfile.write(line)
                                    total_entries += 1
                            except json.JSONDecodeError:
                                continue
                except FileNotFoundError:
                    continue

        self.logger.info(f"Compiled {total_entries} entries for {year}-{month:02d} to {output_file.name}")
        return output_file

    def transfer_via_ssh(self, local_file: Path, remote_path: Optional[str] = None) -> bool:
        """Transfer file via SSH/SCP"""
        if not self.remote_host:
            self.logger.error("SSH host not configured")
            return False

        if remote_path is None:
            remote_path = self.remote_path

        # Build scp command
        remote = f"{self.remote_user}@{self.remote_host}:{remote_path}" if self.remote_user else f"{self.remote_host}:{remote_path}"

        cmd = ["scp"]
        if self.ssh_key:
            cmd.extend(["-i", self.ssh_key])
        cmd.extend(["-o", "StrictHostKeyChecking=no"])
        cmd.extend([str(local_file), remote])

        self.logger.info(f"Transferring via SSH: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info(f"Successfully transferred {local_file.name} to {remote}")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"SSH transfer failed: {e.stderr}")
            return False

    def transfer_via_smb(self, local_file: Path, remote_path: Optional[str] = None) -> bool:
        """Transfer file via SMB"""
        if not all([self.remote_host, self.smb_share, self.smb_user]):
            self.logger.error("SMB configuration incomplete")
            return False

        if remote_path is None:
            remote_path = self.remote_path

        # Use smbclient to transfer
        # Format: //host/share
        smb_url = f"//{self.remote_host}/{self.smb_share}"

        # Build remote file path
        if remote_path and remote_path != "/":
            remote_path = remote_path.lstrip("/").rstrip("/")
            remote_file = f"{remote_path}/{local_file.name}"
        else:
            remote_file = local_file.name

        try:
            # Build smbclient command with commands
            cmd = [
                "smbclient",
                smb_url,
                "-U", self.smb_user,
                "-N" if not self.smb_password else "",  # -N for no password prompt if no password
                "-c", f"cd {remote_path or '.'}; put {local_file} {remote_file}; quit"
            ]
            # Remove empty strings
            cmd = [c for c in cmd if c]

            if self.smb_password:
                # Use environment variable for password
                env = os.environ.copy()
                env['SMBPASS'] = self.smb_password

                # Alternative: use password file (more secure)
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as pass_file:
                    pass_file.write(f"username={self.smb_user}\n")
                    pass_file.write(f"password={self.smb_password}\n")
                    pass_file_path = pass_file.name

                cmd = [
                    "smbclient",
                    smb_url,
                    "-A", pass_file_path,  # Use authentication file
                    "-c", f"cd {remote_path or '.'}; put {local_file} {remote_file}; quit"
                ]

                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env
                )
                stdout, stderr = process.communicate()

                # Clean up password file
                try:
                    os.unlink(pass_file_path)
                except:
                    pass
            else:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                stdout, stderr = process.communicate()

            if process.returncode == 0:
                self.logger.info(f"Successfully transferred {local_file.name} to SMB share")
                return True
            else:
                self.logger.error(f"SMB transfer failed: {stderr}")
                return False

        except Exception as e:
            self.logger.error(f"SMB transfer error: {e}")
            return False

    def transfer_via_rsync(self, local_file: Path, remote_path: Optional[str] = None) -> bool:
        """Transfer file via rsync over SSH (more efficient for large files)"""
        if not self.remote_host:
            self.logger.error("SSH host not configured")
            return False

        if remote_path is None:
            remote_path = self.remote_path

        # Build rsync command
        remote = f"{self.remote_user}@{self.remote_host}:{remote_path}" if self.remote_user else f"{self.remote_host}:{remote_path}"

        cmd = ["rsync", "-avz", "--progress"]
        if self.ssh_key:
            cmd.extend(["-e", f"ssh -i {self.ssh_key} -o StrictHostKeyChecking=no"])
        cmd.extend([str(local_file), remote])

        self.logger.info(f"Transferring via rsync: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info(f"Successfully transferred {local_file.name} via rsync")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Rsync transfer failed: {e.stderr}")
            return False

    def archive_and_transfer(self, year: Optional[int] = None, month: Optional[str] = None, compress: bool = True,
                           keep_local: bool = True, send_each: bool = False) -> bool:
        """Archive logs and transfer to remote server.

        Behavior:
        - If month is provided (1-12 or 'prev'), archive that specific month (default previous month when None).
        - If only year is provided, archive the entire year.
        - If neither is provided, default to previous month.
        """
        now = datetime.now()

        # Determine target year/month
        target_year = year
        target_month: Optional[int] = None

        if month is not None or year is None:
            # Monthly mode
            if month is None or (isinstance(month, str) and month.lower() == 'prev'):
                # Previous month from now
                first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                last_month_last_day = first_of_this_month - timedelta(days=1)
                target_year = last_month_last_day.year
                target_month = last_month_last_day.month
            else:
                try:
                    m = int(month)
                    if m < 1 or m > 12:
                        raise ValueError
                    target_month = m
                except Exception:
                    self.logger.error(f"Invalid --month value: {month}")
                    return False
                if target_year is None:
                    target_year = now.year

        archive_dir = self.log_dir / "archives"
        archive_dir.mkdir(exist_ok=True)

        if target_month is not None:
            # Monthly mode
            self.logger.info(f"Starting archive process for {target_year}-{target_month:02d}")
            if send_each:
                # Send each weekly file that has entries in this month (no compression)
                weekly_files = self.find_weekly_files_for_month(target_year, target_month)
                if not weekly_files:
                    self.logger.warning("No weekly files found for this month")
                    return False
                all_ok = True
                for wf in weekly_files:
                    ok = False
                    if self.transfer_method == "ssh":
                        ok = self.transfer_via_ssh(wf)
                    elif self.transfer_method == "rsync":
                        ok = self.transfer_via_rsync(wf)
                    elif self.transfer_method == "smb":
                        ok = self.transfer_via_smb(wf)
                    else:
                        self.logger.error(f"Unknown transfer method: {self.transfer_method}")
                        return False
                    if not ok:
                        all_ok = False
                    else:
                        # Delete local weekly file after successful transfer if not keeping
                        if not keep_local:
                            try:
                                wf.unlink()
                                self.logger.info(f"Removed local weekly log after transfer: {wf.name}")
                            except Exception as e:
                                self.logger.error(f"Failed to remove local weekly log {wf.name}: {e}")
                return all_ok
            else:
                # Compile into a single monthly archive
                archive_file = archive_dir / f"data_log_{target_year}-{target_month:02d}_compiled.jsonl"
                compiled_file = self.compile_month(target_year, target_month, archive_file, compress=compress)
        else:
            # Yearly archive
            if target_year is None:
                target_year = now.year
            self.logger.info(f"Starting archive process for year {target_year}")
            log_files = self.get_yearly_log_files(target_year)
            if not log_files:
                self.logger.warning(f"No log files found for year {target_year}")
                return False
            archive_file = archive_dir / f"data_log_{target_year}_compiled.jsonl"
            compiled_file = self.compile_logs(log_files, archive_file, compress=compress)

        # Transfer to remote server
        transfer_success = False
        if self.transfer_method == "ssh":
            transfer_success = self.transfer_via_ssh(compiled_file)
        elif self.transfer_method == "rsync":
            transfer_success = self.transfer_via_rsync(compiled_file)
        elif self.transfer_method == "smb":
            transfer_success = self.transfer_via_smb(compiled_file)
        else:
            self.logger.error(f"Unknown transfer method: {self.transfer_method}")
            return False

        # Clean up local archive if transfer successful and not keeping
        if transfer_success and not keep_local:
            compiled_file.unlink()
            self.logger.info(f"Removed local archive after successful transfer")

        return transfer_success


def main():
    parser = argparse.ArgumentParser(description='Archive and transfer MQTT logs')
    parser.add_argument('--year', type=int, help='Year to archive (default: current year or derived from --month)')
    parser.add_argument('--month', help='Month to archive: 1-12 or prev (previous month). If set, archives that month.')
    parser.add_argument('--no-compress', action='store_true', help='Do not compress archive')
    parser.add_argument('--no-keep', action='store_true', help='Delete local archive after transfer')
    parser.add_argument('--send-each', action='store_true', help='Send each weekly file individually (no compilation)')
    parser.add_argument('--config', default='config.ini', help='Config file path')

    args = parser.parse_args()

    archiver = LogArchiver(args.config)
    success = archiver.archive_and_transfer(
        year=args.year,
        month=args.month,
        compress=not args.no_compress,
        keep_local=not args.no_keep,
        send_each=args.send_each
    )

    exit(0 if success else 1)


if __name__ == "__main__":
    main()
