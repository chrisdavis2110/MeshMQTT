#!/usr/bin/env python3
"""
Archive and Transfer Mesh metadata
Creates timestamped archives containing nodes.json and channels/*.json, then
transfers them using the same methods as log archiver (ssh/rsync/smb).
"""

import argparse
import configparser
import gzip
import logging
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional


class MetadataArchiver:
    def __init__(self, config_file: str = "config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Logging dir
        self.log_dir = Path("mqtt_logs")
        self.log_dir.mkdir(exist_ok=True)
        self._setup_logging()

        # Transfer config (reuse [archive] settings)
        self.transfer_method = self.config.get("archive", "method", fallback="ssh").lower()
        self.remote_host = self.config.get("archive", "host", fallback="")
        self.remote_path = self.config.get("archive", "path", fallback="/")
        # Optional separate paths
        self.remote_path_nodes = self.config.get("archive", "nodes_path", fallback=self.remote_path)
        self.remote_path_messages = self.config.get("archive", "channels_path", fallback=self.remote_path)
        self.remote_user = self.config.get("archive", "user", fallback="")
        self.ssh_key = self.config.get("archive", "ssh_key", fallback="")
        self.smb_user = self.config.get("archive", "smb_user", fallback="")
        self.smb_password = self.config.get("archive", "smb_password", fallback="")
        self.smb_share = self.config.get("archive", "smb_share", fallback="")
        # Behavior toggles
        self.delete_original_channels_default = False
        try:
            if self.config.has_option('archive', 'delete_original_channels'):
                self.delete_original_channels_default = self.config.getboolean('archive', 'delete_original_channels')
        except Exception:
            self.delete_original_channels_default = False

        # Inputs
        self.nodes_file = Path("nodes.json")
        self.channels_dir = Path("channels")

        # Output dir
        self.archive_dir = self.log_dir / "archives"
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def _setup_logging(self):
        log_file = self.log_dir / "archive_metadata.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("metadata_archiver")

    def collect_nodes_file(self) -> Optional[Path]:
        if self.nodes_file.exists():
            return self.nodes_file
        self.logger.warning("nodes.json not found")
        return None

    def collect_channel_files(self) -> List[Path]:
        files: List[Path] = []
        if self.channels_dir.exists() and self.channels_dir.is_dir():
            files = sorted(self.channels_dir.glob("*.json"))
        self.logger.info(f"Collected {len(files)} channel files for archiving")
        return files

    def snapshot_nodes(self, tag: str) -> Optional[Path]:
        src = self.collect_nodes_file()
        if not src:
            return None
        dst = self.archive_dir / f"nodes_{tag}.json"
        try:
            dst.write_text(src.read_text())
            self.logger.info(f"Created nodes snapshot: {dst.name}")
            return dst
        except Exception as e:
            self.logger.error(f"Failed to create nodes snapshot: {e}")
            return None

    def snapshot_messages(self, tag: str) -> List[tuple]:
        out: List[tuple] = []  # (snapshot_path, original_path)
        for p in self.collect_channel_files():
            dst = self.archive_dir / f"messages_{tag}_{p.name}"
            try:
                dst.write_text(p.read_text())
                out.append((dst, p))
            except Exception as e:
                self.logger.error(f"Failed to snapshot {p.name}: {e}")
        self.logger.info(f"Created {len(out)} message snapshots")
        return out

    def _remote_spec(self, remote_path: Optional[str]) -> str:
        target = remote_path or self.remote_path
        return f"{self.remote_user}@{self.remote_host}:{target}" if self.remote_user else f"{self.remote_host}:{target}"

    def transfer_via_ssh(self, local_file: Path, remote_path: Optional[str] = None) -> bool:
        if not self.remote_host:
            self.logger.error("SSH host not configured")
            return False
        remote = self._remote_spec(remote_path)
        cmd = ["scp"]
        if self.ssh_key:
            cmd.extend(["-i", self.ssh_key])
        cmd.extend(["-o", "StrictHostKeyChecking=no", str(local_file), remote])
        self.logger.info(f"Transferring via SSH: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info(f"Transferred {local_file.name} to {remote}")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"SSH transfer failed: {e.stderr}")
            return False

    def transfer_via_rsync(self, local_file: Path, remote_path: Optional[str] = None) -> bool:
        if not self.remote_host:
            self.logger.error("SSH host not configured")
            return False
        remote = self._remote_spec(remote_path)
        cmd = ["rsync", "-avz", "--progress"]
        if self.ssh_key:
            cmd.extend(["-e", f"ssh -i {self.ssh_key} -o StrictHostKeyChecking=no"])
        cmd.extend([str(local_file), remote])
        self.logger.info(f"Transferring via rsync: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            self.logger.info(f"Transferred {local_file.name} via rsync")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Rsync transfer failed: {e.stderr}")
            return False

    def transfer_via_smb(self, local_file: Path, remote_path: Optional[str] = None) -> bool:
        if not all([self.remote_host, self.smb_share, self.smb_user]):
            self.logger.error("SMB configuration incomplete")
            return False

        target_path = (remote_path or self.remote_path) or "."
        smb_url = f"//{self.remote_host}/{self.smb_share}"

        # Use auth file for password, if provided
        try:
            if self.smb_password:
                env = os.environ.copy()
                env['SMBPASS'] = self.smb_password
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as pf:
                    pf.write(f"username={self.smb_user}\n")
                    pf.write(f"password={self.smb_password}\n")
                    passfile = pf.name
                cmd = [
                    "smbclient", smb_url, "-A", passfile,
                    "-c", f"cd {target_path}; put {local_file} {local_file.name}; quit"
                ]
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
                stdout, stderr = proc.communicate()
                try:
                    os.unlink(passfile)
                except Exception:
                    pass
            else:
                cmd = [
                    "smbclient", smb_url, "-U", self.smb_user, "-N",
                    "-c", f"cd {target_path}; put {local_file} {local_file.name}; quit"
                ]
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                stdout, stderr = proc.communicate()

            if proc.returncode == 0:
                self.logger.info(f"Transferred {local_file.name} to SMB share")
                return True
            else:
                self.logger.error(f"SMB transfer failed: {stderr}")
                return False
        except Exception as e:
            self.logger.error(f"SMB transfer error: {e}")
            return False

    def archive_and_transfer(self, tag: Optional[str] = None, compress: bool = True, keep_local: bool = True, keep_nodes: bool = False, delete_original_channels: bool = False) -> bool:
        # We no longer compress; snapshots are plain JSON copies.
        if not tag:
            tag = datetime.now().strftime("%Y-%m-%d")

        # Create snapshots in archives/ so originals remain untouched
        nodes_snap = self.snapshot_nodes(tag)
        msg_snaps = self.snapshot_messages(tag)

        if not nodes_snap and not msg_snaps:
            self.logger.warning("Nothing to transfer")
            return False

        self.logger.info(
            f"Options: keep_local={keep_local}, keep_nodes={keep_nodes}, delete_original_channels={delete_original_channels}"
        )

        # Transfer nodes snapshot
        all_ok = True
        if nodes_snap:
            ok = False
            if self.transfer_method == "ssh":
                ok = self.transfer_via_ssh(nodes_snap, self.remote_path_nodes)
            elif self.transfer_method == "rsync":
                ok = self.transfer_via_rsync(nodes_snap, self.remote_path_nodes)
            elif self.transfer_method == "smb":
                ok = self.transfer_via_smb(nodes_snap, self.remote_path_nodes)
            else:
                self.logger.error(f"Unknown transfer method: {self.transfer_method}")
                return False
            if ok and not keep_local and not keep_nodes:
                try:
                    nodes_snap.unlink()
                except Exception as e:
                    self.logger.error(f"Failed to remove nodes snapshot: {e}")
            all_ok = all_ok and ok

        # Transfer message snapshots (per-channel) to messages path
        for snap, orig in msg_snaps:
            ok = False
            if self.transfer_method == "ssh":
                ok = self.transfer_via_ssh(snap, self.remote_path_messages)
            elif self.transfer_method == "rsync":
                ok = self.transfer_via_rsync(snap, self.remote_path_messages)
            elif self.transfer_method == "smb":
                ok = self.transfer_via_smb(snap, self.remote_path_messages)
            else:
                self.logger.error(f"Unknown transfer method: {self.transfer_method}")
                return False
            if ok and not keep_local:
                try:
                    snap.unlink()
                except Exception as e:
                    self.logger.error(f"Failed to remove message snapshot {snap.name}: {e}")
            if ok and delete_original_channels:
                try:
                    orig.unlink()
                    self.logger.info(f"Deleted original channel file: {orig.name}")
                except Exception as e:
                    self.logger.error(f"Failed to delete original channel file {orig.name}: {e}")
            all_ok = all_ok and ok

        return all_ok


def main():
    parser = argparse.ArgumentParser(description='Archive and transfer nodes.json and channels/*.json')
    parser.add_argument('--tag', help='Tag for archive (default: YYYY-MM-DD)')
    parser.add_argument('--no-compress', action='store_true', help='Do not gzip the archive')
    parser.add_argument('--no-keep', action='store_true', help='Delete local archive after successful transfer')
    parser.add_argument('--config', default='config.ini', help='Config file path')
    parser.add_argument('--keep-nodes', action='store_true', help='Keep nodes snapshot locally even with --no-keep')
    parser.add_argument('--delete-original-channels', action='store_true', help='Delete original channels/*.json after successful transfer')
    args = parser.parse_args()

    archiver = MetadataArchiver(args.config)

    # Effective delete flag: CLI overrides config default
    delete_original = args.delete_original_channels or archiver.delete_original_channels_default

    success = archiver.archive_and_transfer(
        tag=args.tag,
        compress=not args.no_compress,
        keep_local=not args.no_keep,
        keep_nodes=args.keep_nodes,
        delete_original_channels=delete_original
    )
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
