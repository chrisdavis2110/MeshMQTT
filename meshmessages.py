#!/usr/bin/env python3
"""
Message Data Processor - Watch MQTT packet logs and write per-channel JSON files
"""

import json
import configparser
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.types.enums import PayloadType
from meshcoredecoder.crypto import MeshCoreKeyStore
from meshcoredecoder.types.crypto import DecryptionOptions


class MessageDataProcessor:
    def __init__(self, log_file=None):
        """Initialize the message data processor"""
        self.messages: List[Dict] = []  # legacy; not used for saving
        if log_file is None:
            log_file = "mqtt_logs/data_log.jsonl"
        self.log_file = Path(log_file)
        self.output_dir = Path("channels")
        self.processed_lines = 0

        # Message hash tracking to avoid duplicates
        self.seen_message_hashes = set()

        # Load config and keys if available
        self.channel_keys_by_hash: Dict[str, str] = {}
        self.channel_keys_by_name: Dict[str, Tuple[str, str]] = {}
        self.decryption_options: Optional[DecryptionOptions] = None
        self._load_config()

        # Aggregate messages by channel for output
        self.channel_messages: Dict[str, List[Dict]] = {}

    def _load_config(self):
        """Load configuration from config.ini and prepare decryption keys"""
        try:
            config = configparser.ConfigParser()
            config.read('config.ini')

            # Load channel secrets
            if config.has_section('channels'):
                self.channel_keys_by_hash, self.channel_keys_by_name = self._load_channel_keys(config)
                if self.channel_keys_by_hash:
                    # Initialize key store (library will compute hashes)
                    key_store = MeshCoreKeyStore({
                        'channel_secrets': list(self.channel_keys_by_hash.values())
                    })
                    self.decryption_options = DecryptionOptions(key_store=key_store)
        except Exception as e:
            print(f"Could not load config: {e}")

    def _compute_channel_hash(self, secret_key: str) -> str:
        """First byte of SHA256 of the channel secret, as uppercase hex"""
        try:
            if len(secret_key) == 32:
                key_bytes = bytes.fromhex(secret_key)
            else:
                key_bytes = secret_key.encode() if isinstance(secret_key, str) else secret_key
        except ValueError:
            key_bytes = secret_key.encode() if isinstance(secret_key, str) else secret_key
        h = hashlib.sha256(key_bytes).digest()[0]
        return f"{h:02X}"

    def _load_channel_keys(self, config: configparser.ConfigParser) -> Tuple[Dict[str, str], Dict[str, Tuple[str, str]]]:
        """Return (by_hash, by_name) channel key maps from config"""
        by_hash: Dict[str, str] = {}
        by_name: Dict[str, Tuple[str, str]] = {}
        for name in config.options('channels'):
            secret = config.get('channels', name).strip()
            chash = self._compute_channel_hash(secret)
            by_hash[chash] = secret
            by_name[name] = (chash, secret)
        return by_hash, by_name

    def process_log_file(self, only_new=False):
        """Read and process entries from the log file"""
        if not self.log_file.exists():
            print(f"Log file not found: {self.log_file}")
            return

        with open(self.log_file, 'r') as f:
            lines = f.readlines()

        # Only process new lines if only_new is True
        lines_to_process = lines[self.processed_lines:] if only_new else lines
        self.processed_lines = len(lines)

        for line in lines_to_process:
            try:
                entry = json.loads(line.strip())
                if entry.get('topic', '').endswith('/packets'):
                    self.process_packet(entry)
            except json.JSONDecodeError:
                continue

    def process_packet(self, entry):
        """Process a single packet entry"""
        try:
            data = entry.get('data', {})

            # Skip if not a PACKET type
            if data.get('type') != 'PACKET':
                return

            raw_hex = data.get('raw', '')
            if not raw_hex:
                return

            # Decode and check if it's a GroupText packet
            self.decode_and_store(raw_hex, entry, data)
        except Exception as e:
            print(f"Error processing packet: {e}")

    def decode_and_store(self, hex_string: str, entry: Dict, packet_data: Dict):
        """Decode a packet and store into per-channel buckets"""
        try:
            # Decode the packet (with decryption if keys available)
            packet = MeshCoreDecoder.decode(hex_string, self.decryption_options)

            # Only process valid GroupText packets
            if not packet.is_valid or packet.payload_type != PayloadType.GroupText:
                return

            payload = packet.payload
            if not payload or not payload.get('decoded'):
                return

            group_text = payload['decoded']

            # Skip if we've already seen this message hash
            message_hash = packet.message_hash
            if message_hash in self.seen_message_hashes:
                return
            self.seen_message_hashes.add(message_hash)

            # Build message entry (with metadata)
            channel_hash = group_text.channel_hash if hasattr(group_text, 'channel_hash') else None
            message_entry = {
                'message_hash': message_hash,
                'timestamp': entry.get('timestamp'),
                'received_time': datetime.now().isoformat() + 'Z',
                'origin': packet_data.get('origin', ''),
                'origin_id': packet_data.get('origin_id', ''),
                'route_type': packet.route_type.name if packet.route_type else None,
                'channel_hash': channel_hash,
                'SNR': packet_data.get('SNR'),
                'RSSI': packet_data.get('RSSI'),
                'score': packet_data.get('score'),
                'decrypted': False
            }

            # Determine channel bucket name
            bucket_name = None

            # If decrypted via provided keys
            if hasattr(group_text, 'decrypted') and group_text.decrypted:
                decrypted = group_text.decrypted
                message_entry['decrypted'] = True
                message_entry['sender'] = decrypted.get('sender', '')
                message_entry['message'] = decrypted.get('message', '')
                if decrypted.get('timestamp'):
                    message_entry['message_timestamp'] = datetime.fromtimestamp(
                        decrypted['timestamp']
                    ).isoformat() + 'Z'

                # Map channel hash to configured channel name if possible
                channel_name = None
                for name, (h, _) in self.channel_keys_by_name.items():
                    if channel_hash and h.upper() == channel_hash.upper():
                        channel_name = name
                        break
                bucket_name = channel_name or (f"encrypted_{channel_hash}" if channel_hash else "encrypted_unknown")
            else:
                # Not decrypted
                if hasattr(group_text, 'ciphertext'):
                    message_entry['ciphertext'] = group_text.ciphertext[:64] + '...' if len(group_text.ciphertext) > 64 else group_text.ciphertext
                bucket_name = f"encrypted_{channel_hash}" if channel_hash else "encrypted_unknown"

            # Append to bucket
            if bucket_name not in self.channel_messages:
                self.channel_messages[bucket_name] = []
            self.channel_messages[bucket_name].append(message_entry)

        except Exception as e:
            print(f"Error decoding GroupText packet: {e}")

    def save_messages_json(self, output_file=None):
        """Deprecated: no longer saving to messages.json"""
        return

    def save_channel_files(self):
        """Write one JSON file per channel bucket to output_dir"""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        for channel, msgs in self.channel_messages.items():
            if not msgs:
                continue
            sorted_messages = sorted(
                msgs,
                key=lambda x: x.get('timestamp', ''),
                reverse=True
            )
            output_data = {
                "timestamp": datetime.now().isoformat() + 'Z',
                "channel": channel,
                "channel_hash": sorted_messages[0].get('channel_hash') if sorted_messages else None,
                "count": len(sorted_messages),
                "decrypted_count": sum(1 for m in sorted_messages if m.get('decrypted', False)),
                "data": sorted_messages
            }
            filepath = self.output_dir / f"{channel}.json"
            with open(filepath, 'w') as f:
                json.dump(output_data, f, indent=2)
            print(f"Wrote {filepath.name}: {output_data['count']} messages ({output_data['decrypted_count']} decrypted)")

    def run(self, only_new=False):
        """Process log file and write per-channel JSON files"""
        print(f"Processing {self.log_file} for GroupText messages...")
        # Reset buckets if full run; keep accumulating if only_new
        if not only_new:
            self.channel_messages = {}
        self.process_log_file(only_new=only_new)
        self.save_channel_files()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Watch MQTT packet logs and write per-channel JSON files')
    parser.add_argument('--log', default='mqtt_logs/data_log.jsonl', help='Input log file')
    parser.add_argument('--output', default='channels', help='Output directory for per-channel JSON files')
    parser.add_argument('--watch', action='store_true', help='Watch log file for changes and continuously update channel files')

    args = parser.parse_args()

    processor = MessageDataProcessor(args.log)
    processor.output_dir = Path(args.output)

    if args.watch:
        # Watch mode - continuously monitor the log file
        print("Watching for new GroupText messages... (Ctrl+C to stop)")
        import time
        # Initial run
        processor.run(only_new=False)
        while True:
            try:
                current_size = processor.log_file.stat().st_size if processor.log_file.exists() else 0
                if current_size > 0:
                    # Process only new data
                    processor.run(only_new=True)
                time.sleep(5)  # Check every 5 seconds
            except KeyboardInterrupt:
                print("\nStopping watcher...")
                break
    else:
        # One-time processing
        processor.run(only_new=False)


if __name__ == "__main__":
    main()
