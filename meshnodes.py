#!/usr/bin/env python3
"""
Node Data Processor - Decode MQTT packet data and create nodes.json
"""

import json
import requests
import configparser
import asyncio
from datetime import datetime
from pathlib import Path

from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.types.enums import PayloadType, DeviceRole


class NodeDataProcessor:
    def __init__(self, log_file=None, api_url=None, output_file=None):
        """Initialize the node data processor"""
        self.nodes = {}
        if log_file is None:
            log_file = "mqtt_logs/data_log.jsonl"
        self.log_file = Path(log_file)
        self.api_url = api_url
        self.api_nodes = {}
        self.output_file = output_file if output_file else "nodes.json"
        self.processed_lines = 0

        # Statistics for tracking
        self.stats = {
            'total_entries': 0,
            'packet_topic_entries': 0,
            'advert_packet_type': 0,
            'with_raw_hex': 0,
            'successfully_decoded': 0,
            'invalid_or_wrong_type': 0,
            'no_decoded_payload': 0,
            'skipped_older_timestamp': 0,
            'decode_errors': 0,
            'signature_verification_failed': 0
        }

        # Load config if available
        self._load_config()

        # Load existing nodes (to preserve first_seen across runs)
        # Only load from the specific output file for this region
        self._load_existing_nodes()

    def _load_config(self):
        """Load configuration from config.ini"""
        self.config = None
        try:
            config = configparser.ConfigParser()
            config.read('config.ini')
            if not self.api_url and config.has_option('meshcore', 'mqtt_api'):
                self.api_url = config.get('meshcore', 'mqtt_api')
            self.config = config
        except Exception as e:
            print(f"Could not load config: {e}")

    def fetch_api_data(self):
        """Fetch node data from the API"""
        if not self.api_url:
            return

        try:
            print("Fetching node data from API...")
            response = requests.get(self.api_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'data' in data:
                    for node in data['data']:
                        public_key = node.get('public_key', '')
                        if public_key:
                            self.api_nodes[public_key] = node
                print(f"Fetched {len(self.api_nodes)} nodes from API")
        except Exception as e:
            print(f"Could not fetch API data: {e}")

    def process_log_file(self, only_new=False):
        """Read and process entries from the log file"""
        if not self.log_file.exists():
            print(f"Log file not found: {self.log_file}")
            return

        with open(self.log_file, 'r') as f:
            lines = f.readlines()

        current_file_size = len(lines)

        # If file has shrunk (weekly rotation), reset the counter
        # This happens when a new week's log file is smaller than the previous week's
        if only_new and current_file_size < self.processed_lines:
            print(f"Detected log file rotation (file size decreased from {self.processed_lines} to {current_file_size} lines). Resetting line counter.")
            self.processed_lines = 0

        # Only process new lines if only_new is True
        lines_to_process = lines[self.processed_lines:] if only_new else lines
        self.processed_lines = len(lines)

        for line in lines_to_process:
            try:
                entry = json.loads(line.strip())
                self.stats['total_entries'] += 1
                if entry.get('topic', '').endswith('/packets'):
                    self.stats['packet_topic_entries'] += 1
                    self.process_packet(entry)
            except json.JSONDecodeError:
                continue

    def process_packet(self, entry):
        """Process a single packet entry"""
        try:
            data = entry.get('data', {})

            # Only process advertisement packets (type 4)
            if data.get('packet_type') == '4':
                self.stats['advert_packet_type'] += 1
                raw_hex = data.get('raw', '')
                if raw_hex:
                    self.stats['with_raw_hex'] += 1
                    self.decode_and_store(raw_hex, entry.get('timestamp'))
        except Exception as e:
            print(f"Error processing packet: {e}")
            self.stats['decode_errors'] += 1

    async def _verify_packet(self, hex_string):
        """Verify packet signature using async decode_with_verification"""
        try:
            packet = await MeshCoreDecoder.decode_with_verification(hex_string)
            if packet.payload.get('decoded'):
                advert = packet.payload['decoded']
                if hasattr(advert, 'signature_valid'):
                    return advert.signature_valid
            return False
        except Exception as e:
            print(f"Error verifying packet signature: {e}")
            return False

    def _run_verify_packet(self, hex_string):
        """Run packet verification synchronously"""
        try:
            # Since this is called from synchronous code, asyncio.run() should work
            return asyncio.run(self._verify_packet(hex_string))
        except RuntimeError as e:
            # If there's already an event loop running, we need a different approach
            # This shouldn't happen in normal usage, but handle it gracefully
            print(f"Warning: Could not verify packet signature due to event loop issue: {e}")
            return False

    def decode_and_store(self, hex_string, log_timestamp):
        """Decode a packet and store node information"""
        try:
            # First verify the packet signature
            signature_valid = self._run_verify_packet(hex_string)
            if not signature_valid:
                print(f"Warning: Packet signature verification failed, skipping node")
                self.stats['signature_verification_failed'] += 1
                return

            packet = MeshCoreDecoder.decode(hex_string)

            # Only process valid advertisement packets
            if not packet.is_valid or packet.payload_type != PayloadType.Advert:
                self.stats['invalid_or_wrong_type'] += 1
                return

            decoded = packet.payload.get('decoded')
            if not decoded:
                self.stats['no_decoded_payload'] += 1
                return

            # Extract node information
            public_key = decoded.public_key
            app_data = decoded.app_data if hasattr(decoded, 'app_data') and decoded.app_data else {}

            # Validate public_key
            if not public_key:
                self.stats['decode_errors'] += 1
                print("Warning: Packet decoded but has no public_key, skipping")
                return

            # Determine the timestamp to use
            # Prefer log entry timestamp (when we received it) over device timestamp
            # Convert log_timestamp string to timestamp if needed
            if isinstance(log_timestamp, str):
                try:
                    # Parse ISO format timestamp
                    if log_timestamp.endswith('Z'):
                        log_timestamp = log_timestamp[:-1]
                    log_ts = int(datetime.fromisoformat(log_timestamp).timestamp())
                except (ValueError, AttributeError):
                    # Fallback to decoded timestamp if log timestamp can't be parsed
                    log_ts = decoded.timestamp
            else:
                log_ts = log_timestamp if log_timestamp else decoded.timestamp

            # Use the maximum of log timestamp and decoded timestamp to handle clock issues
            # But prefer log timestamp as it's when we actually received the packet
            effective_timestamp = max(log_ts, decoded.timestamp) if log_ts > 0 else decoded.timestamp

            # Skip if we already have a newer entry for this node
            if public_key in self.nodes:
                existing_timestamp = self.nodes[public_key].get('timestamp', 0)
                if effective_timestamp <= existing_timestamp:
                    self.stats['skipped_older_timestamp'] += 1
                    return
                # Debug: Log when we're updating an existing node with newer timestamp
                if effective_timestamp > existing_timestamp:
                    print(f"Updating node {public_key[:8]}...: {datetime.fromtimestamp(existing_timestamp).isoformat()} -> {datetime.fromtimestamp(effective_timestamp).isoformat()}")

            # Build node entry
            last_seen_iso = datetime.fromtimestamp(effective_timestamp).isoformat() + 'Z'

            # Determine first_seen value
            if public_key in self.nodes:
                existing = self.nodes[public_key]
                # Prefer existing first_seen; fallback to previous last_seen or timestamp
                first_seen_value = existing.get('first_seen') or existing.get('last_seen')
                if not first_seen_value and existing.get('timestamp'):
                    first_seen_value = datetime.fromtimestamp(existing['timestamp']).isoformat() + 'Z'
                first_seen_iso = first_seen_value or last_seen_iso
            else:
                first_seen_iso = last_seen_iso

            # # Extract battery voltage (check both decoded object and app_data)
            # battery_voltage = None
            # if hasattr(decoded, 'battery_voltage'):
            #     battery_voltage = decoded.battery_voltage
            # elif app_data and isinstance(app_data, dict):
            #     battery_voltage = app_data.get('battery_voltage')

            node_data = {
                'public_key': public_key,
                'first_seen': first_seen_iso,
                'last_seen': last_seen_iso,
                'timestamp': effective_timestamp,
                'device_role': self._get_device_role(app_data.get('device_role') if app_data else None),
                'name': app_data.get('name', '') if app_data else '',
                'location': app_data.get('location', {'latitude': 0, 'longitude': 0}) if app_data else {'latitude': 0, 'longitude': 0},
                'battery_voltage': app_data.get('battery_voltage', 0) if app_data else 0
            }

            # # Add battery_voltage if available
            # if battery_voltage is not None:
            #     node_data['battery_voltage'] = battery_voltage

            # Merge with API data if available
            if public_key in self.api_nodes:
                api_node = self.api_nodes[public_key]
                # Update location from API if available and not already set
                if node_data['location']['latitude'] == 0 and api_node.get('location'):
                    node_data['location'] = api_node['location']
                # Update name from API if packet doesn't have one
                if not node_data['name'] and api_node.get('name'):
                    node_data['name'] = api_node['name']

            self.nodes[public_key] = node_data
            self.stats['successfully_decoded'] += 1
            device_role_name = "Repeater" if node_data.get('device_role') == 2 else f"Role {node_data.get('device_role')}"
            print(f"Decoded node: {public_key[:8]}... ({node_data.get('name', 'Unnamed')}) [{device_role_name}]")

        except Exception as e:
            print(f"Error decoding packet: {e}")
            self.stats['decode_errors'] += 1

    def _get_device_role(self, role):
        """Convert device role enum to numeric value"""
        # DeviceRole enum values:
        # 1: ChatNode (companion)
        # 2: Repeater
        # 3: RoomServer
        # 4: Sensor
        if isinstance(role, DeviceRole):
            return role.value
        elif isinstance(role, int):
            return role
        else:
            return 1

    def _load_existing_nodes(self):
        """Load nodes from an existing output file to preserve fields like first_seen"""
        try:
            existing_path = Path(self.output_file)
            if not existing_path.exists():
                return
            with open(existing_path, 'r') as f:
                data = json.load(f)
            if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                for node in data['data']:
                    pk = node.get('public_key')
                    if not pk:
                        continue
                    # Ensure required fields exist
                    if 'timestamp' not in node and 'last_seen' in node:
                        # Try to infer numeric timestamp from last_seen if possible (skip if not parseable)
                        try:
                            # last_seen is ISO with trailing Z
                            ts = datetime.fromisoformat(node['last_seen'].rstrip('Z'))
                            node['timestamp'] = int(ts.timestamp())
                        except Exception:
                            continue
                    self.nodes[pk] = node
        except Exception as e:
            print(f"Could not load existing nodes: {e}")

    def save_nodes_json(self, output_file=None):
        """Save nodes to JSON file"""
        if output_file is None:
            output_file = self.output_file

        # Filter to only include repeaters (device_role == 2) with valid public_key
        valid_nodes = [
            node for node in self.nodes.values()
            if node.get('public_key') and node.get('device_role') == 2
        ]

        total_nodes = len(list(self.nodes.values()))
        if len(valid_nodes) < total_nodes:
            filtered_count = total_nodes - len(valid_nodes)
            print(f"Filtered out {filtered_count} nodes (non-repeaters or missing public_key)")

        # Sort nodes by public_key
        sorted_nodes = sorted(valid_nodes, key=lambda x: x['public_key'])

        # Create final data structure
        data = {
            "timestamp": datetime.now().isoformat() + 'Z',
            "data": sorted_nodes
        }

        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"\nSaved {len(sorted_nodes)} nodes to {output_file}")
        # self._print_stats()

    def _print_stats(self):
        """Print processing statistics"""
        print("\n=== Processing Statistics ===")
        print(f"Total log entries processed: {self.stats['total_entries']}")
        print(f"Entries with /packets topic: {self.stats['packet_topic_entries']}")
        print(f"Advert packets (type 4): {self.stats['advert_packet_type']}")
        print(f"With raw hex data: {self.stats['with_raw_hex']}")
        print(f"Successfully decoded: {self.stats['successfully_decoded']}")
        print(f"Invalid/wrong payload type: {self.stats['invalid_or_wrong_type']}")
        print(f"No decoded payload: {self.stats['no_decoded_payload']}")
        print(f"Skipped (older timestamp): {self.stats['skipped_older_timestamp']}")
        print(f"Decode errors: {self.stats['decode_errors']}")
        print(f"Signature verification failed: {self.stats['signature_verification_failed']}")
        print("=" * 30)

    def run(self, only_new=False):
        """Process log file and create nodes.json"""
        print(f"Processing {self.log_file}...")
        # Reset stats for this run
        if not only_new:
            self.stats = {
                'total_entries': 0,
                'packet_topic_entries': 0,
                'advert_packet_type': 0,
                'with_raw_hex': 0,
                'successfully_decoded': 0,
                'invalid_or_wrong_type': 0,
                'no_decoded_payload': 0,
                'skipped_older_timestamp': 0,
                'decode_errors': 0,
                'signature_verification_failed': 0
            }
        # Fetch API data first (only on initial run)
        if not only_new and not self.api_nodes:
            self.fetch_api_data()
        # Then process local packets
        self.process_log_file(only_new=only_new)
        self.save_nodes_json()


def get_region_log_names():
    """Get all unique log names from config.ini [region_logs] section"""
    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        if not config.has_section('region_logs'):
            return []

        # Get unique log names (values in the region_logs section)
        log_names = set()
        for region, log_name in config.items('region_logs'):
            log_names.add(log_name.lower())
        return sorted(list(log_names))
    except Exception as e:
        print(f"Could not read region_logs from config: {e}")
        return []


def get_region_log_file(log_name):
    """Get the log file path for a given log name"""
    log_dir = Path("mqtt_logs")
    if log_name == "data_log":
        return log_dir / "data_log.jsonl"
    else:
        return log_dir / f"data_log_{log_name}.jsonl"


def process_all_regions(api_url=None, watch=False, create_combined=True):
    """Process all region log files and create separate nodes files"""
    log_names = get_region_log_names()

    if not log_names:
        print("No region logs found in config.ini. Processing default data_log.jsonl")
        log_names = ["data_log"]

    print(f"Found {len(log_names)} region log(s): {', '.join(log_names)}")

    all_nodes = {}  # For combined nodes.json
    processors = {}  # Track processors for watch mode

    # Process each region
    for log_name in log_names:
        log_file = get_region_log_file(log_name)
        output_file = f"nodes_{log_name}.json" if log_name != "data_log" else "nodes.json"

        print(f"\n{'='*60}")
        print(f"Processing region: {log_name}")
        print(f"  Log file: {log_file}")
        print(f"  Output file: {output_file}")
        print(f"{'='*60}")

        processor = NodeDataProcessor(str(log_file), api_url=api_url, output_file=output_file)

        if watch:
            processors[log_name] = processor
        else:
            processor.run(only_new=False)
            # Collect nodes for combined file, keeping the newest timestamp for each node
            if create_combined:
                for public_key, node in processor.nodes.items():
                    if public_key not in all_nodes:
                        all_nodes[public_key] = node
                    else:
                        # Keep the node with the newer timestamp
                        existing_ts = all_nodes[public_key].get('timestamp', 0)
                        new_ts = node.get('timestamp', 0)
                        if new_ts > existing_ts:
                            all_nodes[public_key] = node

    # Create combined nodes.json if requested
    if create_combined and not watch and all_nodes:
        print(f"\n{'='*60}")
        print("Creating combined nodes.json...")
        print(f"{'='*60}")
        combined_processor = NodeDataProcessor()
        combined_processor.nodes = all_nodes
        combined_processor.output_file = "nodes.json"
        combined_processor.save_nodes_json()

    # Watch mode for multiple regions
    if watch:
        print("\nWatching for new packet data in all regions... (Ctrl+C to stop)")
        import time
        while True:
            try:
                for log_name, processor in processors.items():
                    log_file = processor.log_file
                    if log_file.exists() and log_file.stat().st_size > 0:
                        processor.run(only_new=True)
                time.sleep(5)  # Check every 5 seconds
            except KeyboardInterrupt:
                print("\nStopping watcher...")
                break


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Decode MQTT packet data and create nodes.json')
    parser.add_argument('--log', default=None, help='Input log file (if not specified, processes all region logs)')
    parser.add_argument('--output', default='nodes.json', help='Output JSON file (only used with --log)')
    parser.add_argument('--watch', action='store_true', help='Watch log file(s) for changes and update nodes.json continuously')
    parser.add_argument('--no-combined', action='store_true', help='Skip creating combined nodes.json when processing all regions')

    args = parser.parse_args()

    # If --log is specified, use single-file mode (backward compatibility)
    if args.log:
        processor = NodeDataProcessor(args.log)
        processor.output_file = args.output

        if args.watch:
            # Watch mode - continuously monitor the log file
            print("Watching for new packet data... (Ctrl+C to stop)")
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
    else:
        # Multi-region mode: process all region logs
        config = configparser.ConfigParser()
        api_url = None
        try:
            config.read('config.ini')
            if config.has_option('meshcore', 'mqtt_api'):
                api_url = config.get('meshcore', 'mqtt_api')
        except Exception:
            pass

        process_all_regions(api_url=api_url, watch=args.watch, create_combined=not args.no_combined)


if __name__ == "__main__":
    main()