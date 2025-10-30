#!/usr/bin/env python3
"""
Node Data Processor - Decode MQTT packet data and create nodes.json
"""

import json
import sys
import requests
import configparser
from datetime import datetime
from pathlib import Path

from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.types.enums import PayloadType, DeviceRole


class NodeDataProcessor:
    def __init__(self, log_file=None, api_url=None):
        """Initialize the node data processor"""
        self.nodes = {}
        if log_file is None:
            log_file = "mqtt_logs/data_log.jsonl"
        self.log_file = Path(log_file)
        self.api_url = api_url
        self.api_nodes = {}
        self.output_file = "nodes.json"
        self.processed_lines = 0

        # Load config if available
        self._load_config()

        # Load existing nodes (to preserve first_seen across runs)
        self._load_existing_nodes()

    def _load_config(self):
        """Load configuration from config.ini"""
        try:
            config = configparser.ConfigParser()
            config.read('config.ini')
            if not self.api_url and config.has_option('meshcore', 'mqtt_api'):
                self.api_url = config.get('meshcore', 'mqtt_api')
        except Exception as e:
            print(f"Could not load config: {e}")

    def fetch_api_data(self):
        """Fetch node data from the API"""
        if not self.api_url:
            return

        try:
            print(f"Fetching node data from API...")
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

            # Only process advertisement packets (type 4)
            if data.get('packet_type') == '4':
                raw_hex = data.get('raw', '')
                if raw_hex:
                    self.decode_and_store(raw_hex, entry.get('timestamp'))
        except Exception as e:
            print(f"Error processing packet: {e}")

    def decode_and_store(self, hex_string, timestamp):
        """Decode a packet and store node information"""
        try:
            packet = MeshCoreDecoder.decode(hex_string)

            # Only process valid advertisement packets
            if not packet.is_valid or packet.payload_type != PayloadType.Advert:
                return

            decoded = packet.payload.get('decoded')
            if not decoded:
                return

            # Extract node information
            public_key = decoded.public_key
            app_data = decoded.app_data

            # Skip if we already have a newer entry for this node
            if public_key in self.nodes:
                existing_timestamp = self.nodes[public_key].get('timestamp', 0)
                if decoded.timestamp <= existing_timestamp:
                    return

            # Build node entry
            node_data = {
                'public_key': public_key,
                'last_seen': datetime.fromtimestamp(decoded.timestamp).isoformat() + 'Z',
                'timestamp': decoded.timestamp,
                'device_role': self._get_device_role(app_data.get('device_role')),
                'name': app_data.get('name', ''),
                'location': app_data.get('location', {'latitude': 0, 'longitude': 0})
            }

            # Set or preserve first_seen
            if public_key in self.nodes:
                existing = self.nodes[public_key]
                # Prefer existing first_seen; fallback to previous last_seen or timestamp
                first_seen_value = existing.get('first_seen') or existing.get('last_seen')
                if not first_seen_value and existing.get('timestamp'):
                    first_seen_value = datetime.fromtimestamp(existing['timestamp']).isoformat() + 'Z'
                node_data['first_seen'] = first_seen_value or node_data['last_seen']
            else:
                node_data['first_seen'] = node_data['last_seen']

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
            print(f"Decoded node: {public_key[:8]}... ({node_data.get('name', 'Unnamed')})")

        except Exception as e:
            print(f"Error decoding packet: {e}")

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

        # Sort nodes by public_key
        sorted_nodes = sorted(self.nodes.values(), key=lambda x: x['public_key'])

        # Create final data structure
        data = {
            "timestamp": datetime.now().isoformat() + 'Z',
            "data": sorted_nodes
        }

        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"\nSaved {len(sorted_nodes)} nodes to {output_file}")

    def run(self, only_new=False):
        """Process log file and create nodes.json"""
        print(f"Processing {self.log_file}...")
        # Fetch API data first (only on initial run)
        if not only_new and not self.api_nodes:
            self.fetch_api_data()
        # Then process local packets
        self.process_log_file(only_new=only_new)
        self.save_nodes_json()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Decode MQTT packet data and create nodes.json')
    parser.add_argument('--log', default='mqtt_logs/data_log.jsonl', help='Input log file')
    parser.add_argument('--output', default='nodes.json', help='Output JSON file')
    parser.add_argument('--watch', action='store_true', help='Watch log file for changes and update nodes.json continuously')

    args = parser.parse_args()

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


if __name__ == "__main__":
    main()