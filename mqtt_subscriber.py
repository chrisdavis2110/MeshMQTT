#!/usr/bin/env python3
"""
MQTT Subscriber - Receives data from MQTT broker and logs to file
"""

import paho.mqtt.client as mqtt
import json
import logging
import configparser
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.utils.enum_names import get_payload_type_name


class MQTTSubscriber:
    def __init__(self, config_file="config.ini"):
        """Initialize MQTT subscriber with configuration"""
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Get MQTT settings from config
        self.broker_url = self.config.get("mqtt", "mqtt_url")
        self.broker_port = self.config.getint("mqtt", "mqtt_port")
        self.username = self.config.get("mqtt", "mqtt_username")
        self.password = self.config.get("mqtt", "mqtt_password")

        # Topics to subscribe to
        topics_string = self.config.get("mqtt", "mqtt_topics")
        self.topics = [topic.strip() for topic in topics_string.split(',')]

        # Load region to log name mappings
        self.region_log_map = {}
        if self.config.has_section("region_logs"):
            for region, log_name in self.config.items("region_logs"):
                self.region_log_map[region.upper()] = log_name.lower()

        # Set up logging directory
        self.log_dir = Path("mqtt_logs")
        self.log_dir.mkdir(exist_ok=True)

        # Set up file logging
        # log_file = self.log_dir / f"mqtt_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.setup_logging()

        # Log region mappings after logger is set up
        if self.region_log_map:
            self.logger.info(f"Loaded region mappings: {self.region_log_map}")

        # Daily log rotation - track per region
        self.current_day_start = self._get_day_start()
        self.region_log_files = {}  # Maps log_name -> log_file_path
        self.region_symlinks = {}   # Maps log_name -> symlink_path
        self.region_day_starts = {}  # Maps log_name -> day_start

        # Set up MQTT client (using callback API version 2)
        try:
            # paho-mqtt 2.x uses CallbackAPIVersion
            self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            # Fallback for older versions
            self.client = mqtt.Client()
        self.client.username_pw_set(self.username, self.password)

        # Set up callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.client.tls_set(cert_reqs=mqtt.ssl.CERT_NONE)
        self.client.tls_insecure_set(True)

        self.logger.info(f"Initialized MQTT subscriber for broker: {self.broker_url}:{self.broker_port}")

    def setup_logging(self):
        """Set up logging to both console and file"""
        # Create logger
        self.logger = logging.getLogger("mqtt_subscriber")
        self.logger.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)

        # File handler for general logs
        # file_handler = logging.FileHandler(log_file)
        # file_handler.setLevel(logging.INFO)
        # file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        # file_handler.setFormatter(file_formatter)

        self.logger.addHandler(console_handler)
        # self.logger.addHandler(file_handler)

        # Legacy data file for structured logging (symlink to current day's file)
        # Kept for backward compatibility
        self.data_log_symlink = self.log_dir / "data_log.jsonl"

    def format_timestamp(self, ts_str: str) -> str:
        """Format timestamp string for display"""
        try:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return ts_str

    def extract_origin_name(self, origin: str) -> str:
        """Extract just the name from origin, removing ' MQTT' suffix and extra whitespace"""
        if not origin:
            return 'unknown'
        # Remove ' MQTT' suffix if present
        name = origin
        return name

    def process_packet_data(self, data: dict, timestamp: str, region: Optional[str] = None) -> Optional[dict]:
        """Process packet data and return formatted info for logging"""
        try:
            # Only process PACKET entries with raw hex
            if data.get('type') != 'PACKET' or not data.get('raw'):
                return None

            raw_hex = data.get('raw', '')
            direction = data.get('direction', 'unknown')
            origin = data.get('origin', 'unknown')
            packet_type_raw = data.get('packet_type', '')

            # Decode the packet
            try:
                packet = MeshCoreDecoder.decode(raw_hex)
            except Exception as e:
                return {
                    'timestamp': timestamp,
                    'direction': direction,
                    'origin': origin,
                    'packet_type': 'decode_error',
                    'raw_packet_type': packet_type_raw,
                    'raw_hex': raw_hex,
                    'region': region,
                    'error': str(e)
                }

            # Get payload type name
            payload_type_name = get_payload_type_name(packet.payload_type) if packet.is_valid else 'Invalid'

            return {
                'timestamp': timestamp,
                'direction': direction,
                'origin': origin,
                'packet_type': payload_type_name,
                'raw_packet_type': packet_type_raw,
                'raw_hex': raw_hex,
                'region': region,
                'is_valid': packet.is_valid
            }
        except Exception as e:
            return {
                'timestamp': timestamp,
                'direction': 'error',
                'origin': 'error',
                'packet_type': 'error',
                'raw_packet_type': '',
                'raw_hex': data.get('raw', ''),
                'region': region,
                'error': str(e)
            }

    def format_packet_output(self, info: dict) -> str:
        """Format packet information for display (matching watch_packets.py format)"""
        timestamp = self.format_timestamp(info['timestamp'])
        direction = info['direction'].upper()
        origin = self.extract_origin_name(info.get('origin', 'unknown'))
        packet_type = info['packet_type']
        raw_hex = info.get('raw_hex', '')
        region = info.get('region', '')

        # Build output line
        parts = [
            direction,
            packet_type,
        ]

        # Add region code if available
        if region:
            parts.append(f"[{region}]")

        parts.append(origin)

        if raw_hex:
            parts.append(f"raw={raw_hex}")

        return " ".join(parts)

    def _get_day_start(self):
        """Get the start of the current day"""
        today = datetime.now()
        return today.replace(hour=0, minute=0, second=0, microsecond=0)

    def _get_daily_log_filename(self, day_start, log_name="data_log"):
        """Get the filename for a daily log file"""
        # Format: data_log_socal_2025-01-15.jsonl or data_log_2025-01-15.jsonl (YYYY-MM-DD format)
        if log_name == "data_log":
            return self.log_dir / f"data_log_{day_start.strftime('%Y-%m-%d')}.jsonl"
        else:
            return self.log_dir / f"data_log_{log_name}_{day_start.strftime('%Y-%m-%d')}.jsonl"

    def _extract_region_from_topic(self, topic):
        """Extract region code from topic (e.g., meshcore/OXR/+/packets -> OXR)"""
        parts = topic.split('/')
        if len(parts) >= 2 and parts[0] == "meshcore":
            return parts[1].upper()
        return None

    def _get_log_name_for_region(self, region):
        """Get the log file name for a given region"""
        if region and region in self.region_log_map:
            return self.region_log_map[region]
        elif region:
            return region.lower()
        else:
            return "data_log"  # Default fallback

    def _setup_daily_log_for_region(self, log_name):
        """Set up or rotate to the current day's log file for a specific region"""
        current_day_start = self._get_day_start()

        # Check if we need to rotate for this region
        if log_name not in self.region_day_starts:
            self.region_day_starts[log_name] = current_day_start
        elif current_day_start != self.region_day_starts[log_name]:
            self.logger.info(f"Rotating log for {log_name}: New day started ({current_day_start.date()})")
            self.region_day_starts[log_name] = current_day_start

        # Get current day's log file
        daily_log_file = self._get_daily_log_filename(current_day_start, log_name)

        # Create the daily log file if it doesn't exist
        if not daily_log_file.exists():
            daily_log_file.touch()
            self.logger.info(f"Created new daily log file: {daily_log_file.name}")

        # Set up symlink for this region
        symlink_name = f"data_log_{log_name}.jsonl" if log_name != "data_log" else "data_log.jsonl"
        symlink_path = self.log_dir / symlink_name

        # Handle existing symlink
        if symlink_path.exists():
            if symlink_path.is_symlink():
                # Check if it already points to the right file
                try:
                    if symlink_path.resolve() == daily_log_file.resolve():
                        # Already correct, no need to recreate
                        pass
                    else:
                        # Pointing to wrong file (old day), remove and recreate
                        symlink_path.unlink()
                        symlink_path.symlink_to(daily_log_file.resolve())
                        self.logger.info(f"Rotated to new daily log: {daily_log_file.name}")
                except OSError:
                    # Broken symlink, remove it
                    symlink_path.unlink()
                    symlink_path.symlink_to(daily_log_file.resolve())
                    self.logger.info(f"Fixed broken symlink, using: {daily_log_file.name}")
            else:
                # It's a regular file (legacy), rename it and create symlink
                backup_name = self.log_dir / f"{symlink_path.stem}_legacy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
                symlink_path.rename(backup_name)
                self.logger.info(f"Moved legacy {symlink_name} to {backup_name.name}")
                symlink_path.symlink_to(daily_log_file.resolve())
                self.logger.info(f"Created symlink to: {daily_log_file.name}")
        else:
            # No existing file, create new symlink
            try:
                symlink_path.symlink_to(daily_log_file.resolve())
                self.logger.debug(f"Created symlink to daily log: {daily_log_file.name}")
            except OSError as e:
                # Permission issue or other error
                self.logger.warning(f"Could not create symlink: {e}")

        # Store the log file and symlink paths
        self.region_log_files[log_name] = daily_log_file
        self.region_symlinks[log_name] = symlink_path

        return daily_log_file

    def _setup_daily_log(self):
        """Set up or rotate to the current day's log file (legacy method for backward compatibility)"""
        # This method is kept for backward compatibility but is no longer the primary method
        # Individual regions are set up via _setup_daily_log_for_region
        current_day_start = self._get_day_start()

        # Check if we need to rotate
        if current_day_start != self.current_day_start:
            self.logger.info(f"Rotating log: New day started ({current_day_start.date()})")
            self.current_day_start = current_day_start

        # Get current day's log file
        daily_log_file = self._get_daily_log_filename(self.current_day_start)

        # Create the daily log file if it doesn't exist
        if not daily_log_file.exists():
            daily_log_file.touch()
            self.logger.info(f"Created new daily log file: {daily_log_file.name}")

        # Handle existing data_log.jsonl (symlink or regular file)
        if self.data_log_symlink.exists():
            if self.data_log_symlink.is_symlink():
                # Check if it already points to the right file
                try:
                    if self.data_log_symlink.resolve() == daily_log_file.resolve():
                        # Already correct, no need to recreate
                        self.logger.info(f"Using existing daily log: {daily_log_file.name}")
                    else:
                        # Pointing to wrong file (old day), remove and recreate
                        self.data_log_symlink.unlink()
                        self.data_log_symlink.symlink_to(daily_log_file.resolve())
                        self.logger.info(f"Rotated to new daily log: {daily_log_file.name}")
                except OSError:
                    # Broken symlink, remove it
                    self.data_log_symlink.unlink()
                    self.data_log_symlink.symlink_to(daily_log_file.resolve())
                    self.logger.info(f"Fixed broken symlink, using: {daily_log_file.name}")
            else:
                # It's a regular file (legacy), rename it and create symlink
                backup_name = self.log_dir / f"data_log_legacy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
                self.data_log_symlink.rename(backup_name)
                self.logger.info(f"Moved legacy data_log.jsonl to {backup_name.name}")
                self.data_log_symlink.symlink_to(daily_log_file.resolve())
                self.logger.info(f"Created symlink to: {daily_log_file.name}")
        else:
            # No existing file, create new symlink
            try:
                self.data_log_symlink.symlink_to(daily_log_file.resolve())
                self.logger.info(f"Created symlink to daily log: {daily_log_file.name}")
            except OSError as e:
                # Permission issue or other error
                self.logger.warning(f"Could not create symlink: {e}")

        # Set the actual log file for writing (legacy)
        self.data_log_file = daily_log_file

    def _check_log_rotation(self, log_name):
        """Check if we need to rotate the log file for a specific region (new day)"""
        current_day_start = self._get_day_start()
        if log_name not in self.region_day_starts or current_day_start != self.region_day_starts[log_name]:
            self._setup_daily_log_for_region(log_name)

    def log_message_data(self, topic, payload):
        """Log message data in structured JSON Lines format"""
        try:
            # Extract region from topic
            region = self._extract_region_from_topic(topic)
            log_name = self._get_log_name_for_region(region)

            # Debug logging
            self.logger.debug(f"Topic: {topic}, Extracted region: {region}, Log name: {log_name}")

            # Check if we need to rotate to a new day's log file for this region
            self._check_log_rotation(log_name)

            # Get the log file for this region (create if needed)
            if log_name not in self.region_log_files:
                self._setup_daily_log_for_region(log_name)

            log_file = self.region_log_files[log_name]
            self.logger.debug(f"Writing to log file: {log_file}")

            # Parse JSON if possible, otherwise keep as string
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                data = {"raw_data": payload}

            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "topic": topic,
                "data": data
            }

            with open(log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            self.logger.error(f"Error logging message data: {e}")

    def on_connect(self, client, userdata, flags, rc, *args):
        """Callback for when client connects to broker"""
        # Handle both API versions: VERSION1 uses int rc, VERSION2 uses ReasonCode object
        if hasattr(rc, 'value'):
            # VERSION2 API - rc is a ReasonCode object
            reason_code = rc.value
        else:
            # VERSION1 API - rc is an int
            reason_code = rc

        if reason_code == 0:
            self.logger.info("Successfully connected to MQTT broker")
            # Subscribe to all topics
            for topic in self.topics:
                client.subscribe(topic)
                self.logger.info(f"Subscribed to topic: {topic}")
        else:
            self.logger.error(f"Failed to connect to broker, return code {reason_code}")

    def on_message(self, client, userdata, msg):
        """Callback for when a message is received"""
        topic = msg.topic
        payload = msg.payload.decode('utf-8')

        # Extract region from topic
        region = self._extract_region_from_topic(topic)

        # Parse JSON if possible
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {"raw_data": payload}

        # Process and log packet data in watch_packets.py format
        timestamp = datetime.now().isoformat()
        packet_info = self.process_packet_data(data, timestamp, region)

        if packet_info:
            # Log in watch_packets.py format
            formatted_output = self.format_packet_output(packet_info)
            self.logger.info(formatted_output)

        # Log structured data to file
        self.log_message_data(topic, payload)

    def on_disconnect(self, client, userdata, rc, *args):
        """Callback for when client disconnects from broker"""
        # Handle both API versions: VERSION1 uses int rc, VERSION2 uses ReasonCode object
        if hasattr(rc, 'value'):
            # VERSION2 API - rc is a ReasonCode object
            reason_code = rc.value
        else:
            # VERSION1 API - rc is an int
            reason_code = rc

        if reason_code != 0:
            self.logger.warning(f"Unexpected disconnection from broker (rc={reason_code})")
        else:
            self.logger.info("Disconnected from broker")

    def start(self):
        """Start the MQTT subscriber"""
        try:
            self.logger.info(f"Connecting to MQTT broker at {self.broker_url}:{self.broker_port}")
            self.client.connect(self.broker_url, self.broker_port, 60)

            # Start the loop to process callbacks
            self.logger.info("Starting MQTT subscriber loop...")
            self.logger.info("Press Ctrl+C to stop")
            self.client.loop_forever()

        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down...")
            self.client.loop_stop()
            self.client.disconnect()
            self.logger.info("MQTT subscriber stopped")
        except Exception as e:
            self.logger.error(f"Error in MQTT subscriber: {e}")
            raise


def main():
    """Main entry point"""
    subscriber = MQTTSubscriber()
    subscriber.start()


if __name__ == "__main__":
    main()
