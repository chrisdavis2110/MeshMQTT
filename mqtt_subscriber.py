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

        # Set up logging directory
        self.log_dir = Path("mqtt_logs")
        self.log_dir.mkdir(exist_ok=True)

        # Set up file logging
        log_file = self.log_dir / f"mqtt_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.setup_logging(log_file)

        # Weekly log rotation
        self.current_week_start = self._get_week_start()
        self._setup_weekly_log()

        # Set up MQTT client
        self.client = mqtt.Client()
        self.client.username_pw_set(self.username, self.password)

        # Set up callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.client.tls_set(cert_reqs=mqtt.ssl.CERT_NONE)
        self.client.tls_insecure_set(True)

        self.logger.info(f"Initialized MQTT subscriber for broker: {self.broker_url}:{self.broker_port}")

    def setup_logging(self, log_file):
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
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_formatter)

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

        # Data file for structured logging (symlink to current week's file)
        self.data_log_symlink = self.log_dir / "data_log.jsonl"

    def _get_week_start(self):
        """Get the start of the current week (Monday)"""
        today = datetime.now()
        # Get Monday of current week (weekday 0 = Monday)
        days_since_monday = today.weekday()
        week_start = today - timedelta(days=days_since_monday)
        return week_start.replace(hour=0, minute=0, second=0, microsecond=0)

    def _get_weekly_log_filename(self, week_start):
        """Get the filename for a weekly log file"""
        # Format: data_log_2025-W01.jsonl (ISO week format)
        year, week_num, _ = week_start.isocalendar()
        return self.log_dir / f"data_log_{year}-W{week_num:02d}.jsonl"

    def _setup_weekly_log(self):
        """Set up or rotate to the current week's log file"""
        current_week_start = self._get_week_start()

        # Check if we need to rotate
        if current_week_start != self.current_week_start:
            self.logger.info(f"Rotating log: New week started ({current_week_start.date()})")
            self.current_week_start = current_week_start

        # Get current week's log file
        weekly_log_file = self._get_weekly_log_filename(self.current_week_start)

        # Create the weekly log file if it doesn't exist
        if not weekly_log_file.exists():
            weekly_log_file.touch()
            self.logger.info(f"Created new weekly log file: {weekly_log_file.name}")

        # Handle existing data_log.jsonl (symlink or regular file)
        if self.data_log_symlink.exists():
            if self.data_log_symlink.is_symlink():
                # Check if it already points to the right file
                try:
                    if self.data_log_symlink.resolve() == weekly_log_file.resolve():
                        # Already correct, no need to recreate
                        self.logger.info(f"Using existing weekly log: {weekly_log_file.name}")
                    else:
                        # Pointing to wrong file (old week), remove and recreate
                        self.data_log_symlink.unlink()
                        self.data_log_symlink.symlink_to(weekly_log_file.resolve())
                        self.logger.info(f"Rotated to new weekly log: {weekly_log_file.name}")
                except OSError:
                    # Broken symlink, remove it
                    self.data_log_symlink.unlink()
                    self.data_log_symlink.symlink_to(weekly_log_file.resolve())
                    self.logger.info(f"Fixed broken symlink, using: {weekly_log_file.name}")
            else:
                # It's a regular file (legacy), rename it and create symlink
                backup_name = self.log_dir / f"data_log_legacy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
                self.data_log_symlink.rename(backup_name)
                self.logger.info(f"Moved legacy data_log.jsonl to {backup_name.name}")
                self.data_log_symlink.symlink_to(weekly_log_file.resolve())
                self.logger.info(f"Created symlink to: {weekly_log_file.name}")
        else:
            # No existing file, create new symlink
            try:
                self.data_log_symlink.symlink_to(weekly_log_file.resolve())
                self.logger.info(f"Created symlink to weekly log: {weekly_log_file.name}")
            except OSError as e:
                # Permission issue or other error
                self.logger.warning(f"Could not create symlink: {e}")

        # Set the actual log file for writing
        self.data_log_file = weekly_log_file

    def _check_log_rotation(self):
        """Check if we need to rotate the log file (new week)"""
        current_week_start = self._get_week_start()
        if current_week_start != self.current_week_start:
            self._setup_weekly_log()

    def log_message_data(self, topic, payload):
        """Log message data in structured JSON Lines format"""
        try:
            # Check if we need to rotate to a new week's log file
            self._check_log_rotation()

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

            with open(self.data_log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            self.logger.error(f"Error logging message data: {e}")

    def on_connect(self, client, userdata, flags, rc):
        """Callback for when client connects to broker"""
        if rc == 0:
            self.logger.info("Successfully connected to MQTT broker")
            # Subscribe to all topics
            for topic in self.topics:
                client.subscribe(topic)
                self.logger.info(f"Subscribed to topic: {topic}")
        else:
            self.logger.error(f"Failed to connect to broker, return code {rc}")

    def on_message(self, client, userdata, msg):
        """Callback for when a message is received"""
        topic = msg.topic
        payload = msg.payload.decode('utf-8')

        # Log to console and file
        self.logger.info(f"Received message from topic: {topic}")
        self.logger.info(f"Payload length: {len(payload)} bytes")

        # Log structured data
        self.log_message_data(topic, payload)

    def on_disconnect(self, client, userdata, rc):
        """Callback for when client disconnects from broker"""
        if rc != 0:
            self.logger.warning(f"Unexpected disconnection from broker (rc={rc})")
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
