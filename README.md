# MeshMQTT

Subscribes to a MQTT broker, grabs data packets, decodes packets into nodes list and channels/messages list.

## Table of Contents

- [Data Processing Scripts](#data-processing-scripts)
- [Service Management Scripts](#service-management-scripts)
- [Archiving Scripts](#archiving-scripts)

---

## Data Processing Scripts

#### `mqtt_subscriber.py`
The core MQTT subscriber that connects to an MQTT broker and logs all received messages to JSON Lines format. Features:
- Subscribes to configured MQTT topics
- Logs messages to weekly log files (`data_log_YYYY-W##.jsonl`)
- Maintains a symlink `data_log.jsonl` pointing to the current week's log
- Automatically rotates to a new weekly log file at the start of each week
- Handles TLS/SSL connections

**Usage:** `python mqtt_subscriber.py` (typically run as a service)

#### `meshnodes.py`
Processes MQTT log files to extract node information from advertisement packets. Creates and maintains `nodes.json` containing:
- Node public keys
- First seen / last seen timestamps
- Device role (ChatNode, Repeater, RoomServer, Sensor)
- Node name and location
- Battery voltage

**Usage:**
- One-time: `python meshnodes.py --log mqtt_logs/data_log.jsonl`
- Watch mode: `python meshnodes.py --watch`

#### `meshmessages.py`
Processes MQTT log files to extract GroupText messages and organize them by channel. Creates per-channel JSON files in `channels/` directory:
- Decrypts messages if channel keys are configured in `config.ini`
- Groups messages by channel name (if decrypted) or channel hash (if encrypted)
- Maintains message metadata (timestamp, sender, SNR, RSSI, etc.)

**Usage:**
- One-time: `python meshmessages.py --log mqtt_logs/data_log.jsonl`
- Watch mode: `python meshmessages.py --watch`

#### `decode_messages.py`
Decodes and decrypts MeshCore messages from log files or `messages.json`. Can:
- Decode messages from log files (recommended - has full packet data)
- Decode specific messages by hash
- Decrypt messages using channel keys from `config.ini` or command line
- Output decoded messages grouped by channel to separate JSON files

**Usage:**
- Decode from log: `python decode_messages.py --log mqtt_logs/data_log.jsonl`
- Decode specific message: `python decode_messages.py --log mqtt_logs/data_log.jsonl --hash 223F6428`
- Output to files: `python decode_messages.py --log mqtt_logs/data_log.jsonl --output decoded_messages/`

#### `process_all_logs.py`
Processes all weekly log files in chronological order to build a complete `nodes.json`. Useful for:
- Rebuilding `nodes.json` from historical data
- Processing logs after the system has been offline
- Ensuring all nodes are captured, not just the current week

**Usage:** `python process_all_logs.py --log-dir mqtt_logs --output nodes.json`

---

## Service Management Scripts

#### `rotate_service_logs.sh`
Rotates service log files to prevent them from growing too large. When a log file exceeds 10MB, it:
- Compresses the current log file to `.gz`
- Truncates the original file
- Maintains up to 5 rotated log files (`.1.gz`, `.2.gz`, etc.)
- Deletes older rotations beyond the keep limit

Rotates these log files:
- `mqtt_logs/mqtt_subscriber.log`
- `mqtt_logs/mqtt_subscriber_error.log`
- `mqtt_logs/nodewatcher.log`
- `mqtt_logs/nodewatcher_error.log`
- `mqtt_logs/meshmessages.log`
- `mqtt_logs/meshmessages_error.log`

**Usage:** `./rotate_service_logs.sh` (typically run via cron)

---

## Archiving Scripts

#### `archive_logs.py`
Archives MQTT log files by compiling weekly logs into monthly or yearly archives and optionally transfers them to a remote server. Features:
- Compiles weekly log files (`data_log_YYYY-W##.jsonl`) into monthly or yearly archives
- Filters entries by timestamp to create accurate monthly archives (handles weeks that cross month boundaries)
- Supports compression (gzip)
- Transfers archives via SSH/SCP, rsync, or SMB
- Can send individual weekly files or compiled archives
- Configurable via `config.ini` [archive] section

**Usage:**
- Archive previous month: `python archive_logs.py`
- Archive specific month: `python archive_logs.py --month 11 --year 2025`
- Archive entire year: `python archive_logs.py --year 2025`
- Send weekly files individually: `python archive_logs.py --month prev --send-each`

#### `archive_metadata.py`
Archives Mesh metadata files (`nodes.json` and `channels/*.json`) by creating timestamped snapshots and transferring them to a remote server. Features:
- Creates timestamped snapshots of `nodes.json` and all channel JSON files
- Transfers via SSH/SCP, rsync, or SMB
- Can delete original channel files after successful transfer
- Configurable via `config.ini` [archive] section

**Usage:**
- Archive with default tag (YYYY-MM-DD): `python archive_metadata.py`
- Custom tag: `python archive_metadata.py --tag 2025-11-15`
- Delete originals after transfer: `python archive_metadata.py --no-keep --delete-original-channels`

#### `cleanup_logs.py`
Deletes old log files based on retention period configured in `config.ini`. Features:
- Deletes weekly log files older than the retention period (default: 30 days)
- Calculates age based on week number in filename (not file modification time)
- Optionally cleans up archives directory
- Supports dry-run mode to preview what would be deleted
- Configurable retention period via `config.ini` [cleanup] section

**Usage:**
- Run cleanup: `python cleanup_logs.py`
- Dry run: `python cleanup_logs.py --dry-run`
- Custom retention: `python cleanup_logs.py --retention-days 60`
- Include archives: `python cleanup_logs.py --cleanup-archives`

---

## Configuration

Most scripts read configuration from `config.ini`. Key sections:

- **[mqtt]**: MQTT broker connection settings
- **[channels]**: Channel secret keys for message decryption
- **[archive]**: Archive transfer settings (method, host, paths, credentials)
- **[cleanup]**: Log cleanup settings (retention_days, cleanup_archives)

---

## Service Dependencies

The services have dependencies and should be started in this order:

1. **mqttwatcher** (mqtt_subscriber.py) - Must start first to create log files
2. **nodewatcher** (meshnodes.py) - Depends on log files from mqttwatcher
3. **messagewatcher** (meshmessages.py) - Depends on log files from mqttwatcher

The installation scripts handle this ordering automatically.

---

## Notes

- All Python scripts should be run from the project root directory
- Scripts use the virtual environment (`venv/`) when installed as services
- Log files are stored in `mqtt_logs/` directory
- Channel files are stored in `channels/` directory
- Node data is stored in `nodes.json`
