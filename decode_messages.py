#!/usr/bin/env python3
"""
Message Decoder - Decode messages from messages.json or log files

This script can decode MeshCore messages from:
1. messages.json (if full ciphertext is available - note: current messages.json has truncated data)
2. mqtt_logs/data_log.jsonl (recommended - contains full packet data)
"""

import json
import argparse
import configparser
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.crypto import MeshCoreKeyStore
from meshcoredecoder.types.enums import PayloadType
from meshcoredecoder.types.crypto import DecryptionOptions


def decode_from_messages_json(messages_file: str, keys: Optional[Dict[str, str]] = None):
    """
    Attempt to decode messages from messages.json

    NOTE: Current messages.json has truncated ciphertext, so this will show limitations
    """
    print(f"Reading messages from: {messages_file}")

    with open(messages_file, 'r') as f:
        data = json.load(f)

    messages = data.get('data', [])
    print(f"Found {len(messages)} messages\n")

    # Set up decryption if keys provided
    decryption_options = None
    if keys:
        key_store = MeshCoreKeyStore(keys)
        decryption_options = DecryptionOptions(key_store=key_store)

    decoded_count = 0
    for i, msg in enumerate(messages, 1):
        print(f"--- Message {i} ---")
        print(f"Hash: {msg.get('message_hash', 'N/A')}")
        print(f"Channel: {msg.get('channel_hash', 'N/A')}")
        print(f"Timestamp: {msg.get('timestamp', 'N/A')}")
        print(f"Decrypted: {msg.get('decrypted', False)}")

        ciphertext = msg.get('ciphertext', '')
        if ciphertext:
            if ciphertext.endswith('...'):
                print(f"⚠️  Ciphertext is truncated in messages.json (shows only first 32 bytes)")
                print(f"   To decode this message, use the original log file (data_log.jsonl)")
                print(f"   and look for message_hash: {msg.get('message_hash')}\n")
            else:
                print(f"Decoding ciphertext: {ciphertext[:64]}...")
                try:
                    packet = MeshCoreDecoder.decode(ciphertext, decryption_options)
                    if packet.is_valid and packet.payload_type == PayloadType.GroupText:
                        payload = packet.payload
                        if payload and payload.get('decoded'):
                            group_text = payload['decoded']
                            if hasattr(group_text, 'decrypted') and group_text.decrypted:
                                decrypted = group_text.decrypted
                                print(f"✅ Decrypted Message:")
                                print(f"   Sender: {decrypted.get('sender', 'N/A')}")
                                print(f"   Message: {decrypted.get('message', 'N/A')}")
                                decoded_count += 1
                    print()
                except Exception as e:
                    print(f"❌ Error decoding: {e}\n")

        # If already decrypted in JSON
        if msg.get('decrypted') is True:
            sender = msg.get('sender', 'N/A')
            message = msg.get('message', 'N/A')
            print(f"✅ Already decrypted:")
            print(f"   Sender: {sender}")
            print(f"   Message: {message}")
            decoded_count += 1
            print()

    print(f"\nSummary: {decoded_count}/{len(messages)} messages decoded/decrypted")


def compute_channel_hash(secret_key: str) -> str:
    """
    Compute channel hash from secret key (first byte of SHA256)
    Returns: Channel hash as uppercase hex string (e.g., "D9")
    """
    # Convert hex string to bytes if needed
    try:
        if len(secret_key) == 32:  # Hex string (16 bytes = 32 hex chars)
            key_bytes = bytes.fromhex(secret_key)
        else:
            # Assume it's already bytes or raw string
            key_bytes = secret_key.encode() if isinstance(secret_key, str) else secret_key
    except ValueError:
        # If it's not valid hex, treat as raw string
        key_bytes = secret_key.encode() if isinstance(secret_key, str) else secret_key

    # Compute SHA256 and get first byte
    sha256_hash = hashlib.sha256(key_bytes).digest()
    channel_hash = sha256_hash[0]

    # Return as uppercase hex string (2 characters)
    return f"{channel_hash:02X}"


def load_keys_from_config(config_file: str = "config.ini") -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Load channel keys from config.ini and compute their channel hashes
    Returns: (channel_keys_by_hash, channel_keys_by_name)
      - channel_keys_by_hash: Dict mapping channel_hash to secret key
      - channel_keys_by_name: Dict mapping channel_name to (channel_hash, secret)
    """
    config = configparser.ConfigParser()
    config.read(config_file)

    if not config.has_section('channels'):
        return {}, {}

    keys_by_hash: Dict[str, str] = {}
    keys_by_name: Dict[str, Tuple[str, str]] = {}

    for channel_name in config.options('channels'):
        secret = config.get('channels', channel_name).strip()
        channel_hash = compute_channel_hash(secret)
        keys_by_hash[channel_hash] = secret
        keys_by_name[channel_name] = (channel_hash, secret)
        print(f"  {channel_name}: hash={channel_hash}")

    return keys_by_hash, keys_by_name


def decode_from_log_file(log_file: str, message_hash: Optional[str] = None,
                        keys: Optional[Dict[str, str]] = None,
                        output_dir: Optional[str] = None):
    """
    Decode messages from the log file (recommended method)

    Args:
        log_file: Path to the log file
        message_hash: Optional specific message hash to decode
        keys: Optional dict mapping channel_hash to secret key
        output_dir: If provided, save each channel's messages to separate JSON files
    """
    print(f"Reading log file: {log_file}")

    log_path = Path(log_file)
    if not log_path.exists():
        print(f"❌ Log file not found: {log_file}")
        return

    # Load keys from config if not provided
    channel_keys_by_hash = None
    channel_keys_by_name = None

    if keys is None:
        print("Loading channel keys from config.ini...")
        channel_keys_by_hash, channel_keys_by_name = load_keys_from_config()
        if channel_keys_by_hash:
            print(f"Computed channel hashes for {len(channel_keys_by_hash)} keys:")
            keys = channel_keys_by_hash
        else:
            print("⚠️  No channel keys found in config.ini")
            keys = {}

    # Find all messages first
    found_messages = []
    with open(log_path, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                if entry.get('topic', '').endswith('/packets'):
                    data = entry.get('data', {})
                    if data.get('type') == 'PACKET':
                        raw_hex = data.get('raw', '')
                        if raw_hex:
                            try:
                                packet = MeshCoreDecoder.decode(raw_hex)
                                if packet.is_valid and packet.payload_type == PayloadType.GroupText:
                                    # Check if this is the message we're looking for
                                    if message_hash is None or packet.message_hash == message_hash:
                                        found_messages.append({
                                            'packet': packet,
                                            'entry': entry,
                                            'data': data,
                                            'raw_hex': raw_hex
                                        })
                            except Exception as e:
                                continue
            except json.JSONDecodeError:
                continue

    print(f"Found {len(found_messages)} GroupText messages\n")

    # If output_dir is specified, group messages by channel
    if output_dir:
        return decode_and_group_by_channel(found_messages, keys, channel_keys_by_name, output_dir)

    # Otherwise, just print decoded messages
    decoded_count = 0
    for i, msg_data in enumerate(found_messages, 1):
        packet = msg_data['packet']
        entry = msg_data['entry']
        data = msg_data['data']
        raw_hex = msg_data['raw_hex']

        print(f"--- Message {i} ---")
        print(f"Hash: {packet.message_hash}")
        print(f"Timestamp: {entry.get('timestamp', 'N/A')}")
        print(f"Origin: {data.get('origin', 'N/A')}")
        print(f"Route Type: {packet.route_type.name if packet.route_type else 'N/A'}")

        payload = packet.payload
        if payload and payload.get('decoded'):
            group_text = payload['decoded']
            channel_hash = group_text.channel_hash if hasattr(group_text, 'channel_hash') else None
            print(f"Channel Hash: {channel_hash}")

            # Try to decrypt with available keys
            decrypted_data = None
            channel_name = None
            if keys:
                decrypted_data, channel_name = try_decrypt_message(raw_hex, channel_hash, keys, channel_keys_by_name)

            if decrypted_data:
                print(f"✅ Decrypted Message (channel: {channel_name}):")
                print(f"   Sender: {decrypted_data.get('sender', 'N/A')}")
                print(f"   Message: {decrypted_data.get('message', 'N/A')}")
                if decrypted_data.get('timestamp'):
                    ts = datetime.fromtimestamp(decrypted_data['timestamp'])
                    print(f"   Timestamp: {ts.isoformat()}")
                decoded_count += 1
            elif hasattr(group_text, 'decrypted') and group_text.decrypted:
                decrypted = group_text.decrypted
                print(f"✅ Decrypted Message:")
                print(f"   Sender: {decrypted.get('sender', 'N/A')}")
                print(f"   Message: {decrypted.get('message', 'N/A')}")
                if decrypted.get('timestamp'):
                    ts = datetime.fromtimestamp(decrypted['timestamp'])
                    print(f"   Timestamp: {ts.isoformat()}")
                decoded_count += 1
            else:
                print(f"⚠️  Message is encrypted (channel: {channel_hash})")
                if hasattr(group_text, 'ciphertext'):
                    ciphertext = group_text.ciphertext
                    print(f"   Ciphertext: {ciphertext[:64]}...")
                    if keys:
                        print(f"   Could not decrypt with any available key")
                    else:
                        print(f"   To decrypt, provide channel secret key using --key option")

        print(f"SNR: {data.get('SNR', 'N/A')}, RSSI: {data.get('RSSI', 'N/A')}")
        print()

    print(f"\nSummary: {decoded_count}/{len(found_messages)} messages decrypted")


def try_decrypt_message(raw_hex: str, channel_hash: str,
                       channel_keys_by_hash: Dict[str, str],
                       channel_keys_by_name: Optional[Dict[str, Tuple[str, str]]] = None) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Try to decrypt a message using the key for its channel_hash.
    Returns: (decrypted_data, channel_name) if successful, (None, None) otherwise
    """
    # Look up key by channel_hash - MeshCoreKeyStore expects a list of all secrets
    # and will automatically match them by computed hash
    if channel_hash and channel_keys_by_hash:
        # Get all secret keys as a list
        all_secrets = list(channel_keys_by_hash.values())

        try:
            key_store = MeshCoreKeyStore({
                'channel_secrets': all_secrets
            })
            decryption_options = DecryptionOptions(key_store=key_store)
            packet = MeshCoreDecoder.decode(raw_hex, decryption_options)

            payload = packet.payload
            if payload and payload.get('decoded'):
                group_text = payload['decoded']
                if hasattr(group_text, 'decrypted') and group_text.decrypted:
                    # Find channel name for this hash
                    channel_name = None
                    if channel_keys_by_name:
                        for name, (hash_val, _) in channel_keys_by_name.items():
                            if hash_val.upper() == channel_hash.upper():
                                channel_name = name
                                break
                    return group_text.decrypted, channel_name or channel_hash
        except Exception as e:
            # Silently fail
            pass

    return None, None


def decode_and_group_by_channel(messages: List[Dict],
                                channel_keys_by_hash: Dict[str, str],
                                channel_keys_by_name: Optional[Dict[str, Tuple[str, str]]],
                                output_dir: str):
    """
    Decode messages and group them by channel, saving each channel to a separate JSON file
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)

    # Track messages by channel
    channel_messages: Dict[str, List[Dict]] = {}
    channel_hash_to_name: Dict[str, str] = {}  # Map discovered channel_hash -> channel_name

    decoded_count = 0
    encrypted_count = 0

    print("Decrypting messages and grouping by channel...\n")

    for msg_data in messages:
        packet = msg_data['packet']
        entry = msg_data['entry']
        data = msg_data['data']
        raw_hex = msg_data['raw_hex']

        payload = packet.payload
        if not payload or not payload.get('decoded'):
            continue

        group_text = payload['decoded']
        channel_hash = group_text.channel_hash if hasattr(group_text, 'channel_hash') else None

        if not channel_hash:
            continue

        # Try to decrypt with available keys
        decrypted_data, channel_name = try_decrypt_message(raw_hex, channel_hash, channel_keys_by_hash, channel_keys_by_name)

        # Build message entry
        message_entry = {
            'message_hash': packet.message_hash,
            'timestamp': entry.get('timestamp'),
            'received_time': datetime.now().isoformat() + 'Z',
            'origin': data.get('origin', ''),
            'origin_id': data.get('origin_id', ''),
            'route_type': packet.route_type.name if packet.route_type else None,
            'channel_hash': channel_hash,
            'SNR': data.get('SNR'),
            'RSSI': data.get('RSSI'),
            'score': data.get('score'),
            'decrypted': False
        }

        if decrypted_data:
            # Successfully decrypted
            message_entry['decrypted'] = True
            message_entry['channel_name'] = channel_name
            message_entry['sender'] = decrypted_data.get('sender', '')
            message_entry['message'] = decrypted_data.get('message', '')
            if decrypted_data.get('timestamp'):
                message_entry['message_timestamp'] = datetime.fromtimestamp(
                    decrypted_data['timestamp']
                ).isoformat() + 'Z'
            decoded_count += 1

            # Track channel mapping
            if channel_hash not in channel_hash_to_name:
                channel_hash_to_name[channel_hash] = channel_name

            # Group by channel name
            if channel_name not in channel_messages:
                channel_messages[channel_name] = []
            channel_messages[channel_name].append(message_entry)
        else:
            # Still encrypted - group by channel_hash
            encrypted_count += 1
            encrypted_channel = f"encrypted_{channel_hash}"
            if encrypted_channel not in channel_messages:
                channel_messages[encrypted_channel] = []
            channel_messages[encrypted_channel].append(message_entry)

    # Save each channel to a JSON file
    print(f"\nSaving messages to {output_dir}/...\n")

    for channel, msgs in channel_messages.items():
        if not msgs:
            continue

        # Sort by timestamp (most recent first)
        sorted_messages = sorted(
            msgs,
            key=lambda x: x.get('timestamp', ''),
            reverse=True
        )

        # Create output structure
        output_data = {
            "timestamp": datetime.now().isoformat() + 'Z',
            "channel": channel,
            "channel_hash": sorted_messages[0].get('channel_hash') if sorted_messages else None,
            "count": len(sorted_messages),
            "decrypted_count": sum(1 for m in sorted_messages if m.get('decrypted', False)),
            "data": sorted_messages
        }

        # Save to file
        filename = f"{channel}.json"
        filepath = output_path / filename

        with open(filepath, 'w') as f:
            json.dump(output_data, f, indent=2)

        decrypted = output_data["decrypted_count"]
        print(f"✅ {filename}: {len(sorted_messages)} messages ({decrypted} decrypted)")

    print(f"\nSummary:")
    print(f"  Total messages processed: {len(messages)}")
    print(f"  Successfully decrypted: {decoded_count}")
    print(f"  Still encrypted: {encrypted_count}")
    print(f"  Channels: {', '.join(channel_messages.keys())}")
    if channel_hash_to_name:
        print(f"\nChannel hash mappings:")
        for hash_val, name in channel_hash_to_name.items():
            print(f"  {hash_val} -> {name}")


def main():
    parser = argparse.ArgumentParser(
        description='Decode MeshCore messages from messages.json or log files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Decode from messages.json (may be limited due to truncated ciphertext)
  python decode_messages.py --messages messages.json

  # Decode from log file (recommended - has full packet data)
  python decode_messages.py --log mqtt_logs/data_log.jsonl

  # Decode specific message by hash
  python decode_messages.py --log mqtt_logs/data_log.jsonl --hash 223F6428

  # Decode with decryption keys
  python decode_messages.py --log mqtt_logs/data_log.jsonl --key 11:YOUR_CHANNEL_SECRET
        """
    )

    parser.add_argument('--messages', help='Decode from messages.json file')
    parser.add_argument('--log', help='Decode from log file (recommended)')
    parser.add_argument('--hash', help='Specific message hash to decode (requires --log)')
    parser.add_argument('--key', action='append', dest='keys',
                       help='Channel secret keys for decryption (format: channel_hash:secret_hex)')
    parser.add_argument('--output', '-o', help='Output directory to save channel JSON files (each channel gets a separate file)')
    parser.add_argument('--config', default='config.ini', help='Config file path (default: config.ini)')

    args = parser.parse_args()

    # Parse decryption keys from command line if provided
    key_dict = None
    if args.keys:
        key_dict = {}
        for key_spec in args.keys:
            try:
                channel_hash, secret = key_spec.split(':', 1)
                key_dict[channel_hash] = secret
            except ValueError:
                print(f"⚠️  Invalid key format: {key_spec} (expected channel_hash:secret_hex)")
                return

    # Decide which method to use
    if args.log:
        decode_from_log_file(args.log, args.hash, key_dict, args.output)
    elif args.messages:
        decode_from_messages_json(args.messages, key_dict)
    else:
        # Default: try messages.json if it exists
        if Path('messages.json').exists():
            print("No source specified, using messages.json\n")
            decode_from_messages_json('messages.json', key_dict)
        else:
            parser.print_help()


if __name__ == "__main__":
    main()
