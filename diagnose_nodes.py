#!/usr/bin/env python3
"""
Diagnostic script to analyze node differences between environments
"""

import json
from pathlib import Path
from collections import Counter


def load_nodes(filepath):
    """Load nodes from a JSON file"""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        if isinstance(data, dict) and 'data' in data:
            return {node['public_key']: node for node in data['data']}
        return {}
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return {}


def analyze_nodes(nodes_dict, label):
    """Analyze and print statistics about nodes"""
    print(f"\n=== {label} ===")
    print(f"Total nodes: {len(nodes_dict)}")

    if not nodes_dict:
        return

    # Count by device role
    roles = Counter(node.get('device_role', 'unknown') for node in nodes_dict.values())
    print(f"\nBy device role:")
    for role, count in sorted(roles.items()):
        print(f"  Role {role}: {count}")

    # Nodes with/without names
    with_names = sum(1 for node in nodes_dict.values() if node.get('name'))
    print(f"\nNodes with names: {with_names}")
    print(f"Nodes without names: {len(nodes_dict) - with_names}")

    # Public keys (first 8 chars)
    print(f"\nPublic keys (first 8 chars):")
    for pk in sorted(nodes_dict.keys())[:10]:
        node = nodes_dict[pk]
        name = node.get('name', 'Unnamed')
        role = node.get('device_role', '?')
        print(f"  {pk[:8]}... - {name} (role: {role})")
    if len(nodes_dict) > 10:
        print(f"  ... and {len(nodes_dict) - 10} more")


def compare_nodes(dev_nodes, prod_nodes):
    """Compare dev and prod nodes"""
    dev_keys = set(dev_nodes.keys())
    prod_keys = set(prod_nodes.keys())

    only_in_dev = dev_keys - prod_keys
    only_in_prod = prod_keys - dev_keys
    in_both = dev_keys & prod_keys

    print(f"\n=== Comparison ===")
    print(f"Nodes in both: {len(in_both)}")
    print(f"Nodes only in DEV: {len(only_in_dev)}")
    print(f"Nodes only in PROD: {len(only_in_prod)}")

    if only_in_dev:
        print(f"\n=== Nodes only in DEV ({len(only_in_dev)}):")
        for pk in sorted(only_in_dev)[:10]:
            node = dev_nodes[pk]
            name = node.get('name', 'Unnamed')
            role = node.get('device_role', '?')
            last_seen = node.get('last_seen', 'unknown')
            print(f"  {pk[:8]}... - {name} (role: {role}, last_seen: {last_seen})")
        if len(only_in_dev) > 10:
            print(f"  ... and {len(only_in_dev) - 10} more")

    if only_in_prod:
        print(f"\n=== Nodes only in PROD ({len(only_in_prod)}):")
        for pk in sorted(only_in_prod)[:10]:
            node = prod_nodes[pk]
            name = node.get('name', 'Unnamed')
            role = node.get('device_role', '?')
            last_seen = node.get('last_seen', 'unknown')
            print(f"  {pk[:8]}... - {name} (role: {role}, last_seen: {last_seen})")
        if len(only_in_prod) > 10:
            print(f"  ... and {len(only_in_prod) - 10} more")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Compare nodes.json files')
    parser.add_argument('--dev', default='nodes.json', help='Dev nodes.json file')
    parser.add_argument('--prod', required=True, help='Prod nodes.json file (path or paste content)')

    args = parser.parse_args()

    # Load dev nodes
    dev_nodes = load_nodes(args.dev)
    analyze_nodes(dev_nodes, "DEV Environment")

    # Load prod nodes
    prod_path = Path(args.prod)
    if prod_path.exists():
        prod_nodes = load_nodes(str(prod_path))
    else:
        # Try to parse as JSON string
        try:
            data = json.loads(args.prod)
            if isinstance(data, dict) and 'data' in data:
                prod_nodes = {node['public_key']: node for node in data['data']}
            else:
                print("Invalid prod JSON format")
                return
        except json.JSONDecodeError:
            print("Could not parse prod as JSON. Provide a file path or valid JSON string.")
            return

    analyze_nodes(prod_nodes, "PROD Environment")

    # Compare
    compare_nodes(dev_nodes, prod_nodes)


if __name__ == "__main__":
    main()
