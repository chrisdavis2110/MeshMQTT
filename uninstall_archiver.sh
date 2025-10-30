#!/bin/bash
# Uninstall the MeshCore Log Archiver

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Uninstalling MeshCore Log Archiver...${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Please run as root (use sudo)${NC}"
    exit 1
fi

# Detect the operating system
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo -e "${YELLOW}Detected Linux system${NC}"

    # Stop and disable timer
    echo -e "${YELLOW}Stopping timer...${NC}"
    systemctl stop logarchiver.timer 2>/dev/null || true
    systemctl disable logarchiver.timer 2>/dev/null || true

    # Remove service and timer files
    echo -e "${YELLOW}Removing service files...${NC}"
    rm -f /etc/systemd/system/logarchiver.service
    rm -f /etc/systemd/system/logarchiver.timer

    # Reload systemd
    echo -e "${YELLOW}Reloading systemd...${NC}"
    systemctl daemon-reload
    systemctl reset-failed

    echo -e "${GREEN}Log archiver uninstalled successfully!${NC}"
fi
