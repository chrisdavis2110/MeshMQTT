#!/bin/bash
# Install the MeshCore Log Archiver Service/Timer for Linux

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Installing MeshCore Log Archiver...${NC}"

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Please run as root (use sudo)${NC}"
    exit 1
fi

# Detect the operating system
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo -e "${YELLOW}Detected Linux system${NC}"

    # Get the actual working directory
    WORK_DIR=$(pwd)

    # Get current user
    CURRENT_USER=$(whoami)

    # Services to install
    FILES=(
        "logarchiver.service"
        "logarchiver.timer"
    )

    for file in "${FILES[@]}"; do
        SERVICE_FILE="$SCRIPT_DIR/$file"

        echo -e "${YELLOW}Installing $file...${NC}"

        # Replace paths in service file
        sed -e "s|/path/to/meshcore/mqtt|$WORK_DIR|g" \
            -e "s|YOUR_USERNAME|$CURRENT_USER|g" \
            "$SERVICE_FILE" > /tmp/${file}

        # Copy to systemd directory
        cp /tmp/${file} /etc/systemd/system/${file}
        chmod 644 /etc/systemd/system/${file}

        echo -e "${GREEN}  ✓ $file installed${NC}"
    done

    # Reload systemd
    echo -e "${YELLOW}Reloading systemd...${NC}"
    systemctl daemon-reload

    # Enable and start timer
    echo -e "${YELLOW}Enabling and starting timer...${NC}"
    systemctl enable logarchiver.timer
    systemctl start logarchiver.timer

    echo -e "${GREEN}Log archiver timer installed and started!${NC}"
    echo ""
    echo -e "${YELLOW}Timer Status:${NC}"
    systemctl status logarchiver.timer --no-pager -l || true
    echo ""
    echo -e "${YELLOW}View Timer:${NC}"
    echo "  Status:      sudo systemctl status logarchiver.timer"
    echo "  List timers: sudo systemctl list-timers logarchiver"
    echo ""
    echo -e "${YELLOW}Manual Run:${NC}"
    echo "  sudo systemctl start logarchiver"
    echo ""
    echo -e "${YELLOW}Uninstall:${NC}"
    echo "  sudo ./uninstall_archiver.sh"
    echo ""
fi
