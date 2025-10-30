#!/bin/bash
# Install the MeshCore Services for Linux
# This installs three services:
# 1. mqttwatcher - Watches MQTT broker and logs data
# 2. nodewatcher - Processes advertisements and creates nodes.json
# 3. messagewatcher - Processes GroupText messages and creates messages.json

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Installing MeshCore Services...${NC}"

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
    SERVICES=(
        "mqttwatcher:mqttwatcher.service"
        "nodewatcher:nodewatcher.service"
        "messagewatcher:messagewatcher.service"
    )

    echo -e "${YELLOW}Installing ${#SERVICES[@]} services...${NC}"

    for service_info in "${SERVICES[@]}"; do
        IFS=':' read -r service_name service_file <<< "$service_info"
        SERVICE_FILE="$SCRIPT_DIR/$service_file"

        echo -e "${YELLOW}Installing $service_name service...${NC}"

        # Replace paths in service file
        sed -e "s|/path/to/meshcore/mqtt|$WORK_DIR|g" \
            -e "s|YOUR_USERNAME|$CURRENT_USER|g" \
            "$SERVICE_FILE" > /tmp/${service_file}

        # Copy to systemd directory
        cp /tmp/${service_file} /etc/systemd/system/${service_file}
        chmod 644 /etc/systemd/system/${service_file}

        echo -e "${GREEN}  ✓ $service_name service file installed${NC}"
    done

    # Reload systemd
    echo -e "${YELLOW}Reloading systemd...${NC}"
    systemctl daemon-reload

    # Enable and start services in order
    echo -e "${YELLOW}Enabling and starting services...${NC}"

    # 1. Start MQTT subscriber first
    systemctl enable mqttwatcher.service
    systemctl start mqttwatcher.service
    echo -e "${GREEN}  ✓ mqttwatcher service started${NC}"

    # Wait a moment for MQTT subscriber to create log file
    sleep 2

    # 2. Start node processor
    systemctl enable nodewatcher.service
    systemctl start nodewatcher.service
    echo -e "${GREEN}  ✓ nodewatcher service started${NC}"

    # 3. Start message processor
    systemctl enable messagewatcher.service
    systemctl start messagewatcher.service
    echo -e "${GREEN}  ✓ messagewatcher service started${NC}"

    # Check status of all services
    echo -e "${YELLOW}Checking service status...${NC}"
    for service_info in "${SERVICES[@]}"; do
        IFS=':' read -r service_name service_file <<< "$service_info"
        systemctl status ${service_file%.service} --no-pager -l || true
        echo ""
    done

    echo -e "${GREEN}All services installed and started successfully!${NC}"
    echo ""
    echo -e "${YELLOW}Service Management:${NC}"
    echo "  Start all:   sudo systemctl start mqttwatcher nodewatcher messagewatcher"
    echo "  Stop all:    sudo systemctl stop mqttwatcher nodewatcher messagewatcher"
    echo "  Restart all: sudo systemctl restart mqttwatcher nodewatcher messagewatcher"
    echo "  Status:      sudo systemctl status mqttwatcher nodewatcher messagewatcher"
    echo ""
    echo -e "${YELLOW}View Logs:${NC}"
    echo "  MQTT Subscriber: journalctl -u mqttwatcher -f"
    echo "  Node Processor:  journalctl -u nodewatcher -f"
    echo "  Message Processor: journalctl -u messagewatcher -f"
    echo ""
    echo -e "${YELLOW}Uninstall:${NC}"
    echo "  sudo ./uninstall_service.sh"
    echo ""
fi
