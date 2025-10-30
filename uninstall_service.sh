#!/bin/bash
# Uninstall the MeshCore Services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Uninstalling MeshCore Services...${NC}"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Please run as root (use sudo)${NC}"
    exit 1
fi

# Detect the operating system
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo -e "${YELLOW}Detected Linux system${NC}"

    # Services to uninstall (in reverse order of dependencies)
    SERVICES=(
        "messagewatcher:messagewatcher.service"
        "nodewatcher:nodewatcher.service"
        "mqttwatcher:mqttwatcher.service"
    )

    for service_info in "${SERVICES[@]}"; do
        IFS=':' read -r service_name service_file <<< "$service_info"
        SERVICE_NAME=${service_file%.service}

        echo -e "${YELLOW}Uninstalling $service_name service...${NC}"

        # Stop the service
        systemctl stop ${SERVICE_NAME} 2>/dev/null || true

        # Disable the service
        systemctl disable ${SERVICE_NAME} 2>/dev/null || true

        # Remove the service file
        rm -f /etc/systemd/system/${service_file}

        echo -e "${GREEN}  ✓ $service_name service removed${NC}"
    done

    # Reload systemd
    echo -e "${YELLOW}Reloading systemd...${NC}"
    systemctl daemon-reload
    systemctl reset-failed

    echo -e "${GREEN}All services uninstalled successfully!${NC}"
fi