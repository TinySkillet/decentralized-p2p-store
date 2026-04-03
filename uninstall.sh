#!/bin/bash
# Uninstallation script for P2P Decentralized Storage

set -e

echo "=== P2P Storage Uninstallation ==="
echo ""

# Check if running as root
if [ "$EUID" -eq 0 ]; then
  echo "Please do NOT run as root. Run as the user who installed the service."
  exit 1
fi

# Confirm uninstallation
read -p "This will remove the P2P storage service and binary. Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Uninstallation cancelled."
    exit 0
fi

echo ""
echo "Step 1: Stopping and removing systemd service..."

# Stop the service if running
if systemctl is-active --quiet p2p-storage@$USER 2>/dev/null; then
    echo "  - Stopping service..."
    sudo systemctl stop p2p-storage@$USER
fi

# Disable the service if enabled
if systemctl is-enabled --quiet p2p-storage@$USER 2>/dev/null; then
    echo "  - Disabling service..."
    sudo systemctl disable p2p-storage@$USER
fi

# Remove service file
if [ -f /etc/systemd/system/p2p-storage@.service ]; then
    echo "  - Removing service file..."
    sudo rm /etc/systemd/system/p2p-storage@.service
    sudo systemctl daemon-reload
fi

echo "Step 2: Removing binary..."

# Remove binary
if [ -f /usr/local/bin/DecentralizedP2PStorage ]; then
    echo "  - Removing /usr/local/bin/DecentralizedP2PStorage"
    sudo rm /usr/local/bin/DecentralizedP2PStorage
else
    echo "  - Binary not found (already removed)"
fi

echo "Step 3: Cleaning up data..."

# Ask about data removal
read -p "Remove data directory ~/.p2p? This will delete all databases and configs. (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [ -d ~/.p2p ]; then
        echo "  - Removing ~/.p2p directory..."
        rm -rf ~/.p2p
    else
        echo "  - Data directory not found (already removed)"
    fi
else
    echo "  - Keeping data directory ~/.p2p"
fi

echo ""
echo "=== Uninstallation Complete! ==="
echo ""
echo "The following were removed:"
echo "  ✓ Systemd service"
echo "  ✓ Binary (/usr/local/bin/DecentralizedP2PStorage)"
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "  ✓ Data directory (~/.p2p)"
fi
echo ""
echo "The project source code remains in:"
echo "  $(pwd)"
echo ""
echo "To clean up test files in the project directory:"
echo "  rm -rf node_* gossip_test.txt test.txt retrieved.txt bin/p2p"
echo ""
