#!/bin/bash
# Installation script for P2P Decentralized Storage

set -e

echo "=== P2P Storage Installation ==="
echo ""

# Check if running as root
if [ "$EUID" -eq 0 ]; then
  echo "Please do NOT run as root. Run as the user who will run the service."
  exit 1
fi

# Build the binary
echo "Building binary..."
go build -o bin/p2p

# Install binary
echo "Installing binary to /usr/local/bin/ (requires sudo)..."
sudo cp bin/p2p /usr/local/bin/DecentralizedP2PStorage
sudo chmod +x /usr/local/bin/DecentralizedP2PStorage

# Create config directory
echo "Creating config directory..."
mkdir -p ~/.p2p
chmod 700 ~/.p2p

# Create default config file if it doesn't exist
if [ ! -f ~/.p2p/config ]; then
    echo "Creating default config file..."
    cp config.example ~/.p2p/config
    echo "  - Config created at ~/.p2p/config"
    echo "  - Edit this file to customize bootstrap nodes"
else
    echo "  - Config file already exists at ~/.p2p/config"
fi

# Copy systemd service file
echo "Installing systemd service..."
sudo cp p2p-storage@.service /etc/systemd/system/
sudo systemctl daemon-reload

echo ""
echo "=== Installation Complete! ==="
echo ""
echo "Next steps:"
echo "1. Configure your node:"
echo "   - Edit bootstrap nodes if needed"
echo "   - Set listen address"
echo ""
echo "2. Start the service:"
echo "   sudo systemctl start p2p-storage@$USER"
echo ""
echo "3. Enable on boot (optional):"
echo "   sudo systemctl enable p2p-storage@$USER"
echo ""
echo "4. Check status:"
echo "   sudo systemctl status p2p-storage@$USER"
echo ""
echo "5. View logs:"
echo "   sudo journalctl -u p2p-storage@$USER -f"
echo ""
