#!/bin/bash
# Setup script for local development nodes

echo "Setting up local node directories..."

# Create directories for 3 nodes
mkdir -p node_3000
mkdir -p node_4000
mkdir -p node_5000

echo "Created directories:"
echo "  - node_3000"
echo "  - node_4000"
echo "  - node_5000"
echo ""

# Initialize databases for each node
echo "Initializing databases..."

# Check if the p2p binary exists
if [ ! -f "./bin/p2p" ]; then
    echo "Error: ./bin/p2p binary not found. Please build the project first with 'make build'"
    exit 1
fi

# Initialize database for node_3000
echo "  - Initializing node_3000/p2p.db"
./bin/p2p peers --db node_3000/p2p.db > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "    ✓ Database initialized successfully"
else
    echo "    ✗ Failed to initialize database"
fi

# Initialize database for node_4000
echo "  - Initializing node_4000/p2p.db"
./bin/p2p peers --db node_4000/p2p.db > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "    ✓ Database initialized successfully"
else
    echo "    ✗ Failed to initialize database"
fi

# Initialize database for node_5000
echo "  - Initializing node_5000/p2p.db"
./bin/p2p peers --db node_5000/p2p.db > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "    ✓ Database initialized successfully"
else
    echo "    ✗ Failed to initialize database"
fi

echo ""
echo "Setup complete! You can now run the nodes:"
echo "1. ./bin/p2p serve --listen :3000 --db node_3000/p2p.db"
echo "2. ./bin/p2p serve --listen :4000 --db node_4000/p2p.db --bootstrap localhost:3000"
echo "3. ./bin/p2p serve --listen :5000 --db node_5000/p2p.db --bootstrap localhost:4000"
