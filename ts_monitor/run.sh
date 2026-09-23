#!/bin/bash
# TimeScope - Time-Series Monitoring Platform
# Startup Script

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PORT=${1:-8080}
DATA_DIR="./data"

echo "╔══════════════════════════════════════════════════════╗"
echo "║   TimeScope - 时序数据监控与异常检测平台            ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║   Starting server on port $PORT...                  ║"
echo "║   Dashboard: http://localhost:$PORT                 ║"
echo "║   API Docs:  http://localhost:$PORT/api/status      ║"
echo "╚══════════════════════════════════════════════════════╝"

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is required"
    exit 1
fi

# Create data directory
mkdir -p "$DATA_DIR"

# Start server
python3 server.py "$PORT" "$DATA_DIR"