#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  TimeScope - 时序数据监控与异常检测平台 启动脚本
# ═══════════════════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${1:-8080}"
DATA_DIR="./data"
PID_FILE="./data/.server.pid"
LOG_FILE="./data/server.log"

# ── 颜色定义 ──
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ── 清理函数 ──
cleanup() {
    echo ""
    echo -e "${YELLOW}正在停止服务...${NC}"
    if [ -f "$PID_FILE" ]; then
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
    echo -e "${GREEN}已停止${NC}"
    exit 0
}
trap cleanup SIGINT SIGTERM

# ── 检查端口占用 ──
check_port() {
    if lsof -i :"$PORT" -t &>/dev/null; then
        echo -e "${RED}端口 $PORT 已被占用${NC}"
        echo -e "使用方法: ${BOLD}$0 [端口号]${NC}"
        echo -e "例: ${BOLD}$0 9090${NC}"
        exit 1
    fi
}

# ── 启动服务 ──
start_server() {
    mkdir -p "$DATA_DIR"

    echo -e "${CYAN}正在启动后端服务...${NC}"
    nohup python3 server.py "$PORT" "$DATA_DIR" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"

    # 等待服务就绪
    for i in $(seq 1 10); do
        if curl -s "http://localhost:$PORT/api/status" &>/dev/null; then
            return 0
        fi
        sleep 0.5
    done

    echo -e "${RED}服务启动失败，查看日志: $LOG_FILE${NC}"
    exit 1
}

# ── 打印信息 ──
print_info() {
    local HOST_IP
    HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    [ -z "$HOST_IP" ] && HOST_IP="localhost"

    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║${NC}  ${BOLD}TimeScope 启动成功${NC}                                   ${GREEN}║${NC}"
    echo -e "${GREEN}╠══════════════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║${NC}                                                        ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}  ${CYAN}前端地址:${NC}                                            ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}    ${BOLD}→ http://localhost:${PORT}${NC}                            ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}    ${BOLD}→ http://${HOST_IP}:${PORT}${NC}                     ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}                                                        ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}  ${CYAN}API 接口:${NC}                                            ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}    ${BOLD}→ http://localhost:${PORT}/api/status${NC}                  ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}                                                        ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}  ${YELLOW}按 Ctrl+C 停止服务${NC}                                   ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}                                                        ${GREEN}║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# ── 主流程 ──
check_port
start_server
print_info

# 前台保持运行，等待信号
echo -e "${CYAN}服务运行中 (PID: $(cat "$PID_FILE"))...${NC}"
wait "$(cat "$PID_FILE")" 2>/dev/null || true