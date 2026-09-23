# TimeScope - 时序数据监控与异常检测平台

一个完整的时序数据监控与异常检测平台，包含 Python 后端和 HTML 前端仪表盘。

## 架构概览

```
ts_monitor/
├── server.py          # HTTP API 服务器（多线程）
├── storage.py         # 时序数据存储引擎（JSON 分片）
├── anomaly.py         # 异常检测算法（Z-score/EWMA/移动中位数）
├── downsample.py      # LTTB 降采样算法
├── run.sh             # 启动脚本
├── data/              # 数据目录
│   ├── timeseries/    # 按小时分片的时序数据
│   ├── metadata.json  # 数据源元数据
│   ├── rules.json     # 检测规则
│   └── alerts.json    # 告警记录
└── ../ts_dashboard.html  # 前端仪表盘
```

## 快速启动

```bash
cd ts_monitor
python3 server.py 8080
```

然后访问 http://localhost:8080

## 功能特性

### 前端（5 个页面）

1. **实时仪表盘** - ECharts 多指标曲线图、热力图、统计卡片
2. **数据源配置** - 管理 API/模拟器/文件数据源
3. **历史查询** - 时间范围选择、LTTB 降采样、CSV 导出
4. **异常告警** - 告警列表、状态过滤、确认/解决操作
5. **规则管理** - CRUD 检测规则、多算法配置

### 后端核心能力

| 功能 | 说明 |
|------|------|
| 高吞吐写入 | 写入缓冲区 + 批量刷盘 |
| JSON 分片存储 | 按小时自动分片，原子写入 |
| LTTB 降采样 | Largest Triangle Three Buckets，保留视觉形状 |
| Z-score 检测 | 基于滑动窗口的标准差检测 |
| EWMA 检测 | 指数加权移动平均，检测渐变漂移 |
| 移动中位数 | 基于 MAD 的鲁棒异常检测 |
| 动态阈值 | 基于百分位数的自适应阈值 |
| 告警去重 | 5 分钟窗口抑制相同指标+规则的重复告警 |

## API 接口

### 数据摄入

```bash
# 单点摄入
curl -X POST http://localhost:8080/api/data/ingest \
  -H "Content-Type: application/json" \
  -d '{"metric":"cpu.usage","value":72.5,"timestamp":1695000000}'

# 批量摄入
curl -X POST http://localhost:8080/api/data/ingest/batch \
  -H "Content-Type: application/json" \
  -d '{"points":[{"metric":"cpu.usage","value":72.5},{"metric":"mem","value":4.2}]}'
```

### 数据查询

```bash
# 原始查询
curl "http://localhost:8080/api/data/query?metric=cpu.usage&start=1695000000&end=1695003600"

# 降采样查询
curl "http://localhost:8080/api/data/downsample?metric=cpu.usage&start=1695000000&end=1695003600&target=200&method=lttb"
```

### 规则管理

```bash
# 创建规则
curl -X POST http://localhost:8080/api/rules \
  -H "Content-Type: application/json" \
  -d '{"name":"CPU异常","metric":"cpu.usage","algorithm":"zscore","threshold":3.0}'

# 获取规则
curl http://localhost:8080/api/rules

# 删除规则
curl -X DELETE http://localhost:8080/api/rules/rule_id
```

### 告警管理

```bash
# 获取告警
curl http://localhost:8080/api/alerts
curl "http://localhost:8080/api/alerts?status=active&severity=critical"

# 确认告警
curl -X POST http://localhost:8080/api/alerts/acknowledge \
  -d '{"alert_id":"alert_xxx"}'

# 解决告警
curl -X POST http://localhost:8080/api/alerts/resolve \
  -d '{"alert_id":"alert_xxx"}'
```

### 模拟器

```bash
# 启动数据模拟
curl -X POST http://localhost:8080/api/simulate \
  -d '{"metrics":["cpu.usage","memory.usage"],"duration":300,"interval":1}'
```

## 存储设计

### 时序数据分片

```
data/timeseries/
├── cpu_usage_20260923_04.json
├── cpu_usage_20260923_05.json
├── memory_usage_20260923_04.json
└── ...
```

每个分片文件包含该小时内的所有数据点，按时间排序，自动去重。

### 数据点格式

```json
{
  "t": 1695000000.123,  // 时间戳（秒，保留3位小数）
  "v": 72.5,            // 值
  "tags": {"host": "s1"}, // 标签
  "src": "api"           // 来源
}
```

## 异常检测算法

### Z-Score
- 适用场景：正态分布数据的突变检测
- 参数：`window_size`（窗口大小）、`threshold`（阈值，通常 2-4）
- 原理：值偏离均值超过 N 个标准差即为异常

### EWMA（指数加权移动平均）
- 适用场景：检测渐变漂移
- 参数：`alpha`（衰减因子，0-1，越大越敏感）
- 原理：对近期数据赋予更高权重，偏差超过阈值即为异常

### 移动中位数
- 适用场景：含离群值的鲁棒检测
- 参数：`window_size`、`threshold`
- 原理：使用中位数和 MAD（中位绝对偏差）代替均值和标准差

## 性能优化

| 挑战 | 解决方案 |
|------|----------|
| 高吞吐写入 | 写入缓冲区，批量刷盘（1000点或2秒） |
| JSON 分片性能 | 原子写入（tmp + rename），限制单分片大小 |
| 窗口状态内存 | deque 固定大小，增量统计量更新 |
| 跨分片合并 | 按需加载分片，内存缓存最近 2 小时数据 |
| 告警风暴 | 5 分钟窗口去重，相同指标+规则的告警自动抑制 |

## 依赖

- Python 3.8+（无第三方依赖）
- 现代浏览器（Chrome/Firefox/Safari/Edge）
- ECharts 5.5.0（CDN 加载）