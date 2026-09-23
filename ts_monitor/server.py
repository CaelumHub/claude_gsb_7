"""
Time-Series Monitoring Server
- HTTP API for data ingestion, querying, and management
- Real-time data simulation
- Anomaly detection pipeline
"""

import json
import time
import threading
import random
import math
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from storage import TimeSeriesStorage
from anomaly import AnomalyDetector
from downsample import downsample_simple


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Multi-threaded HTTP server."""
    daemon_threads = True
    allow_reuse_address = True


class TimeSeriesHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the time-series API."""

    storage: TimeSeriesStorage = None
    detector: AnomalyDetector = None

    def log_message(self, format, *args):
        """Suppress default logging for cleaner output."""
        pass

    def _send_json(self, data: Any, status: int = 200):
        """Send JSON response."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode())

    def _send_error(self, message: str, status: int = 400):
        """Send error response."""
        self._send_json({"error": message}, status)

    def _read_body(self) -> Dict:
        """Read and parse JSON request body."""
        content_length = int(self.headers.get('Content-Length', 0))
        if content_length == 0:
            return {}
        body = self.rfile.read(content_length)
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}

    def _parse_query(self) -> Dict:
        """Parse URL query parameters."""
        parsed = urlparse(self.path)
        return parse_qs(parsed.query)

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        """Handle GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path
        # Normalize: strip trailing slash but keep root "/"
        if path != '/' and path.endswith('/'):
            path = path.rstrip('/')
        query = self._parse_query()

        try:
            if path == '/api/status':
                self._handle_status()
            elif path == '/api/data/query':
                self._handle_data_query(query)
            elif path == '/api/data/downsample':
                self._handle_downsample(query)
            elif path == '/api/data/metrics':
                self._handle_metrics()
            elif path == '/api/dashboard':
                self._handle_dashboard(query)
            elif path == '/api/alerts':
                self._handle_get_alerts(query)
            elif path == '/api/rules':
                self._handle_get_rules()
            elif path == '/api/sources':
                self._handle_get_sources()
            elif path == '/api/shards':
                self._handle_shard_info()
            elif path == '/api/detector/state':
                self._handle_detector_state()
            elif path == '/':
                self._serve_frontend()
            else:
                self._send_error("Not found", 404)
        except Exception as e:
            self._send_error(f"Internal error: {str(e)}", 500)

    def do_POST(self):
        """Handle POST requests."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')

        try:
            if path == '/api/data/ingest':
                self._handle_ingest()
            elif path == '/api/data/ingest/batch':
                self._handle_batch_ingest()
            elif path == '/api/alerts/acknowledge':
                self._handle_acknowledge_alert()
            elif path == '/api/alerts/resolve':
                self._handle_resolve_alert()
            elif path == '/api/rules':
                self._handle_add_rule()
            elif path == '/api/sources':
                self._handle_add_source()
            elif path == '/api/simulate':
                self._handle_simulate()
            else:
                self._send_error("Not found", 404)
        except Exception as e:
            self._send_error(f"Internal error: {str(e)}", 500)

    def do_DELETE(self):
        """Handle DELETE requests."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')

        try:
            if path.startswith('/api/rules/'):
                rule_id = path.split('/')[-1]
                self._handle_delete_rule(rule_id)
            elif path.startswith('/api/sources/'):
                source_id = path.split('/')[-1]
                self._handle_delete_source(source_id)
            else:
                self._send_error("Not found", 404)
        except Exception as e:
            self._send_error(f"Internal error: {str(e)}", 500)

    # ---- API Handlers ----

    def _handle_status(self):
        """Server status endpoint."""
        stats = self.storage.get_stats()
        self._send_json({
            "status": "running",
            "version": "1.0.0",
            "uptime": time.time() - SERVER_START_TIME,
            "storage": stats,
            "timestamp": time.time()
        })

    def _handle_ingest(self):
        """Ingest a single data point."""
        body = self._read_body()
        metric = body.get("metric")
        value = body.get("value")
        timestamp = body.get("timestamp", time.time())
        tags = body.get("tags", {})
        source = body.get("source", "default")

        if not metric or value is None:
            self._send_error("Missing 'metric' or 'value'")
            return

        # Store the data point
        self.storage.write(metric, float(timestamp), float(value), tags, source)

        # Run anomaly detection
        anomaly_result = None
        for rule in self.storage.get_rules():
            if rule.get("metric") == metric and rule.get("enabled", True):
                is_anomaly, result = self.detector.detect(metric, float(value), rule)
                if is_anomaly:
                    alert = {
                        "metric": metric,
                        "value": float(value),
                        "rule_id": rule.get("id"),
                        "rule_name": rule.get("name", "Unknown"),
                        "algorithm": rule.get("algorithm"),
                        "severity": rule.get("severity", "warning"),
                        "score": result.get("score"),
                        "details": result.get("details"),
                        "timestamp": float(timestamp),
                        "source": source,
                        "tags": tags
                    }
                    saved_alert = self.storage.add_alert(alert)
                    anomaly_result = saved_alert

        self._send_json({
            "success": True,
            "metric": metric,
            "timestamp": float(timestamp),
            "anomaly_detected": anomaly_result is not None,
            "alert": anomaly_result
        })

    def _handle_batch_ingest(self):
        """Ingest multiple data points."""
        body = self._read_body()
        points = body.get("points", [])

        if not points:
            self._send_error("Missing 'points' array")
            return

        self.storage.write_batch(points)

        # Run anomaly detection on each point
        anomalies = []
        for p in points:
            metric = p.get("metric", "")
            value = p.get("value", 0)
            for rule in self.storage.get_rules():
                if rule.get("metric") == metric and rule.get("enabled", True):
                    is_anomaly, result = self.detector.detect(metric, float(value), rule)
                    if is_anomaly:
                        alert = {
                            "metric": metric,
                            "value": float(value),
                            "rule_id": rule.get("id"),
                            "rule_name": rule.get("name", "Unknown"),
                            "algorithm": rule.get("algorithm"),
                            "severity": rule.get("severity", "warning"),
                            "score": result.get("score"),
                            "details": result.get("details"),
                            "timestamp": p.get("timestamp", time.time()),
                            "source": p.get("source", "default")
                        }
                        saved = self.storage.add_alert(alert)
                        anomalies.append(saved)

        self._send_json({
            "success": True,
            "ingested": len(points),
            "anomalies_detected": len(anomalies),
            "alerts": anomalies
        })

    def _handle_data_query(self, query: Dict):
        """Query time-series data."""
        metric = query.get("metric", [None])[0]
        start = float(query.get("start", [time.time() - 3600])[0])
        end = float(query.get("end", [time.time()])[0])
        max_points = int(query.get("max_points", [1000])[0])

        if not metric:
            self._send_error("Missing 'metric' parameter")
            return

        data = self.storage.query(metric, start, end, max_points=max_points)
        self._send_json({
            "metric": metric,
            "start": start,
            "end": end,
            "count": len(data),
            "data": data
        })

    def _handle_downsample(self, query: Dict):
        """Query with downsampling."""
        metric = query.get("metric", [None])[0]
        start = float(query.get("start", [time.time() - 3600])[0])
        end = float(query.get("end", [time.time()])[0])
        target = int(query.get("target", [200])[0])
        method = query.get("method", ["lttb"])[0]

        if not metric:
            self._send_error("Missing 'metric' parameter")
            return

        data = self.storage.query(metric, start, end, max_points=50000)
        downsampled = downsample_simple(data, target, method)

        self._send_json({
            "metric": metric,
            "original_count": len(data),
            "downsampled_count": len(downsampled),
            "method": method,
            "data": downsampled
        })

    def _handle_metrics(self):
        """Get available metrics."""
        metrics = self.storage.get_metrics()
        self._send_json({"metrics": metrics})

    def _handle_dashboard(self, query: Dict):
        """Get dashboard summary data."""
        now = time.time()
        period = int(query.get("period", [300])[0])  # Last 5 minutes default

        metrics = self.storage.get_metrics()
        dashboard_data = {}

        for metric in metrics[:20]:  # Limit to 20 metrics
            data = self.storage.query(metric, now - period, now, max_points=200)
            if data:
                values = [p["v"] for p in data]
                dashboard_data[metric] = {
                    "current": round(values[-1], 4) if values else 0,
                    "min": round(min(values), 4),
                    "max": round(max(values), 4),
                    "avg": round(sum(values) / len(values), 4),
                    "count": len(values),
                    "data": data[-50:]  # Last 50 points for sparkline
                }

        # Recent alerts
        recent_alerts = self.storage.get_alerts(limit=10)

        self._send_json({
            "timestamp": now,
            "period": period,
            "metrics": dashboard_data,
            "recent_alerts": recent_alerts,
            "stats": self.storage.get_stats()
        })

    def _handle_get_alerts(self, query: Dict):
        """Get alerts list."""
        status = query.get("status", [None])[0]
        severity = query.get("severity", [None])[0]
        limit = int(query.get("limit", [200])[0])

        alerts = self.storage.get_alerts(status=status, severity=severity, limit=limit)
        self._send_json({"alerts": alerts, "count": len(alerts)})

    def _handle_acknowledge_alert(self):
        """Acknowledge an alert."""
        body = self._read_body()
        alert_id = body.get("alert_id")
        if not alert_id:
            self._send_error("Missing 'alert_id'")
            return

        success = self.storage.acknowledge_alert(alert_id)
        self._send_json({"success": success})

    def _handle_resolve_alert(self):
        """Resolve an alert."""
        body = self._read_body()
        alert_id = body.get("alert_id")
        if not alert_id:
            self._send_error("Missing 'alert_id'")
            return

        success = self.storage.resolve_alert(alert_id)
        self._send_json({"success": success})

    def _handle_get_rules(self):
        """Get all rules."""
        rules = self.storage.get_rules()
        self._send_json({"rules": rules, "count": len(rules)})

    def _handle_add_rule(self):
        """Add or update a rule."""
        body = self._read_body()
        if not body.get("metric") or not body.get("algorithm"):
            self._send_error("Missing 'metric' or 'algorithm'")
            return

        rule = self.storage.add_rule(body)
        self._send_json({"success": True, "rule": rule})

    def _handle_delete_rule(self, rule_id: str):
        """Delete a rule."""
        success = self.storage.delete_rule(rule_id)
        self._send_json({"success": success})

    def _handle_get_sources(self):
        """Get all data sources."""
        sources = self.storage.get_sources()
        self._send_json({"sources": sources, "count": len(sources)})

    def _handle_add_source(self):
        """Add or update a data source."""
        body = self._read_body()
        source_id = body.get("id") or f"src_{int(time.time()*1000)}"
        source = self.storage.add_source(source_id, body)
        self._send_json({"success": True, "source": source})

    def _handle_delete_source(self, source_id: str):
        """Delete a data source."""
        success = self.storage.delete_source(source_id)
        self._send_json({"success": success})

    def _handle_shard_info(self):
        """Get shard file information."""
        info = self.storage.get_shard_info()
        self._send_json({"shards": info, "count": len(info)})

    def _handle_detector_state(self):
        """Get anomaly detector state."""
        state = self.detector.get_state_info()
        self._send_json({"state": state})

    def _handle_simulate(self):
        """Trigger data simulation."""
        body = self._read_body()
        duration = body.get("duration", 60)
        interval = body.get("interval", 1)
        metrics = body.get("metrics", ["cpu.usage", "memory.usage", "disk.io", "network.throughput"])

        # Start simulation in background
        sim_thread = threading.Thread(
            target=_run_simulation,
            args=(self.storage, self.detector, metrics, duration, interval),
            daemon=True
        )
        sim_thread.start()

        self._send_json({
            "success": True,
            "message": f"Simulation started for {duration}s",
            "metrics": metrics
        })

    def _serve_frontend(self):
        """Serve the frontend HTML."""
        # Try multiple paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(script_dir, "..", "ts_dashboard.html"),
            os.path.join(script_dir, "ts_dashboard.html"),
            os.path.join(os.getcwd(), "ts_dashboard.html"),
            os.path.join(os.getcwd(), "..", "ts_dashboard.html"),
        ]
        html_path = None
        for p in candidates:
            if os.path.exists(p):
                html_path = p
                break
        if html_path:
            with open(html_path, 'r') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(content.encode())
        else:
            self._send_error("Frontend not found", 404)


# ---- Data Simulator ----

class DataSimulator:
    """Generates realistic time-series data with anomalies."""

    def __init__(self):
        self.trends = {}
        self.seasonal = {}
        self.anomaly_injection = {}

    def generate(self, metric: str, timestamp: float) -> float:
        """Generate a data point for a metric."""
        if metric not in self.trends:
            self.trends[metric] = {
                "base": random.uniform(20, 80),
                "trend": random.uniform(-0.1, 0.1),
                "season_period": random.choice([60, 300, 600, 1800]),
                "season_amplitude": random.uniform(5, 20),
                "noise_std": random.uniform(1, 5),
                "last_value": None
            }

        state = self.trends[metric]
        t = timestamp

        # Base value with trend
        base = state["base"] + state["trend"] * t

        # Seasonal component
        seasonal = state["season_amplitude"] * math.sin(2 * math.pi * t / state["season_period"])

        # Random walk component
        if state["last_value"] is not None:
            walk = random.gauss(0, state["noise_std"] * 0.3)
            value = state["last_value"] * 0.7 + (base + seasonal) * 0.3 + walk
        else:
            value = base + seasonal + random.gauss(0, state["noise_std"])

        # Inject anomalies occasionally (2% chance)
        if random.random() < 0.02:
            anomaly_type = random.choice(["spike", "dip", "shift"])
            if anomaly_type == "spike":
                value += random.uniform(20, 50)
            elif anomaly_type == "dip":
                value -= random.uniform(20, 50)
            else:  # shift
                value += random.uniform(-30, 30)

        # Clamp to reasonable range
        value = max(0, min(100, value))
        state["last_value"] = value

        return round(value, 4)


def _run_simulation(storage: TimeSeriesStorage, detector: AnomalyDetector,
                    metrics: list, duration: int, interval: float):
    """Run data simulation in background."""
    sim = DataSimulator()
    start = time.time()
    count = 0

    while time.time() - start < duration:
        ts = time.time()
        points = []

        for metric in metrics:
            value = sim.generate(metric, ts)
            points.append({
                "metric": metric,
                "timestamp": ts,
                "value": value,
                "source": "simulator",
                "tags": {"env": "demo"}
            })

        storage.write_batch(points)

        # Run anomaly detection
        for p in points:
            for rule in storage.get_rules():
                if rule.get("metric") == p["metric"] and rule.get("enabled", True):
                    is_anomaly, result = detector.detect(p["metric"], p["value"], rule)
                    if is_anomaly:
                        alert = {
                            "metric": p["metric"],
                            "value": p["value"],
                            "rule_id": rule.get("id"),
                            "rule_name": rule.get("name", "Unknown"),
                            "algorithm": rule.get("algorithm"),
                            "severity": rule.get("severity", "warning"),
                            "score": result.get("score"),
                            "details": result.get("details"),
                            "timestamp": ts,
                            "source": "simulator"
                        }
                        storage.add_alert(alert)

        count += len(points)
        time.sleep(interval)

    storage.force_flush()
    print(f"Simulation complete: {count} data points generated")


# ---- Server Startup ----

SERVER_START_TIME = time.time()


def create_default_rules(storage: TimeSeriesStorage):
    """Create default anomaly detection rules."""
    default_rules = [
        {
            "id": "rule_cpu_zscore",
            "name": "CPU Usage Z-Score",
            "metric": "cpu.usage",
            "algorithm": "zscore",
            "threshold": 3.0,
            "severity": "warning",
            "enabled": True,
            "dynamic_threshold": True,
            "params": {"window_size": 200},
            "description": "Detects CPU spikes using Z-score"
        },
        {
            "id": "rule_memory_ewma",
            "name": "Memory EWMA Alert",
            "metric": "memory.usage",
            "algorithm": "ewma",
            "threshold": 3.5,
            "severity": "warning",
            "enabled": True,
            "dynamic_threshold": False,
            "params": {"alpha": 0.3},
            "description": "Detects memory drift using EWMA"
        },
        {
            "id": "rule_disk_median",
            "name": "Disk I/O Moving Median",
            "metric": "disk.io",
            "algorithm": "moving_median",
            "threshold": 4.0,
            "severity": "critical",
            "enabled": True,
            "dynamic_threshold": True,
            "params": {"window_size": 100},
            "description": "Detects disk I/O anomalies using moving median"
        },
        {
            "id": "rule_network_zscore",
            "name": "Network Throughput Z-Score",
            "metric": "network.throughput",
            "algorithm": "zscore",
            "threshold": 2.5,
            "severity": "warning",
            "enabled": True,
            "dynamic_threshold": False,
            "params": {"window_size": 150},
            "description": "Detects network anomalies"
        }
    ]

    existing = {r["id"] for r in storage.get_rules()}
    for rule in default_rules:
        if rule["id"] not in existing:
            storage.add_rule(rule)


def create_default_sources(storage: TimeSeriesStorage):
    """Create default data sources."""
    default_sources = {
        "simulator": {
            "name": "Local Simulator",
            "type": "simulator",
            "enabled": True,
            "metrics": ["cpu.usage", "memory.usage", "disk.io", "network.throughput"],
            "interval": 1,
            "description": "Built-in data simulator"
        },
        "api": {
            "name": "API Ingestion",
            "type": "api",
            "enabled": True,
            "endpoint": "/api/data/ingest",
            "description": "HTTP API data ingestion endpoint"
        }
    }

    for sid, config in default_sources.items():
        if sid not in storage.get_sources():
            storage.add_source(sid, config)


def run_server(host: str = "0.0.0.0", port: int = 8080, data_dir: str = "./data"):
    """Start the time-series monitoring server."""
    import os

    # Initialize components
    storage = TimeSeriesStorage(data_dir)
    detector = AnomalyDetector()

    # Set class-level attributes
    TimeSeriesHandler.storage = storage
    TimeSeriesHandler.detector = detector

    # Create defaults
    create_default_rules(storage)
    create_default_sources(storage)

    # Create server
    server = ThreadedHTTPServer((host, port), TimeSeriesHandler)

    print(f"╔══════════════════════════════════════════════════════╗")
    print(f"║   Time-Series Monitoring Server v1.0.0              ║")
    print(f"╠══════════════════════════════════════════════════════╣")
    print(f"║   API:     http://{host}:{port}/api                ║")
    print(f"║   Status:  http://{host}:{port}/api/status         ║")
    print(f"║   Data:    {data_dir:<40s} ║")
    print(f"╚══════════════════════════════════════════════════════╝")

    # Start auto-simulation
    def auto_simulate():
        time.sleep(2)  # Wait for server to start
        sim = DataSimulator()
        metrics = ["cpu.usage", "memory.usage", "disk.io", "network.throughput"]

        while True:
            ts = time.time()
            points = []
            for metric in metrics:
                value = sim.generate(metric, ts)
                points.append({
                    "metric": metric,
                    "timestamp": ts,
                    "value": value,
                    "source": "auto-simulator",
                    "tags": {"env": "production"}
                })

            storage.write_batch(points)

            # Anomaly detection
            for p in points:
                for rule in storage.get_rules():
                    if rule.get("metric") == p["metric"] and rule.get("enabled", True):
                        is_anomaly, result = detector.detect(p["metric"], p["value"], rule)
                        if is_anomaly:
                            alert = {
                                "metric": p["metric"],
                                "value": p["value"],
                                "rule_id": rule.get("id"),
                                "rule_name": rule.get("name", "Unknown"),
                                "algorithm": rule.get("algorithm"),
                                "severity": rule.get("severity", "warning"),
                                "score": result.get("score"),
                                "details": result.get("details"),
                                "timestamp": ts,
                                "source": "auto-simulator"
                            }
                            storage.add_alert(alert)

            time.sleep(1)

    sim_thread = threading.Thread(target=auto_simulate, daemon=True)
    sim_thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        storage.force_flush()
        server.shutdown()


if __name__ == "__main__":
    import sys
    import os

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    data_dir = sys.argv[2] if len(sys.argv) > 2 else "./data"
    run_server(port=port, data_dir=data_dir)