"""
Time-Series Storage Engine
- Hourly JSON shard files for time-series data
- Separate metadata and rules storage
- Cross-shard query with efficient merging
- Write-ahead buffer for high-throughput ingestion
"""

import json
import os
import re
import time
import threading
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import List, Dict, Any, Optional, Tuple

class TimeSeriesStorage:
    """Manages time-series data with hourly JSON shard files."""

    def __init__(self, data_dir: str = "./data"):
        self.data_dir = data_dir
        self.ts_dir = os.path.join(data_dir, "timeseries")
        self.meta_file = os.path.join(data_dir, "metadata.json")
        self.rules_file = os.path.join(data_dir, "rules.json")
        self.alerts_file = os.path.join(data_dir, "alerts.json")

        os.makedirs(self.ts_dir, exist_ok=True)

        # Write buffer for high-throughput ingestion
        self._write_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self._buffer_lock = threading.Lock()
        self._buffer_flush_interval = 2.0  # seconds
        self._last_flush = time.time()

        # In-memory cache for recent data (last 2 hours)
        self._cache: Dict[str, List[Dict]] = defaultdict(list)
        self._cache_lock = threading.Lock()
        self._max_cache_points = 50000

        # Load metadata and rules
        self.metadata = self._load_json(self.meta_file, {"sources": {}, "stats": {}})
        self.rules = self._load_json(self.rules_file, {"rules": []})
        self.alerts = self._load_json(self.alerts_file, {"alerts": [], "suppressed": {}})

    def _load_json(self, path: str, default: Any) -> Any:
        """Load JSON file with fallback to default."""
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
        return default

    def _save_json(self, path: str, data: Any):
        """Atomically save JSON file."""
        tmp_path = path + ".tmp"
        try:
            with open(tmp_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp_path, path)
        except IOError as e:
            print(f"Error saving {path}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def _get_shard_path(self, metric: str, timestamp: float) -> str:
        """Get the hourly shard file path for a metric and timestamp."""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        shard_key = dt.strftime("%Y%m%d_%H")
        safe_metric = metric.replace("/", "_").replace(".", "_").replace(" ", "_")
        return os.path.join(self.ts_dir, f"{safe_metric}_{shard_key}.json")

    def _get_shard_key(self, metric: str, timestamp: float) -> str:
        """Get the shard key for caching."""
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return f"{metric}_{dt.strftime('%Y%m%d_%H')}"

    def write(self, metric: str, timestamp: float, value: float,
              tags: Optional[Dict[str, str]] = None, source: str = "default"):
        """Write a single data point to the write buffer."""
        point = {
            "t": round(timestamp, 3),
            "v": value,
            "tags": tags or {},
            "src": source
        }

        with self._buffer_lock:
            self._write_buffer[metric].append(point)
            # Auto-flush if buffer is large enough
            if len(self._write_buffer[metric]) >= 1000 or \
               (time.time() - self._last_flush) > self._buffer_flush_interval:
                self._flush_buffer()

        # Update cache
        with self._cache_lock:
            self._cache[metric].append(point)
            # Trim cache if too large
            if len(self._cache[metric]) > self._max_cache_points:
                self._cache[metric] = self._cache[metric][-self._max_cache_points:]

    def write_batch(self, points: List[Dict[str, Any]]):
        """Write multiple data points efficiently."""
        with self._buffer_lock:
            for p in points:
                metric = p.get("metric", "unknown")
                point = {
                    "t": round(p.get("timestamp", time.time()), 3),
                    "v": p.get("value", 0),
                    "tags": p.get("tags", {}),
                    "src": p.get("source", "default")
                }
                self._write_buffer[metric].append(point)

                with self._cache_lock:
                    self._cache[metric].append(point)

            if any(len(v) >= 500 for v in self._write_buffer.values()):
                self._flush_buffer()

    def _flush_buffer(self):
        """Flush write buffer to shard files."""
        if not self._write_buffer:
            return

        shards_to_write: Dict[str, List[Dict]] = defaultdict(list)

        for metric, points in self._write_buffer.items():
            for point in points:
                shard_path = self._get_shard_path(metric, point["t"])
                shards_to_write[shard_path].append(point)

        for shard_path, points in shards_to_write.items():
            existing = []
            if os.path.exists(shard_path):
                try:
                    with open(shard_path, 'r') as f:
                        existing = json.load(f)
                except (json.JSONDecodeError, IOError):
                    existing = []

            existing.extend(points)
            # Sort by timestamp and deduplicate
            existing.sort(key=lambda x: x["t"])
            # Remove exact duplicates
            seen = set()
            unique = []
            for p in existing:
                key = (p["t"], p["v"])
                if key not in seen:
                    seen.add(key)
                    unique.append(p)
            existing = unique

            # Keep only last 10000 points per shard to prevent unbounded growth
            if len(existing) > 10000:
                existing = existing[-10000:]

            try:
                tmp_path = shard_path + ".tmp"
                with open(tmp_path, 'w') as f:
                    json.dump(existing, f)
                os.replace(tmp_path, shard_path)
            except IOError as e:
                print(f"Error writing shard {shard_path}: {e}")

        self._write_buffer.clear()
        self._last_flush = time.time()

    def force_flush(self):
        """Force flush all buffered data."""
        with self._buffer_lock:
            self._flush_buffer()

    def query(self, metric: str, start: float, end: float,
              tags: Optional[Dict[str, str]] = None,
              max_points: int = 10000) -> List[Dict]:
        """Query time-series data across shards."""
        self.force_flush()

        results = []

        # Determine which hourly shards to read
        start_dt = datetime.fromtimestamp(start, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end, tz=timezone.utc)

        current = start_dt.replace(minute=0, second=0, microsecond=0)
        while current <= end_dt + timedelta(hours=1):
            shard_path = self._get_shard_path(metric, current.timestamp())
            if os.path.exists(shard_path):
                try:
                    with open(shard_path, 'r') as f:
                        points = json.load(f)
                    # Filter by time range
                    filtered = [p for p in points if start <= p["t"] <= end]
                    if tags:
                        filtered = [p for p in filtered
                                   if all(p.get("tags", {}).get(k) == v for k, v in tags.items())]
                    results.extend(filtered)
                except (json.JSONDecodeError, IOError):
                    pass
            current += timedelta(hours=1)

        # Also check cache for very recent data
        with self._cache_lock:
            cache_points = self._cache.get(metric, [])
            cache_filtered = [p for p in cache_points if start <= p["t"] <= end]
            if tags:
                cache_filtered = [p for p in cache_filtered
                                 if all(p.get("tags", {}).get(k) == v for k, v in tags.items())]
            results.extend(cache_filtered)

        # Deduplicate and sort
        seen = set()
        unique = []
        for p in sorted(results, key=lambda x: x["t"]):
            key = (p["t"], p["v"])
            if key not in seen:
                seen.add(key)
                unique.append(p)

        # Downsample if too many points
        if len(unique) > max_points:
            step = len(unique) / max_points
            unique = [unique[int(i * step)] for i in range(max_points)]

        return unique

    def get_metrics(self) -> List[str]:
        """Get list of all available metrics."""
        metrics = set()
        # Scan shard files
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    # Extract metric name (everything before the date part)
                    parts = fname.rsplit('_', 2)
                    if len(parts) >= 3:
                        metrics.add(parts[0])
        # Also include cached metrics
        with self._cache_lock:
            metrics.update(self._cache.keys())
        return sorted(metrics)

    def get_shard_info(self) -> List[Dict]:
        """Get information about shard files."""
        info = []
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    fpath = os.path.join(self.ts_dir, fname)
                    stat = os.stat(fpath)
                    info.append({
                        "file": fname,
                        "size": stat.st_size,
                        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
        return sorted(info, key=lambda x: x["file"])

    # ---- Storage Usage & Retention ----

    # Shard files look like: <safe_metric>_YYYYMMDD_HH.json
    _SHARD_RE = re.compile(r"^(.+)_(\d{8})_(\d{2})\.json$")

    @staticmethod
    def _format_size(num_bytes: int) -> str:
        """Format a byte count as a human-readable string."""
        size = float(num_bytes)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024 or unit == "TB":
                return f"{size:.2f} {unit}" if unit != "B" else f"{int(size)} B"
            size /= 1024
        return f"{size:.2f} TB"

    def get_metric_storage_info(self) -> List[Dict[str, Any]]:
        """Get per-metric on-disk storage usage, aggregated from shard files."""
        agg: Dict[str, Dict[str, Any]] = {}

        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                m = self._SHARD_RE.match(fname)
                if not m:
                    continue
                metric, day_part, hour_part = m.group(1), m.group(2), m.group(3)
                try:
                    shard_dt = datetime.strptime(
                        f"{day_part}{hour_part}", "%Y%m%d%H"
                    ).replace(tzinfo=timezone.utc)
                    shard_hour = shard_dt.timestamp()
                except ValueError:
                    shard_hour = 0

                fpath = os.path.join(self.ts_dir, fname)
                try:
                    fsize = os.path.getsize(fpath)
                except OSError:
                    continue

                entry = agg.setdefault(metric, {
                    "metric": metric,
                    "size_bytes": 0,
                    "shard_count": 0,
                    "oldest_shard": None,
                    "newest_shard": None,
                })
                entry["size_bytes"] += fsize
                entry["shard_count"] += 1
                if entry["oldest_shard"] is None or shard_hour < entry["oldest_shard"]:
                    entry["oldest_shard"] = shard_hour
                if entry["newest_shard"] is None or shard_hour > entry["newest_shard"]:
                    entry["newest_shard"] = shard_hour

        # Include metrics that only live in the write cache / memory
        with self._cache_lock:
            cached_metrics = list(self._cache.keys())
        for metric in cached_metrics:
            if metric not in agg:
                agg[metric] = {
                    "metric": metric,
                    "size_bytes": 0,
                    "shard_count": 0,
                    "oldest_shard": None,
                    "newest_shard": None,
                }

        result = list(agg.values())
        for entry in result:
            entry["size"] = self._format_size(entry["size_bytes"])
            entry["oldest_shard_iso"] = (
                datetime.fromtimestamp(entry["oldest_shard"], tz=timezone.utc).isoformat()
                if entry["oldest_shard"] else None
            )
            entry["newest_shard_iso"] = (
                datetime.fromtimestamp(entry["newest_shard"], tz=timezone.utc).isoformat()
                if entry["newest_shard"] else None
            )
        return sorted(result, key=lambda x: x["size_bytes"], reverse=True)

    def get_retention_policy(self) -> Dict[str, Any]:
        """Get the configured data retention / auto-cleanup policy."""
        policy = self.metadata.get("retention_policy", {})
        return {
            "enabled": policy.get("enabled", False),
            "retention_days": policy.get("retention_days", 7),
            "updated_at": policy.get("updated_at"),
            "last_cleanup": policy.get("last_cleanup"),
        }

    def set_retention_policy(self, enabled: bool, retention_days: int) -> Dict[str, Any]:
        """Persist the data retention / auto-cleanup policy."""
        retention_days = int(retention_days)
        if retention_days < 1 or retention_days > 36500:
            raise ValueError("retention_days must be between 1 and 36500")

        policy = dict(self.metadata.get("retention_policy", {}))
        policy.update({
            "enabled": bool(enabled),
            "retention_days": retention_days,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        self.metadata["retention_policy"] = policy
        self._save_json(self.meta_file, self.metadata)
        return self.get_retention_policy()

    def cleanup_expired(self, retention_days: Optional[int] = None) -> Dict[str, Any]:
        """Delete hourly shard files whose entire hour bucket is older than the
        retention window. The shard covering the current hour is never removed,
        so in-window data (including the data being written right now) is safe.
        """
        # Make sure buffered late data is on disk before deciding what is expired
        self.force_flush()

        if retention_days is None:
            retention_days = self.get_retention_policy()["retention_days"]
        retention_days = int(retention_days)
        if retention_days < 1:
            raise ValueError("retention_days must be >= 1")

        now = time.time()
        cutoff = now - retention_days * 86400
        # Only whole completed hour buckets are eligible for deletion
        cutoff_hour_floor = (int(cutoff) // 3600) * 3600

        deleted_files: List[str] = []
        deleted_bytes = 0
        errors: List[str] = []

        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                m = self._SHARD_RE.match(fname)
                if not m:
                    continue
                day_part, hour_part = m.group(2), m.group(3)
                try:
                    shard_dt = datetime.strptime(
                        f"{day_part}{hour_part}", "%Y%m%d%H"
                    ).replace(tzinfo=timezone.utc)
                    shard_hour = shard_dt.timestamp()
                except ValueError:
                    continue

                # Shard covers [shard_hour, shard_hour + 1h); it is expired
                # only when the whole bucket lies before the cutoff.
                if shard_hour + 3600 > cutoff_hour_floor:
                    continue

                fpath = os.path.join(self.ts_dir, fname)
                try:
                    fsize = os.path.getsize(fpath)
                    os.remove(fpath)
                    deleted_files.append(fname)
                    deleted_bytes += fsize
                except OSError as e:
                    errors.append(f"{fname}: {e}")

        # Drop matching expired points from the in-memory cache as well
        with self._cache_lock:
            for metric in list(self._cache.keys()):
                before = len(self._cache[metric])
                self._cache[metric] = [
                    p for p in self._cache[metric] if p["t"] >= cutoff
                ]
                if len(self._cache[metric]) != before:
                    self._cache[metric] = self._cache[metric][-self._max_cache_points:]

        result = {
            "retention_days": retention_days,
            "cutoff": cutoff,
            "cutoff_iso": datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat(),
            "deleted_shards": len(deleted_files),
            "deleted_bytes": deleted_bytes,
            "freed": self._format_size(deleted_bytes),
            "files": deleted_files,
            "errors": errors,
            "ran_at": now,
            "ran_at_iso": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        }

        policy = dict(self.metadata.get("retention_policy", {}))
        policy["last_cleanup"] = result
        self.metadata["retention_policy"] = policy
        self._save_json(self.meta_file, self.metadata)

        return result

    # ---- Metadata (Sources) ----

    def get_sources(self) -> Dict:
        """Get all configured data sources."""
        return self.metadata.get("sources", {})

    def add_source(self, source_id: str, config: Dict) -> Dict:
        """Add or update a data source."""
        self.metadata["sources"][source_id] = {
            **config,
            "id": source_id,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self._save_json(self.meta_file, self.metadata)
        return self.metadata["sources"][source_id]

    def delete_source(self, source_id: str) -> bool:
        """Delete a data source."""
        if source_id in self.metadata.get("sources", {}):
            del self.metadata["sources"][source_id]
            self._save_json(self.meta_file, self.metadata)
            return True
        return False

    # ---- Rules ----

    def get_rules(self) -> List[Dict]:
        """Get all anomaly detection rules."""
        return self.rules.get("rules", [])

    def add_rule(self, rule: Dict) -> Dict:
        """Add or update an anomaly detection rule."""
        rule_id = rule.get("id", f"rule_{int(time.time()*1000)}")
        rule["id"] = rule_id
        rule["updated_at"] = datetime.now(timezone.utc).isoformat()

        # Update existing or add new
        existing = [r for r in self.rules["rules"] if r["id"] != rule_id]
        existing.append(rule)
        self.rules["rules"] = existing

        self._save_json(self.rules_file, self.rules)
        return rule

    def delete_rule(self, rule_id: str) -> bool:
        """Delete an anomaly detection rule."""
        before = len(self.rules["rules"])
        self.rules["rules"] = [r for r in self.rules["rules"] if r["id"] != rule_id]
        if len(self.rules["rules"]) < before:
            self._save_json(self.rules_file, self.rules)
            return True
        return False

    # ---- Alerts ----

    def get_alerts(self, status: Optional[str] = None,
                   severity: Optional[str] = None,
                   limit: int = 200) -> List[Dict]:
        """Get alerts with optional filtering."""
        alerts = self.alerts.get("alerts", [])
        if status:
            alerts = [a for a in alerts if a.get("status") == status]
        if severity:
            alerts = [a for a in alerts if a.get("severity") == severity]
        return sorted(alerts, key=lambda x: x.get("timestamp", 0), reverse=True)[:limit]

    def add_alert(self, alert: Dict) -> Dict:
        """Add a new alert with deduplication."""
        alert_id = alert.get("id", f"alert_{int(time.time()*1000)}")
        alert["id"] = alert_id
        alert["timestamp"] = alert.get("timestamp", time.time())
        alert["status"] = alert.get("status", "active")

        # Check for duplicate/suppressed alerts
        suppressed = self.alerts.get("suppressed", {})
        metric = alert.get("metric", "")
        rule_id = alert.get("rule_id", "")
        suppress_key = f"{metric}:{rule_id}"

        # Suppress if same metric+rule had an alert in the last 5 minutes
        if suppress_key in suppressed:
            last_alert_time = suppressed[suppress_key]
            if time.time() - last_alert_time < 300:  # 5 min suppression
                alert["status"] = "suppressed"
                return alert

        suppressed[suppress_key] = time.time()
        self.alerts["suppressed"] = suppressed

        self.alerts["alerts"].append(alert)
        # Keep only last 1000 alerts
        if len(self.alerts["alerts"]) > 1000:
            self.alerts["alerts"] = self.alerts["alerts"][-1000:]

        self._save_json(self.alerts_file, self.alerts)
        return alert

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        for alert in self.alerts.get("alerts", []):
            if alert.get("id") == alert_id:
                alert["status"] = "acknowledged"
                alert["acknowledged_at"] = time.time()
                self._save_json(self.alerts_file, self.alerts)
                return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        for alert in self.alerts.get("alerts", []):
            if alert.get("id") == alert_id:
                alert["status"] = "resolved"
                alert["resolved_at"] = time.time()
                self._save_json(self.alerts_file, self.alerts)
                return True
        return False

    def cleanup_suppressed(self):
        """Clean up old suppression entries."""
        suppressed = self.alerts.get("suppressed", {})
        now = time.time()
        self.alerts["suppressed"] = {
            k: v for k, v in suppressed.items()
            if now - v < 600  # Keep 10 minutes of suppression history
        }
        self._save_json(self.alerts_file, self.alerts)

    def get_stats(self) -> Dict:
        """Get storage statistics."""
        total_size = 0
        shard_count = 0
        if os.path.exists(self.ts_dir):
            for fname in os.listdir(self.ts_dir):
                if fname.endswith('.json'):
                    fpath = os.path.join(self.ts_dir, fname)
                    total_size += os.path.getsize(fpath)
                    shard_count += 1

        return {
            "shard_count": shard_count,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "metric_count": len(self.get_metrics()),
            "source_count": len(self.get_sources()),
            "rule_count": len(self.get_rules()),
            "alert_count": len(self.alerts.get("alerts", [])),
            "cache_size": sum(len(v) for v in self._cache.values())
        }