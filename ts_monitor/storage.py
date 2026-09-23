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

_SHARD_RE = re.compile(r"^(?P<metric>.+)_(?P<date>\d{8})_(?P<hour>\d{2})\.json$")


def _safe_metric_name(metric: str) -> str:
    """Convert a metric name to its shard-file-safe form."""
    return metric.replace("/", "_").replace(".", "_").replace(" ", "_")


def _parse_shard_filename(fname: str) -> Optional[Tuple[str, float]]:
    """Parse a shard filename into (safe_metric, hour_start_timestamp).

    Returns None for files that don't match the shard naming convention.
    """
    m = _SHARD_RE.match(fname)
    if not m:
        return None
    try:
        dt = datetime.strptime(f"{m.group('date')}{m.group('hour')}", "%Y%m%d%H")
        dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return m.group("metric"), dt.timestamp()


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

        # Serializes shard deletions against buffer flushes
        self._cleanup_lock = threading.Lock()

        # Load metadata and rules
        self.metadata = self._load_json(self.meta_file, {"sources": {}, "stats": {}})
        self.rules = self._load_json(self.rules_file, {"rules": []})
        self.alerts = self._load_json(self.alerts_file, {"alerts": [], "suppressed": {}})

        # Retention / auto-cleanup policy (persisted in metadata.json)
        self.retention = self.metadata.get("retention", {
            "enabled": False,
            "retention_days": 7,
            "auto_cleanup": False,
        })
        self.last_cleanup = self.metadata.get("last_cleanup")

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
        safe_metric = _safe_metric_name(metric)
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

    # ---- Storage usage & retention ----

    def _scan_shards(self) -> List[Tuple[str, str, float, int]]:
        """Scan the timeseries directory and return
        (filename, safe_metric, hour_start_ts, size_bytes) tuples."""
        shards = []
        if not os.path.exists(self.ts_dir):
            return shards
        for fname in os.listdir(self.ts_dir):
            parsed = _parse_shard_filename(fname)
            if parsed is None:
                continue
            safe_metric, hour_ts = parsed
            try:
                size = os.path.getsize(os.path.join(self.ts_dir, fname))
            except OSError:
                continue
            shards.append((fname, safe_metric, hour_ts, size))
        return shards

    def _resolve_metric_names(self, safe_names: set) -> Dict[str, str]:
        """Map safe metric names back to the original metric names when known."""
        candidates = set()
        with self._cache_lock:
            candidates.update(self._cache.keys())
        try:
            candidates.update(self.get_metrics())
        except OSError:
            pass
        mapping: Dict[str, str] = {}
        for name in candidates:
            mapping[_safe_metric_name(name)] = name
        return {safe: mapping.get(safe, safe) for safe in safe_names}

    def get_metric_storage(self) -> Dict[str, Any]:
        """Get per-metric shard storage usage."""
        shards = self._scan_shards()
        name_map = self._resolve_metric_names({s[1] for s in shards})

        agg: Dict[str, Dict[str, Any]] = {}
        total_size = 0
        for fname, safe_metric, hour_ts, size in shards:
            total_size += size
            metric = name_map.get(safe_metric, safe_metric)
            entry = agg.setdefault(metric, {
                "metric": metric,
                "shard_count": 0,
                "size_bytes": 0,
                "oldest_shard_ts": hour_ts,
                "latest_shard_ts": hour_ts,
            })
            entry["shard_count"] += 1
            entry["size_bytes"] += size
            entry["oldest_shard_ts"] = min(entry["oldest_shard_ts"], hour_ts)
            # Shard covers data within that hour; latest data may be up to hour end
            entry["latest_shard_ts"] = max(entry["latest_shard_ts"], hour_ts + 3600)

        now = time.time()
        cutoff = self._retention_cutoff(now)
        metrics = []
        for entry in agg.values():
            size = entry["size_bytes"]
            metrics.append({
                **entry,
                "size_kb": round(size / 1024, 2),
                "size_mb": round(size / (1024 * 1024), 2),
                "percent": round(size * 100.0 / total_size, 1) if total_size else 0.0,
                "expired_shards": sum(
                    1 for _, sm, hour_ts, _ in shards
                    if name_map.get(sm, sm) == entry["metric"] and cutoff is not None
                    and hour_ts + 3600 <= cutoff
                ),
            })
        metrics.sort(key=lambda x: x["size_bytes"], reverse=True)

        return {
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "total_shards": len(shards),
            "metric_count": len(metrics),
            "metrics": metrics,
        }

    def _retention_cutoff(self, now: Optional[float] = None) -> Optional[float]:
        """Timestamp before which data is considered expired."""
        days = self.retention.get("retention_days")
        if not days:
            return None
        return (now or time.time()) - float(days) * 86400

    def get_retention_policy(self) -> Dict[str, Any]:
        """Get the configured retention / auto-cleanup policy."""
        return {
            "enabled": bool(self.retention.get("enabled", False)),
            "retention_days": int(self.retention.get("retention_days", 7)),
            "auto_cleanup": bool(self.retention.get("auto_cleanup", False)),
            "cutoff_ts": self._retention_cutoff(),
            "last_cleanup": self.last_cleanup,
        }

    def set_retention_policy(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        """Persist the retention / auto-cleanup policy."""
        days = int(policy.get("retention_days", self.retention.get("retention_days", 7)))
        if days < 1 or days > 3650:
            raise ValueError("retention_days must be between 1 and 3650")
        self.retention = {
            "enabled": bool(policy.get("enabled", self.retention.get("enabled", False))),
            "retention_days": days,
            "auto_cleanup": bool(policy.get("auto_cleanup",
                                           self.retention.get("auto_cleanup", False))),
        }
        self.metadata["retention"] = self.retention
        self._save_json(self.meta_file, self.metadata)
        return self.get_retention_policy()

    def cleanup_expired(self, retention_days: Optional[int] = None,
                        dry_run: bool = False) -> Dict[str, Any]:
        """Delete hourly shard files that are entirely older than the retention window.

        A shard is only deleted when its full hour (hour_start + 3600) is at or
        before the cutoff, so shards overlapping the retention window are kept.
        Current (unflushed) buffered data and the in-memory cache are untouched.
        """
        days = int(retention_days) if retention_days is not None \
            else int(self.retention.get("retention_days", 7))
        if days < 1:
            raise ValueError("retention_days must be >= 1")

        now = time.time()
        cutoff = now - days * 86400

        # Hold the buffer lock for the whole operation: no flush can rewrite a
        # shard while we are deleting it.
        with self._cleanup_lock, self._buffer_lock:
            if not dry_run:
                self._flush_buffer()

            deleted_files = []
            deleted_bytes = 0
            kept_files = []
            tmp_removed = 0

            for fname in os.listdir(self.ts_dir):
                fpath = os.path.join(self.ts_dir, fname)

                # Remove stale temp files left behind by interrupted writes
                if fname.endswith('.tmp'):
                    if not dry_run:
                        try:
                            os.remove(fpath)
                        except OSError:
                            pass
                    tmp_removed += 1
                    continue

                parsed = _parse_shard_filename(fname)
                if parsed is None:
                    continue

                _safe_metric, hour_ts = parsed
                try:
                    size = os.path.getsize(fpath)
                except OSError:
                    continue

                # Expire only fully-closed hours beyond the cutoff
                if hour_ts + 3600 <= cutoff:
                    if not dry_run:
                        try:
                            os.remove(fpath)
                        except OSError:
                            continue
                    deleted_files.append(fname)
                    deleted_bytes += size
                else:
                    kept_files.append(fname)

        # Drop expired points from the in-memory cache as well (cache is a
        # recent-data hot cache; deleting old entries cannot affect new data).
        with self._cache_lock:
            if not dry_run:
                for metric in list(self._cache.keys()):
                    self._cache[metric] = [p for p in self._cache[metric]
                                           if p["t"] >= cutoff]

        result = {
            "dry_run": dry_run,
            "retention_days": days,
            "cutoff_ts": cutoff,
            "cutoff_time": datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat(),
            "executed_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "deleted_shards": len(deleted_files),
            "deleted_bytes": deleted_bytes,
            "deleted_mb": round(deleted_bytes / (1024 * 1024), 2),
            "kept_shards": len(kept_files),
            "tmp_files_removed": tmp_removed,
            "files": sorted(deleted_files),
        }

        if not dry_run:
            self.last_cleanup = result
            self.metadata["last_cleanup"] = result
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