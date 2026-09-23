"""
Anomaly Detection Engine
- Z-score detection
- EWMA (Exponentially Weighted Moving Average) detection
- Moving Median detection
- Dynamic threshold support
- Sliding window state management
"""

import math
import time
from collections import deque
from typing import List, Dict, Any, Optional, Tuple


class SlidingWindow:
    """Memory-efficient sliding window for time-series data."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.data = deque(maxlen=max_size)
        self.sum = 0.0
        self.sum_sq = 0.0
        self.count = 0

    def add(self, value: float):
        """Add a value to the window, evicting oldest if full."""
        if len(self.data) >= self.max_size:
            old = self.data[0]
            self.sum -= old
            self.sum_sq -= old * old
            self.count -= 1

        self.data.append(value)
        self.sum += value
        self.sum_sq += value * value
        self.count += 1

    def mean(self) -> float:
        """Get the current mean."""
        return self.sum / self.count if self.count > 0 else 0.0

    def variance(self) -> float:
        """Get the current variance."""
        if self.count < 2:
            return 0.0
        mean = self.mean()
        return (self.sum_sq / self.count) - (mean * mean)

    def std(self) -> float:
        """Get the current standard deviation."""
        return math.sqrt(max(0, self.variance()))

    def median(self) -> float:
        """Get the current median (O(n) but window is small)."""
        if not self.data:
            return 0.0
        sorted_data = sorted(self.data)
        n = len(sorted_data)
        if n % 2 == 0:
            return (sorted_data[n//2 - 1] + sorted_data[n//2]) / 2
        return sorted_data[n//2]

    def mad(self) -> float:
        """Median Absolute Deviation - robust measure of spread."""
        med = self.median()
        deviations = sorted(abs(x - med) for x in self.data)
        n = len(deviations)
        if n == 0:
            return 0.0
        if n % 2 == 0:
            return (deviations[n//2 - 1] + deviations[n//2]) / 2
        return deviations[n//2]

    def percentile(self, p: float) -> float:
        """Get the p-th percentile (0-100)."""
        if not self.data:
            return 0.0
        sorted_data = sorted(self.data)
        k = (len(sorted_data) - 1) * p / 100.0
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_data[int(k)]
        return sorted_data[f] * (c - k) + sorted_data[c] * (k - f)

    def clear(self):
        """Clear the window."""
        self.data.clear()
        self.sum = 0.0
        self.sum_sq = 0.0
        self.count = 0

    def __len__(self):
        return len(self.data)


class AnomalyDetector:
    """Multi-algorithm anomaly detection engine."""

    def __init__(self):
        # Per-metric sliding windows
        self.windows: Dict[str, SlidingWindow] = {}
        # EWMA state per metric
        self.ewma_state: Dict[str, Dict[str, float]] = {}
        # Dynamic thresholds per metric
        self.dynamic_thresholds: Dict[str, Dict[str, float]] = {}

    def _get_window(self, metric: str, size: int = 500) -> SlidingWindow:
        """Get or create a sliding window for a metric."""
        key = f"{metric}_window"
        if key not in self.windows:
            self.windows[key] = SlidingWindow(max_size=size)
        return self.windows[key]

    def _get_ewma_state(self, metric: str, alpha: float = 0.3) -> Dict[str, float]:
        """Get or initialize EWMA state for a metric."""
        if metric not in self.ewma_state:
            self.ewma_state[metric] = {
                "ewma": 0.0,
                "ewma_var": 0.0,
                "initialized": 0
            }
        return self.ewma_state[metric]

    def detect_zscore(self, metric: str, value: float,
                      window_size: int = 200,
                      threshold: float = 3.0) -> Tuple[bool, float, Dict]:
        """
        Z-score anomaly detection.
        Returns: (is_anomaly, z_score, details)
        """
        window = self._get_window(metric, window_size)
        window.add(value)

        if window.count < 10:  # Need minimum samples
            return False, 0.0, {"reason": "insufficient_data", "count": window.count}

        mean = window.mean()
        std = window.std()

        if std < 1e-10:  # Near-zero std
            return False, 0.0, {"reason": "near_zero_std", "mean": mean}

        z_score = abs(value - mean) / std
        is_anomaly = z_score > threshold

        return is_anomaly, z_score, {
            "mean": round(mean, 4),
            "std": round(std, 4),
            "z_score": round(z_score, 4),
            "threshold": threshold,
            "value": value,
            "window_size": window.count
        }

    def detect_ewma(self, metric: str, value: float,
                    alpha: float = 0.3,
                    threshold_sigma: float = 3.0) -> Tuple[bool, float, Dict]:
        """
        EWMA (Exponentially Weighted Moving Average) anomaly detection.
        Good for detecting gradual shifts in the mean.
        """
        state = self._get_ewma_state(metric, alpha)

        if state["initialized"] == 0:
            state["ewma"] = value
            state["ewma_var"] = 0.0
            state["initialized"] = 1
            return False, 0.0, {"reason": "initializing", "ewma": value}

        # Update EWMA
        prev_ewma = state["ewma"]
        state["ewma"] = alpha * value + (1 - alpha) * prev_ewma

        # Update EWMA variance
        diff = value - prev_ewma
        state["ewma_var"] = alpha * diff * diff + (1 - alpha) * state["ewma_var"]
        ewma_std = math.sqrt(state["ewma_var"])

        # Calculate deviation
        if ewma_std < 1e-10:
            deviation = 0.0
        else:
            deviation = abs(value - state["ewma"]) / ewma_std

        # Adaptive threshold using EWMA of deviations
        is_anomaly = deviation > threshold_sigma

        return is_anomaly, deviation, {
            "ewma": round(state["ewma"], 4),
            "ewma_std": round(ewma_std, 4),
            "deviation": round(deviation, 4),
            "threshold": threshold_sigma,
            "value": value,
            "alpha": alpha
        }

    def detect_moving_median(self, metric: str, value: float,
                             window_size: int = 100,
                             threshold: float = 3.0) -> Tuple[bool, float, Dict]:
        """
        Moving Median anomaly detection.
        More robust to outliers than mean-based methods.
        Uses Median Absolute Deviation (MAD) instead of standard deviation.
        """
        window = self._get_window(f"{metric}_median", window_size)
        window.add(value)

        if window.count < 10:
            return False, 0.0, {"reason": "insufficient_data", "count": window.count}

        median = window.median()
        mad = window.mad()

        # MAD to std conversion factor (for normal distribution)
        mad_to_std = 1.4826

        if mad < 1e-10:
            return False, 0.0, {"reason": "near_zero_mad", "median": median}

        # Modified Z-score using MAD
        modified_z = abs(value - median) / (mad * mad_to_std)
        is_anomaly = modified_z > threshold

        return is_anomaly, modified_z, {
            "median": round(median, 4),
            "mad": round(mad, 4),
            "modified_z": round(modified_z, 4),
            "threshold": threshold,
            "value": value,
            "window_size": window.count,
            "percentile_25": round(window.percentile(25), 4),
            "percentile_75": round(window.percentile(75), 4)
        }

    def detect(self, metric: str, value: float, rule: Dict) -> Tuple[bool, Dict]:
        """
        Run detection based on rule configuration.
        Returns: (is_anomaly, detection_result)
        """
        algorithm = rule.get("algorithm", "zscore")
        params = rule.get("params", {})
        threshold = rule.get("threshold", 3.0)
        dynamic = rule.get("dynamic_threshold", False)

        if algorithm == "zscore":
            is_anomaly, score, details = self.detect_zscore(
                metric, value,
                window_size=params.get("window_size", 200),
                threshold=threshold
            )
        elif algorithm == "ewma":
            is_anomaly, score, details = self.detect_ewma(
                metric, value,
                alpha=params.get("alpha", 0.3),
                threshold_sigma=threshold
            )
        elif algorithm == "moving_median":
            is_anomaly, score, details = self.detect_moving_median(
                metric, value,
                window_size=params.get("window_size", 100),
                threshold=threshold
            )
        else:
            return False, {"error": f"Unknown algorithm: {algorithm}"}

        # Dynamic threshold adjustment
        if dynamic and is_anomaly:
            # Use percentile-based threshold to reduce false positives
            window_key = f"{metric}_window"
            if window_key in self.windows:
                window = self.windows[window_key]
                if window.count >= 50:
                    p99 = window.percentile(99)
                    p01 = window.percentile(1)
                    if p01 <= value <= p99:
                        # Value is within 1st-99th percentile range
                        is_anomaly = False
                        details["dynamic_override"] = True

        result = {
            "metric": metric,
            "value": value,
            "algorithm": algorithm,
            "is_anomaly": is_anomaly,
            "score": round(score, 4),
            "threshold": threshold,
            "details": details,
            "timestamp": time.time()
        }

        return is_anomaly, result

    def get_state_info(self) -> Dict:
        """Get current detector state for monitoring."""
        info = {}
        for key, window in self.windows.items():
            info[key] = {
                "size": len(window),
                "max_size": window.max_size,
                "mean": round(window.mean(), 4) if window.count > 0 else None,
                "std": round(window.std(), 4) if window.count > 0 else None
            }
        return info

    def reset(self, metric: Optional[str] = None):
        """Reset detector state for a metric or all metrics."""
        if metric:
            keys_to_remove = [k for k in self.windows if k.startswith(metric)]
            for k in keys_to_remove:
                del self.windows[k]
            if metric in self.ewma_state:
                del self.ewma_state[metric]
        else:
            self.windows.clear()
            self.ewma_state.clear()