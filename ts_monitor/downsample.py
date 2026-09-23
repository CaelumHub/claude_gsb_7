"""
LTTB (Largest Triangle Three Buckets) Downsampling Algorithm
- Preserves visual shape of time-series data
- Efficient for large datasets
- Configurable target point count
"""

import math
from typing import List, Dict, Tuple


def lttb_downsample(data: List[Dict], target_points: int) -> List[Dict]:
    """
    Largest Triangle Three Buckets downsampling.

    Preserves the visual shape of the data by selecting points
    that maximize the visible triangle area.

    Args:
        data: List of dicts with 't' (timestamp) and 'v' (value) keys
        target_points: Desired number of output points

    Returns:
        Downsampled list of points
    """
    if len(data) <= target_points or target_points < 3:
        return data

    # Always include first and last points
    sampled = [data[0]]

    # Bucket size (number of points per bucket)
    bucket_size = (len(data) - 2) / (target_points - 2)

    prev_index = 0  # Index of the previously selected point

    for i in range(1, target_points - 1):
        # Calculate bucket boundaries
        bucket_start = int((i - 1) * bucket_size) + 1
        bucket_end = int(i * bucket_size) + 1
        if bucket_end >= len(data):
            bucket_end = len(data) - 1

        # Calculate the average point in the NEXT bucket
        # (used for triangle area calculation)
        next_bucket_start = int(i * bucket_size) + 1
        next_bucket_end = int((i + 1) * bucket_size) + 1
        if next_bucket_end >= len(data):
            next_bucket_end = len(data) - 1

        # Average of next bucket
        avg_t = 0.0
        avg_v = 0.0
        count = 0
        for j in range(next_bucket_start, next_bucket_end + 1):
            avg_t += data[j]["t"]
            avg_v += data[j]["v"]
            count += 1

        if count > 0:
            avg_t /= count
            avg_v /= count

        # Find the point in current bucket that forms the largest triangle
        # with the previous point and the average of the next bucket
        max_area = -1.0
        selected_index = bucket_start

        point_a_t = data[prev_index]["t"]
        point_a_v = data[prev_index]["v"]

        for j in range(bucket_start, bucket_end + 1):
            # Calculate triangle area using cross product
            area = abs(
                (point_a_t - avg_t) * (data[j]["v"] - point_a_v) -
                (point_a_t - data[j]["t"]) * (avg_v - point_a_v)
            )

            if area > max_area:
                max_area = area
                selected_index = j

        sampled.append(data[selected_index])
        prev_index = selected_index

    # Always include last point
    sampled.append(data[-1])

    return sampled


def lttb_downsample_adaptive(data: List[Dict], target_points: int,
                              min_value: float = None,
                              max_value: float = None) -> List[Dict]:
    """
    Adaptive LTTB that adjusts based on data characteristics.

    Uses a tighter bucket for regions with high variance
    and looser buckets for flat regions.
    """
    if len(data) <= target_points or target_points < 3:
        return data

    # Calculate local variance for adaptive bucketing
    window = 20
    variances = []
    for i in range(len(data)):
        start = max(0, i - window)
        end = min(len(data), i + window + 1)
        vals = [data[j]["v"] for j in range(start, end)]
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        variances.append(var)

    # Normalize variances
    max_var = max(variances) if variances else 1.0
    if max_var > 0:
        variances = [v / max_var for v in variances]
    else:
        variances = [1.0] * len(variances)

    # Allocate more points to high-variance regions
    total_variance = sum(variances)
    if total_variance == 0:
        return lttb_downsample(data, target_points)

    # Calculate point allocation per segment
    sampled = [data[0]]
    points_remaining = target_points - 2  # Exclude first and last

    # Divide into segments and allocate points proportionally
    segment_count = min(50, len(data) // 10)
    if segment_count < 2:
        return lttb_downsample(data, target_points)

    segment_size = len(data) // segment_count
    segment_variances = []

    for s in range(segment_count):
        start = s * segment_size
        end = start + segment_size if s < segment_count - 1 else len(data)
        seg_var = sum(variances[start:end]) / (end - start)
        segment_variances.append(seg_var)

    total_seg_var = sum(segment_variances)
    segment_points = []
    for sv in segment_variances:
        pts = max(1, int(points_remaining * sv / total_seg_var))
        segment_points.append(pts)

    # Adjust to match target
    diff = points_remaining - sum(segment_points)
    for i in range(abs(diff)):
        idx = i % len(segment_points)
        segment_points[idx] += 1 if diff > 0 else -1

    # Downsample each segment
    for s in range(segment_count):
        start = s * segment_size
        end = start + segment_size if s < segment_count - 1 else len(data)
        segment_data = data[start:end]

        n_points = segment_points[s]
        if n_points >= len(segment_data):
            sampled.extend(segment_data[1:])
        else:
            sub_sampled = lttb_downsample(segment_data, n_points)
            # Avoid duplicating boundary points
            if sampled and sub_sampled:
                if sampled[-1]["t"] == sub_sampled[0]["t"]:
                    sub_sampled = sub_sampled[1:]
            sampled.extend(sub_sampled)

    sampled.append(data[-1])
    return sampled


def downsample_simple(data: List[Dict], target_points: int,
                      method: str = "lttb") -> List[Dict]:
    """
    Simple downsampling interface.

    Args:
        data: Time-series data points
        target_points: Target number of points
        method: "lttb", "adaptive", or "every_nth"

    Returns:
        Downsampled data
    """
    if not data or target_points <= 0:
        return []

    if len(data) <= target_points:
        return data

    if method == "lttb":
        return lttb_downsample(data, target_points)
    elif method == "adaptive":
        return lttb_downsample_adaptive(data, target_points)
    elif method == "every_nth":
        step = len(data) / target_points
        return [data[int(i * step)] for i in range(target_points)]
    else:
        return lttb_downsample(data, target_points)