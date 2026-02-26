import json
import zlib
from decimal import Decimal
from typing import Any

import psutil


class MemoryTracker:
    def __init__(self):
        self._process = psutil.Process()
        self.peak_mb = 0.0

    def update(self):
        rss = self._process.memory_info().rss / (1024 * 1024)
        if rss > self.peak_mb:
            self.peak_mb = rss


def gzip_stream(chunks, level=5):
    compressor = zlib.compressobj(level, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
    for chunk in chunks:
        if not chunk:
            continue
        data = compressor.compress(chunk)
        if data:
            yield data
    tail = compressor.flush()
    if tail:
        yield tail


def normalize_json_value(value: Any):
    if isinstance(value, Decimal):
        return float(value)
    return value


def json_dumps(value: Any):
    return json.dumps(value, default=normalize_json_value, separators=(",", ":"))
