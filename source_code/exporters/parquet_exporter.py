import os
import tempfile
from decimal import Decimal
from typing import Iterable, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from db import create_named_cursor
from exporters.base import BaseExporter
from exporters.utils import MemoryTracker


class ParquetExporter(BaseExporter):
    format_name = "parquet"
    content_type = "application/vnd.apache.parquet"
    file_extension = "parquet"
    supports_gzip = False

    def stream(self, conn, source_columns: List[str], target_columns: List[str]) -> Iterable[bytes]:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as handle:
            temp_path = handle.name
        try:
            self.export_to_file(conn, source_columns, target_columns, temp_path, None)
            with open(temp_path, "rb") as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def export_to_file(
        self,
        conn,
        source_columns: List[str],
        target_columns: List[str],
        file_path: str,
        memory_tracker: MemoryTracker | None = None,
    ) -> Tuple[int, float]:
        schema = self._build_schema(source_columns, target_columns)
        cur = create_named_cursor(conn, source_columns)
        try:
            with pq.ParquetWriter(file_path, schema=schema) as writer:
                while True:
                    rows = cur.fetchmany(cur.itersize)
                    if not rows:
                        break
                    arrays = []
                    for index, column in enumerate(source_columns):
                        values = [row[index] for row in rows]
                        arrays.append(self._to_arrow_array(column, values))
                    table = pa.Table.from_arrays(arrays, names=target_columns, schema=schema)
                    writer.write_table(table)
                    if memory_tracker:
                        memory_tracker.update()
        finally:
            cur.close()
        return os.path.getsize(file_path), memory_tracker.peak_mb if memory_tracker else 0.0

    def _build_schema(self, source_columns: List[str], target_columns: List[str]) -> pa.Schema:
        fields = []
        for source, target in zip(source_columns, target_columns):
            if source == "id":
                fields.append(pa.field(target, pa.int64()))
            elif source == "created_at":
                fields.append(pa.field(target, pa.timestamp("us", tz="UTC")))
            elif source == "name":
                fields.append(pa.field(target, pa.string()))
            elif source == "value":
                fields.append(pa.field(target, pa.decimal128(18, 4)))
            elif source == "metadata":
                fields.append(pa.field(target, self._metadata_type()))
        return pa.schema(fields)

    def _metadata_type(self) -> pa.DataType:
        return pa.struct(
            [
                pa.field("source", pa.string()),
                pa.field(
                    "flags",
                    pa.struct(
                        [
                            pa.field("is_active", pa.bool_()),
                            pa.field("tier", pa.int32()),
                        ]
                    ),
                ),
                pa.field("tags", pa.list_(pa.string())),
                pa.field("metrics", pa.struct([pa.field("score", pa.float64())])),
            ]
        )

    def _normalize_metadata(self, value):
        if not isinstance(value, dict):
            return None
        return {
            "source": value.get("source"),
            "flags": {
                "is_active": value.get("flags", {}).get("is_active"),
                "tier": value.get("flags", {}).get("tier"),
            },
            "tags": value.get("tags"),
            "metrics": {"score": value.get("metrics", {}).get("score")},
        }

    def _to_arrow_array(self, column: str, values: List):
        if column == "metadata":
            normalized = [self._normalize_metadata(value) for value in values]
            return pa.array(normalized, type=self._metadata_type())
        if column == "value":
            return pa.array(values, type=pa.decimal128(18, 4))
        if column == "created_at":
            return pa.array(values, type=pa.timestamp("us", tz="UTC"))
        if column == "id":
            return pa.array(values, type=pa.int64())
        return pa.array(values)
