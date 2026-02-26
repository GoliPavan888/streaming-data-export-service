from decimal import Decimal
from typing import Iterable, List

from db import create_named_cursor
from exporters.base import BaseExporter
from exporters.utils import json_dumps, normalize_json_value


class JsonExporter(BaseExporter):
    format_name = "json"
    content_type = "application/json"
    file_extension = "json"
    supports_gzip = True

    def stream(self, conn, source_columns: List[str], target_columns: List[str]) -> Iterable[bytes]:
        cur = create_named_cursor(conn, source_columns)
        first = True
        try:
            yield b"["
            while True:
                rows = cur.fetchmany(cur.itersize)
                if not rows:
                    break
                for row in rows:
                    record = {}
                    for index, value in enumerate(row):
                        if isinstance(value, Decimal):
                            record[target_columns[index]] = float(value)
                        else:
                            record[target_columns[index]] = normalize_json_value(value)
                    payload = json_dumps(record).encode("utf-8")
                    if first:
                        yield payload
                        first = False
                    else:
                        yield b"," + payload
            yield b"]"
        finally:
            cur.close()
