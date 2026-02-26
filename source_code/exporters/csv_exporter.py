import csv
import io
import json
from decimal import Decimal
from typing import Iterable, List

from db import create_named_cursor
from exporters.base import BaseExporter


class CsvExporter(BaseExporter):
    format_name = "csv"
    content_type = "text/csv"
    file_extension = "csv"
    supports_gzip = True

    def stream(self, conn, source_columns: List[str], target_columns: List[str]) -> Iterable[bytes]:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(target_columns)
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0)
        buffer.truncate(0)

        cur = create_named_cursor(conn, source_columns)
        try:
            while True:
                rows = cur.fetchmany(cur.itersize)
                if not rows:
                    break
                for row in rows:
                    output = []
                    for value in row:
                        if isinstance(value, Decimal):
                            output.append(str(value))
                        elif isinstance(value, dict):
                            output.append(json.dumps(value, separators=(",", ":")))
                        else:
                            output.append(value)
                    writer.writerow(output)
                yield buffer.getvalue().encode("utf-8")
                buffer.seek(0)
                buffer.truncate(0)
        finally:
            cur.close()
