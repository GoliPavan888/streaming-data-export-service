from decimal import Decimal
from typing import Iterable, List
from xml.sax.saxutils import escape

from db import create_named_cursor
from exporters.base import BaseExporter


class XmlExporter(BaseExporter):
    format_name = "xml"
    content_type = "application/xml"
    file_extension = "xml"
    supports_gzip = True

    def stream(self, conn, source_columns: List[str], target_columns: List[str]) -> Iterable[bytes]:
        cur = create_named_cursor(conn, source_columns)
        try:
            yield b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<records>"
            while True:
                rows = cur.fetchmany(cur.itersize)
                if not rows:
                    break
                chunks = []
                for row in rows:
                    chunks.append("<record>")
                    for index, value in enumerate(row):
                        field_name = target_columns[index]
                        chunks.append(self._to_xml(field_name, value))
                    chunks.append("</record>")
                yield "".join(chunks).encode("utf-8")
            yield b"</records>"
        finally:
            cur.close()

    def _to_xml(self, name: str, value) -> str:
        if value is None:
            return f"<{name} />"
        if isinstance(value, Decimal):
            return f"<{name}>{escape(str(value))}</{name}>"
        if isinstance(value, dict):
            inner = "".join(self._to_xml(key, val) for key, val in value.items())
            return f"<{name}>{inner}</{name}>"
        if isinstance(value, list):
            inner = "".join(self._to_xml("item", item) for item in value)
            return f"<{name}>{inner}</{name}>"
        return f"<{name}>{escape(str(value))}</{name}>"
