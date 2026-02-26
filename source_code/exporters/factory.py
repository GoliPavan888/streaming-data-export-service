from exporters.csv_exporter import CsvExporter
from exporters.json_exporter import JsonExporter
from exporters.parquet_exporter import ParquetExporter
from exporters.xml_exporter import XmlExporter


EXPORTERS = {
    "csv": CsvExporter(),
    "json": JsonExporter(),
    "xml": XmlExporter(),
    "parquet": ParquetExporter(),
}


def get_exporter(format_name: str):
    exporter = EXPORTERS.get(format_name)
    if not exporter:
        raise ValueError("Unsupported format")
    return exporter
