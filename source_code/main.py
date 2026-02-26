import os
import tempfile
import time
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, validator

from db import COLUMN_WHITELIST, create_export_job, get_connection, get_export_job
from exporters.factory import get_exporter
from exporters.utils import MemoryTracker, gzip_stream

app = FastAPI(title="Streaming Data Export Service")


class ColumnMapping(BaseModel):
    source: str = Field(..., description="Column name in DB")
    target: str = Field(..., description="Column name in export")


class ExportRequest(BaseModel):
    format: str
    columns: List[ColumnMapping]
    compression: Optional[str] = None

    @validator("format")
    def format_supported(cls, value):
        if value not in {"csv", "json", "xml", "parquet"}:
            raise ValueError("format must be one of: csv, json, xml, parquet")
        return value

    @validator("compression")
    def compression_supported(cls, value, values):
        if value is None:
            return value
        if value != "gzip":
            raise ValueError("compression must be gzip")
        if values.get("format") == "parquet":
            raise ValueError("compression not supported for parquet")
        return value

    @validator("columns")
    def columns_valid(cls, value):
        if not value:
            raise ValueError("columns must not be empty")
        for column in value:
            if column.source not in COLUMN_WHITELIST:
                raise ValueError(f"unsupported column: {column.source}")
        return value


@app.post("/exports", status_code=201)
def create_export(request: ExportRequest):
    source_columns = [column.source for column in request.columns]
    target_columns = [column.target for column in request.columns]

    if len(source_columns) != len(set(source_columns)):
        raise HTTPException(status_code=400, detail="duplicate source columns")
    if len(target_columns) != len(set(target_columns)):
        raise HTTPException(status_code=400, detail="duplicate target columns")

    with get_connection() as conn:
        export_id = create_export_job(
            conn,
            request.format,
            [column.dict() for column in request.columns],
            request.compression,
        )

    return {"exportId": export_id, "status": "pending"}


@app.get("/exports/{export_id}/download")
def download_export(export_id: str):
    with get_connection() as conn:
        job = get_export_job(conn, export_id)
        if not job:
            raise HTTPException(status_code=404, detail="export job not found")

    exporter = get_exporter(job["format"])
    columns = job["columns"]
    source_columns = [column["source"] for column in columns]
    target_columns = [column["target"] for column in columns]

    def generate():
        with get_connection() as conn:
            for chunk in exporter.stream(conn, source_columns, target_columns):
                yield chunk

    stream = generate()
    headers = {
        "Content-Disposition": f"attachment; filename=export.{exporter.file_extension}",
    }
    if job.get("compression") == "gzip" and exporter.supports_gzip:
        stream = gzip_stream(stream)
        headers["Content-Encoding"] = "gzip"
        headers["Content-Disposition"] = (
            f"attachment; filename=export.{exporter.file_extension}.gz"
        )

    return StreamingResponse(stream, media_type=exporter.content_type, headers=headers)


@app.get("/exports/benchmark")
def export_benchmark():
    dataset_row_count = 10000000
    results = []
    columns = [
        {"source": "id", "target": "id"},
        {"source": "created_at", "target": "created_at"},
        {"source": "name", "target": "name"},
        {"source": "value", "target": "value"},
        {"source": "metadata", "target": "metadata"},
    ]
    source_columns = [col["source"] for col in columns]
    target_columns = [col["target"] for col in columns]

    with get_connection() as conn:
        for format_name in ["csv", "json", "xml", "parquet"]:
            exporter = get_exporter(format_name)
            tracker = MemoryTracker()
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{exporter.file_extension}") as handle:
                file_path = handle.name
            start = time.monotonic()
            if format_name == "parquet":
                exporter.export_to_file(conn, source_columns, target_columns, file_path, tracker)
            else:
                exporter.export_to_file(conn, source_columns, target_columns, file_path, tracker)
            duration = time.monotonic() - start
            size_bytes = os.path.getsize(file_path)
            results.append(
                {
                    "format": format_name,
                    "durationSeconds": round(duration, 3),
                    "fileSizeBytes": size_bytes,
                    "peakMemoryMB": round(tracker.peak_mb, 2),
                }
            )
            os.remove(file_path)

    return JSONResponse(
        {
            "datasetRowCount": dataset_row_count,
            "results": results,
        }
    )
