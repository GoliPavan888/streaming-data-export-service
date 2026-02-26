import json
import os
import uuid
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2 import sql

DATABASE_URL = os.environ.get("DATABASE_URL")

COLUMN_WHITELIST = {
    "id",
    "created_at",
    "name",
    "value",
    "metadata",
}

DEFAULT_FETCH_SIZE = 5000


def _connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    conn = psycopg2.connect(DATABASE_URL)
    psycopg2.extras.register_default_jsonb(conn, loads=json.loads, globally=False)
    return conn


@contextmanager
def get_connection():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def create_export_job(conn, export_format, columns, compression):
    export_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO export_jobs (id, format, columns, compression, status)
            VALUES (%s, %s, %s, %s, 'pending')
            """,
            (export_id, export_format, json.dumps(columns), compression),
        )
    conn.commit()
    return export_id


def get_export_job(conn, export_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, format, columns, compression, status FROM export_jobs WHERE id = %s",
            (export_id,),
        )
        job = cur.fetchone()
    return job


def create_named_cursor(conn, columns, fetch_size=DEFAULT_FETCH_SIZE):
    cursor_name = f"export_{uuid.uuid4().hex}"
    cur = conn.cursor(name=cursor_name)
    cur.itersize = fetch_size
    query = sql.SQL("SELECT {fields} FROM records").format(
        fields=sql.SQL(", ").join(sql.Identifier(col) for col in columns)
    )
    cur.execute(query)
    return cur
