#!/usr/bin/env bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS records (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  name VARCHAR(255) NOT NULL,
  value DECIMAL(18, 4) NOT NULL,
  metadata JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS export_jobs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  format VARCHAR(20) NOT NULL,
  columns JSONB NOT NULL,
  compression VARCHAR(20),
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
DECLARE
  row_count BIGINT;
BEGIN
  SELECT COUNT(*) INTO row_count FROM records;
  IF row_count <> 10000000 THEN
    TRUNCATE TABLE records;
    INSERT INTO records (created_at, name, value, metadata)
    SELECT
      NOW() - (random() * interval '365 days'),
      'name_' || gs,
      ROUND((random() * 10000)::numeric, 4),
      jsonb_build_object(
        'source', 'seed',
        'flags', jsonb_build_object(
          'is_active', (random() > 0.5),
          'tier', (random() * 5)::int
        ),
        'tags', to_jsonb(ARRAY[
          'tag_' || (gs % 10),
          'tag_' || (gs % 20)
        ]),
        'metrics', jsonb_build_object(
          'score', ROUND((random() * 100)::numeric, 2)
        )
      )
    FROM generate_series(1, 10000000) AS gs;
  END IF;
END $$;
SQL
