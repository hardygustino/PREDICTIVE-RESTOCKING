-- WATERMARK / METADATA

CREATE TABLE IF NOT EXISTS etl_metadata (
  pipeline_name        TEXT PRIMARY KEY,
  last_watermark_ts    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  updated_at           TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

INSERT INTO etl_metadata (pipeline_name, last_watermark_ts)
VALUES ('restock_hourly', '2016-01-01 00:00:00')
ON CONFLICT (pipeline_name) DO NOTHING;
