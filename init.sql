-- init.sql

CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.eventos_validos (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    product_price NUMERIC(10,2) NOT NULL,
    quantity INTEGER NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS streaming.eventos_quarentena (
    id SERIAL PRIMARY KEY,
    raw_event JSONB NOT NULL,
    validation_errors TEXT[] NOT NULL,
    quarantined_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS streaming.pipeline_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    total_events INTEGER NOT NULL,
    valid_events INTEGER NOT NULL,
    invalid_events INTEGER NOT NULL,
    error_rate NUMERIC(5,2) NOT NULL,
    consumer_lag_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT now()
);