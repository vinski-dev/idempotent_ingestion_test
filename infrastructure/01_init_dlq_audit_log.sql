-- 01_init_dlq_audit_log.sql
-- Run this in PostgreSQL to create the Unified DLQ Audit Log table

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dlq_audit_log (
    dlq_id UUID PRIMARY KEY,
    error_category VARCHAR(255) NOT NULL,
    error_message TEXT,
    raw_payload JSONB NOT NULL,
    status VARCHAR(50) DEFAULT 'PENDING',
    failed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster querying by the auto-healer
CREATE INDEX IF NOT EXISTS idx_dlq_status_category 
ON analytics.dlq_audit_log(status, error_category);
