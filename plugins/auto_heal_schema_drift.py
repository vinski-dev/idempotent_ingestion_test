# 02_auto_healers.py
import pandas as pd
import json
import uuid
import hashlib
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

def hash_row(row):
    """Hashes the business columns only."""
    row_str = "".join(row.astype(str))
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def heal_schema_type_drift(db_uri: str):
    """
    Sweeps the DLQ for SCHEMA_TYPE_DRIFT errors, fixes the 'age' strings,
    appends the fixed data to the raw table, and marks the DLQ records as REPROCESSED.
    """
    engine = create_engine(db_uri)
    
    # Step 1: Fetch pending records
    query = """
        SELECT dlq_id, raw_payload 
        FROM analytics.dlq_audit_log 
        WHERE status = 'PENDING' AND error_category = 'SCHEMA_TYPE_DRIFT';
    """
    df_dlq = pd.read_sql(query, engine)
    
    if df_dlq.empty:
        print("No pending records to heal.")
        return

    # Step 2: Parse JSON payloads
    parsed_payloads = [json.loads(row) if isinstance(row, str) else row for row in df_dlq['raw_payload']]
    df_healed = pd.DataFrame(parsed_payloads)
    
    # Step 3: Apply Business Rule Fix (Convert bad 'age' strings to -1)
    df_healed['age'] = pd.to_numeric(df_healed['age'], errors='coerce').fillna(-1).astype(int)

    # Step 4: Append Metadata for the Micro-Batch
    batch_id = f"AUTO-HEAL-{uuid.uuid4().hex[:6].upper()}"
    df_healed = df_healed.assign(
        filehash=lambda d: d.apply(hash_row, axis=1),
        batch_id=batch_id,
        _loaded_at=datetime.now(timezone.utc)
    )

    # Step 5: Append to raw_events table
    df_healed.to_sql('raw_events', engine, schema='analytics', if_exists='append', index=False)

    # Step 6: Update DLQ Status
    dlq_ids = tuple(df_dlq['dlq_id'].tolist())
    if len(dlq_ids) == 1:
        update_query = text(f"UPDATE analytics.dlq_audit_log SET status = 'REPROCESSED' WHERE dlq_id = :id")
        params = {'id': dlq_ids[0]}
    else:
        update_query = text(f"UPDATE analytics.dlq_audit_log SET status = 'REPROCESSED' WHERE dlq_id IN :ids")
        params = {'ids': dlq_ids}
        
    with engine.begin() as conn:
        conn.execute(update_query, params)
    
    print(f"Successfully healed and reprocessed {len(df_dlq)} records.")

if __name__ == '__main__':
    import os
    # Example usage (ensure your DB_URI is set)
    # DB_URI = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/postgres")
    # heal_schema_type_drift(DB_URI)
