import pandas as pd
import hashlib
import time
import uuid
from datetime import datetime, timezone


def hash_row(row):
    """Hashes purely the business columns."""
    row_str = "".join(row.astype(str))
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()


def add_metadata(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Calculates hash, then appends tracking metadata."""
    return df.assign(
        filehash=lambda d: d.apply(hash_row, axis=1), batch_id=batch_id, _loaded_at=datetime.now(timezone.utc)
    )


def simulate_dbt_deduplication(raw_table: pd.DataFrame) -> pd.DataFrame:
    """
    Simulates the SQL: ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY _loaded_at DESC)
    We sort by timestamp descending, then drop duplicates keeping the first (newest) row.
    """
    return raw_table.sort_values(by="_loaded_at", ascending=False).drop_duplicates(subset=["event_id"], keep="first")


# ==========================================
# 1. INITIAL INGESTION (With a corrupted record)
# ==========================================
print("--- 1. INITIAL BATCH (EVT-003 has a negative age) ---")
initial_data = [
    {"event_id": "EVT-001", "name": "Alice", "age": 28},
    {"event_id": "EVT-002", "name": "Bob", "age": 35},
    {"event_id": "EVT-003", "name": "Charlie", "age": -99},  # The problematic row
]
df_raw_table = pd.DataFrame(initial_data).pipe(add_metadata, batch_id=f"BATCH-{uuid.uuid4().hex[:6].upper()}")

print(df_raw_table.to_string(index=False))

print("\n(Simulating time passing while data engineers fix the issue...)")
time.sleep(2)

# ==========================================
# 2. TARGETED DLQ REPLAY (Micro-batching the fix)
# ==========================================
print("\n--- 2. TARGETED DLQ REPLAY (Ingesting ONLY the fixed EVT-003) ---")
# We extract ONLY the bad record, fix the business data, and ingest it as a micro-batch
fixed_data = [{"event_id": "EVT-003", "name": "Charlie", "age": 32}]  # Fixed the age
df_dlq_fix = pd.DataFrame(fixed_data).pipe(add_metadata, batch_id=f"FIX-{uuid.uuid4().hex[:6].upper()}")

# Append it to the Raw Table (Notice: We do NOT delete the old corrupted row)
df_raw_table = pd.concat([df_raw_table, df_dlq_fix], ignore_index=True)

print("--- CURRENT RAW TABLE (Contains physical duplicates) ---")
print(df_raw_table.to_string(index=False))


# ==========================================
# 3. DOWNSTREAM RESOLUTION (The Idempotent Magic)
# ==========================================
print("\n--- 3. DOWNSTREAM CLEAN TABLE (After dbt logic runs) ---")
df_clean_table = simulate_dbt_deduplication(df_raw_table)

# Re-sorting by event_id just for clean display
df_clean_table = df_clean_table.sort_values("event_id")
print(df_clean_table.to_string(index=False))
