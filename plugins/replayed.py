import pandas as pd
import hashlib
import time
import uuid
from datetime import datetime, timezone

# 1. Create a "Wide" Source Table (simulating many columns)
# We generate 20 random business columns plus an event_id
data = {f"business_col_{i}": [f"data_value_{i}"] for i in range(1, 21)}
data["event_id"] = ["EVT-9999"]
df_source = pd.DataFrame(data)


def hash_row(row):
    """Hashes ALL columns currently in the row dynamically."""
    row_str = "".join(row.astype(str))
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()


def add_metadata(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Calculates the hash FIRST, then appends metadata."""
    return df.assign(
        # Hash is created here, only looking at the 21 business columns
        filehash=lambda d: d.apply(hash_row, axis=1),
        # Metadata is appended AFTER the hash is generated
        batch_id=batch_id,
        _loaded_at=datetime.now(timezone.utc),
    )


# --- RUNNING THE SIMULATION ---

# Generate a completely unique, random batch ID for Run 1
batch_no_1 = f"BATCH-{uuid.uuid4().hex[:8].upper()}"

print(f"--- RUN 1: Initial Ingestion ---")
df_batch_1 = df_source.copy().pipe(add_metadata, batch_id=batch_no_1)
# Using .to_string() removes the need to pip install 'tabulate'
print(df_batch_1[["event_id", "filehash", "batch_id", "_loaded_at"]].to_string(index=False))

print("\n(Simulating a 2-second delay before the replay...)")
time.sleep(10)

# Generate a completely new, random batch ID for the Replay
batch_no_2 = f"BATCH-{uuid.uuid4().hex[:8].upper()}"

print(f"\n--- RUN 2: Pipeline Replay ---")
df_batch_2 = df_source.copy().pipe(add_metadata, batch_id=batch_no_2)
print(df_batch_2[["event_id", "filehash", "batch_id", "_loaded_at"]].to_string(index=False))
