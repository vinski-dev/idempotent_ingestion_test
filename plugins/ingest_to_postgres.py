import os
import uuid
import hashlib
from datetime import datetime, timezone
import pandas as pd
import json
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError
from faker import Faker
from dotenv import load_dotenv

# Initialize Faker
fake = Faker()

# Load environment variables
load_dotenv()

# --- Configuration ---
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
SCHEMA_NAME = "analytics"
TABLE_NAME = "raw_events"

# NEW: PII Salt and Alias Mapping Contract
PII_SALT = os.getenv("PII_SALT", "enterprise_secret_salt_123")
SCHEMA_CONTRACT_ALIASES = {
    "user_location": "location",
    "loc": "location",
    "client_age": "age",
    "event_name": "event_type",
    "user_email": "email",
}

DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# Create SQLAlchemy Engine
engine = create_engine(DB_URI)


def setup_schema():
    """Ensure the analytics schema exists in the database."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))
        conn.commit()
    print(f"✅ Schema '{SCHEMA_NAME}' is ready.")


def mask_pii(email):
    """Hashes PII deterministically using a secret salt."""
    if pd.isna(email) or email == "":
        return None
    salted = str(email).lower().strip() + PII_SALT
    return hashlib.sha256(salted.encode("utf-8")).hexdigest()


def hash_row(row):
    """Hashes ALL business columns dynamically."""
    row_str = "".join(row.astype(str))
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()


def add_metadata(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Applies the dynamic hash and appends tracking metadata using method chaining."""
    return df.assign(
        filehash=lambda d: d.apply(hash_row, axis=1), batch_id=batch_id, _loaded_at=datetime.now(timezone.utc)
    )


def get_pg_type(dtype):
    """Maps Pandas data types to PostgreSQL data types."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR"


def check_and_evolve_schema(df: pd.DataFrame, table_name: str, engine, schema: str = "analytics"):
    """Checks for new columns and runs ALTER TABLE to evolve the schema dynamically."""
    # Check if table exists first
    check_table_query = text(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        );
    """)

    with engine.connect() as conn:
        table_exists = conn.execute(check_table_query).scalar()
        if not table_exists:
            return  # Let pandas to_sql create the table on the first run

        # Get existing columns
        col_query = text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table_name}';
        """)
        existing_cols = {row[0] for row in conn.execute(col_query)}

    # Identify new columns present in the DataFrame but missing in Postgres
    # Note: We exclude metadata columns since they are appended later
    df_cols = set(df.columns)
    new_cols = df_cols - existing_cols

    # Evolve schema for any new columns found
    if new_cols:
        with engine.begin() as conn:
            for col in new_cols:
                pg_type = get_pg_type(df[col].dtype)
                alter_query = text(f"ALTER TABLE {schema}.{table_name} ADD COLUMN {col} {pg_type};")
                conn.execute(alter_query)
                print(f"[SCHEMA EVOLUTION] Added new column '{col}' of type {pg_type} to {table_name}.")


def ingest_batch(df: pd.DataFrame, table_name: str, engine, batch_id: str):
    """
    Handles Additive Drift (pre-flight ALTER TABLE), Type Drift (try/except DLQ routing),
    Destructive Drift (Alias Mapping), and PII Masking.
    """
    # 1. Intercept and rename destructive drift columns (Alias Mapping)
    rename_mapping = {}
    for col in df.columns:
        if col in SCHEMA_CONTRACT_ALIASES:
            rename_mapping[col] = SCHEMA_CONTRACT_ALIASES[col]
            print(f"[DRIFT DETECTED] Intercepted and renamed '{col}' back to '{SCHEMA_CONTRACT_ALIASES[col]}'")

    if rename_mapping:
        df = df.rename(columns=rename_mapping)

    # 2. Apply PII Masking securely before hitting the database
    if "email" in df.columns:
        df["email_hash"] = df["email"].apply(mask_pii)
        df = df.drop(columns=["email"])
        print("[SECURITY] Hashed 'email' column to 'email_hash' and dropped raw PII.")

    # 3. Pre-flight Check: Evolve schema if there are new columns
    check_and_evolve_schema(df, table_name, engine)

    # 4. Prepare payload using method chaining
    df_prepared = df.copy().pipe(add_metadata, batch_id=batch_id)

    try:
        # 5. Try to insert into the main raw table
        df_prepared.to_sql(table_name, engine, schema="analytics", if_exists="append", index=False)
        print(f"[SUCCESS] Batch {batch_id} successfully ingested into {table_name}.")

    except SQLAlchemyError as e:
        # 6. Catch database errors (like type collisions)
        error_msg = str(e.__dict__.get("orig", e))
        print(f"[FAILED] Batch {batch_id} rejected by database. Routing to DLQ...")

        # 7. Transform the failed dataframe into DLQ Audit Log format
        dlq_records = []
        for _, row in df.iterrows():  # Store the original row, excluding metadata
            dlq_records.append(
                {
                    "dlq_id": str(uuid.uuid4()),
                    "error_category": "SCHEMA_TYPE_DRIFT",
                    "error_message": error_msg,
                    "raw_payload": json.dumps(row.to_dict()),
                    "status": "PENDING",
                    "failed_at": datetime.now(timezone.utc),
                }
            )

        df_dlq = pd.DataFrame(dlq_records)

        # 8. Insert into the Unified DLQ Audit Log table
        df_dlq.to_sql("dlq_audit_log", engine, schema="analytics", if_exists="append", index=False)
        print(f"[DLQ ROUTED] {len(df_dlq)} records safely stored in analytics.dlq_audit_log.")


# ==========================================
# RUNNING THE FULL SIMULATION
# ==========================================
if __name__ == "__main__":
    engine = create_engine(DB_URI)

    # Run 1: Initial Good Data
    good_data = pd.DataFrame({"event_id": ["EVT-101", "EVT-102"], "event_type": ["click", "view"], "age": [25, 30]})
    print("\n--- RUN 1: INITIAL BATCH ---")
    ingest_batch(good_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")

    # Run 2: Additive Drift (New 'location' column)
    additive_data = pd.DataFrame(
        {
            "event_id": ["EVT-103", "EVT-104"],
            "event_type": ["purchase", "click"],
            "age": [35, 40],
            "location": ["Manila", "Cebu"],  # New column
        }
    )
    print("\n--- RUN 2: ADDITIVE DRIFT BATCH ---")
    ingest_batch(additive_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")

    # Run 3: Type Drift (String 'unknown' in integer 'age' column)
    type_drift_data = pd.DataFrame(
        {
            "event_id": ["EVT-105", "EVT-106"],
            "event_type": ["view", "scroll"],
            "age": ["unknown", "not_provided"],  # Type collision
            "location": ["Davao", "Manila"],
        }
    )
    print("\n--- RUN 3: TYPE DRIFT BATCH ---")
    ingest_batch(type_drift_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")

    # NEW: Run 4: Destructive Drift & PII Security
    destructive_pii_data = pd.DataFrame(
        {
            "event_id": ["EVT-107", "EVT-108"],
            "event_name": ["login", "logout"],  # Destructive Drift: should be event_type
            "client_age": [22, 45],  # Destructive Drift: should be age
            "email": ["alvin@example.com", "test@test.com"],  # PII Field
        }
    )
    print("\n--- RUN 4: DESTRUCTIVE DRIFT & PII BATCH ---")
    ingest_batch(destructive_pii_data, "raw_events", engine, batch_id=f"BATCH-{uuid.uuid4().hex[:6]}")
