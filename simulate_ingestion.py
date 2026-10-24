import os
import uuid
import hashlib
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
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

# Create SQLAlchemy Engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def setup_schema():
    """Ensure the analytics schema exists in the database."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))
        conn.commit()
    print(f"✅ Schema '{SCHEMA_NAME}' is ready.")

def hash_row(row):
    """Generate a MD5 hash for a given row's payload."""
    row_str = "".join(row.astype(str))
    return hashlib.md5(row_str.encode('utf-8')).hexdigest()

def add_metadata(df: pd.DataFrame, batch_id: str, source_filename: str) -> pd.DataFrame:
    """Appends required metadata columns for idempotency using method chaining."""
    return (
        df.assign(
            filehash=lambda d: d.apply(hash_row, axis=1),
            batch_id=batch_id,
            source_filename=source_filename,
            _loaded_at=datetime.now(timezone.utc)
        )
    )

def handle_schema_evolution(df: pd.DataFrame, table_name: str):
    """
    Checks if the DataFrame has new columns not present in the Postgres table
    and executes ALTER TABLE statements to add them before ingestion.
    """
    with engine.connect() as conn:
        try:
            # Check existing columns
            result = conn.execute(text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{table_name}';
            """))
            existing_columns = [row[0] for row in result.fetchall()]

            if not existing_columns:
                return # Table doesn't exist yet, pandas will create it

            for col in df.columns:
                if col not in existing_columns:
                    print(f"⚠️ Schema Drift Detected: Adding new column '{col}' to {table_name}")
                    # Defaulting to TEXT for simplicity in this simulation
                    conn.execute(text(f"ALTER TABLE {SCHEMA_NAME}.{table_name} ADD COLUMN {col} TEXT;"))
            conn.commit()
        except ProgrammingError as e:
            print(f"Error checking/altering schema: {e}")

# ==========================================
# SCENARIO 1: Baseline Generation
# ==========================================
def run_scenario_1_baseline():
    print("\n--- Running Scenario 1: Baseline Generation ---")
    data = [{
        "event_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "event_type": fake.random_element(elements=("login", "click", "purchase")),
        "age": fake.random_int(min=18, max=65),
        "email": fake.email()
    } for _ in range(100)]

    df_base = (
        pd.DataFrame(data)
        .pipe(add_metadata, batch_id="batch_001", source_filename="batch_1_baseline.csv")
    )

    df_base.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists='replace', index=False)
    print(f"✅ Loaded 100 baseline records into {SCHEMA_NAME}.{TABLE_NAME}.")
    return df_base

# ==========================================
# SCENARIO 2: Replayability Simulation
# ==========================================
def run_scenario_2_replay(df_base: pd.DataFrame):
    print("\n--- Running Scenario 2: Replayability (Duplicates + New) ---")

    business_cols = ["event_id", "user_id", "event_type", "age", "email"]
    df_duplicates = df_base.head(50)[business_cols].copy()

    new_data = [{
        "event_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "event_type": fake.random_element(elements=("login", "click", "purchase")),
        "age": fake.random_int(min=18, max=65),
        "email": fake.email()
    } for _ in range(50)]

    df_replay = (
        pd.concat([df_duplicates, pd.DataFrame(new_data)], ignore_index=True)
        .pipe(add_metadata, batch_id="batch_002_replay", source_filename="batch_2_replay.csv")
    )

    df_replay.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists='append', index=False)
    print(f"✅ Loaded 100 replay records (50 dupes, 50 new) into {SCHEMA_NAME}.{TABLE_NAME}.")

# ==========================================
# SCENARIO 3: Schema Drift (Add/Drop Columns)
# ==========================================
def run_scenario_3_add_drop():
    print("\n--- Running Scenario 3: Schema Drift (Add 'user_status', Drop 'email') ---")
    data = [{
        "event_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "event_type": fake.random_element(elements=("logout", "scroll")),
        "age": fake.random_int(min=18, max=65),
        "user_status": fake.random_element(elements=("active", "inactive"))
    } for _ in range(50)]

    df_drift = (
        pd.DataFrame(data)
        .pipe(add_metadata, batch_id="batch_003_drift", source_filename="batch_3_add_drop.csv")
    )

    handle_schema_evolution(df_drift, TABLE_NAME)
    df_drift.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists='append', index=False)
    print(f"✅ Loaded 50 drifted records into {SCHEMA_NAME}.{TABLE_NAME}.")

# ==========================================
# SCENARIO 4: Schema Drift (Type Evolution)
# ==========================================
def run_scenario_4_type_evolution():
    print("\n--- Running Scenario 4: Type Evolution ('age' int -> string) ---")
    data = [{
        "event_id": str(uuid.uuid4()),
        "user_id": fake.random_int(min=1000, max=9999),
        "event_type": "update_profile",
        "age": "unknown",
        "email": fake.email()
    } for _ in range(50)]

    df_type_drift = (
        pd.DataFrame(data)
        .pipe(add_metadata, batch_id="batch_004_type", source_filename="batch_4_type_evolution.csv")
    )

    target_table = f"{TABLE_NAME}_type_drift"

    print(f"⚠️ Type collision detected! Diverting payload to landing table: {target_table}")
    df_type_drift.to_sql(target_table, engine, schema=SCHEMA_NAME, if_exists='replace', index=False)
    print(f"✅ Loaded 50 type-drifted records into {SCHEMA_NAME}.{target_table}.")

if __name__ == "__main__":
    try:
        setup_schema()
        df_base = run_scenario_1_baseline()
        run_scenario_2_replay(df_base)
        run_scenario_3_add_drop()
        run_scenario_4_type_evolution()
        print("\n🎉 All simulation scenarios completed successfully!")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
