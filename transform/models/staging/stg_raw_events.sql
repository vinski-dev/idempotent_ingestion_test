{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

WITH raw_data AS (
    SELECT
        event_id,
        event_type,
        age,
        location,

        COALESCE(user_id, -1) AS user_id,
        COALESCE(email_hash, 'UNSPECIFIED_HASH') AS email_hash,

        filehash,
        batch_id,
        _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY filehash
            ORDER BY _loaded_at DESC
        ) as dedupe_rn
    FROM {{ source('analytics', 'raw_events') }}

    {% if is_incremental() %}
    -- 4. Incremental Processing: Only scan records loaded in the last 3 days
    WHERE _loaded_at >= (SELECT MAX(_loaded_at) - INTERVAL '3 days' FROM {{ this }})
    {% endif %}
)

SELECT
    event_id,
    event_type,
    age,
    location,
    user_id,
    email_hash,
    filehash,
    batch_id,
    _loaded_at
FROM raw_data
WHERE dedupe_rn = 1