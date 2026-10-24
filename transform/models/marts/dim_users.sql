{{ config(
    materialized='table',
    unique_key='user_id'
) }}

WITH event_users AS (
    SELECT DISTINCT user_id
    FROM {{ ref('stg_raw_events') }}
    WHERE user_id != -1
),

source_users AS (
    SELECT
        user_id,
        'Auto-Generated User ' || CAST(user_id AS VARCHAR) AS first_name,
        'Active' AS account_status,
        CURRENT_TIMESTAMP AS registered_at
    FROM event_users
),

unknown_user AS (
    -- Using the reusable macro to generate the -1 row
    {{ generate_default_dimension_row({
        'user_id': -1,
        'first_name': "'Unknown User'",
        'account_status': "'Unspecified'",
        'registered_at': "'1900-01-01'::TIMESTAMP"
    }) }}
)

SELECT * FROM source_users
UNION ALL
SELECT * FROM unknown_user