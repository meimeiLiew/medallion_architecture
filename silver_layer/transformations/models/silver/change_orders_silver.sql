{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_change_orders AS (
    SELECT 
        id as change_order_id,
        number as change_order_number,
        name as change_order_name,
        scope as change_order_scope,
        type as change_order_type,
        contractId as contract_id,
        SAFE_CAST(estimated AS NUMERIC) AS estimated_amount,
        SAFE_CAST(proposed AS NUMERIC) AS proposed_amount,
        SAFE_CAST(submitted AS NUMERIC) AS submitted_amount,
        SAFE_CAST(approved AS NUMERIC) AS approved_amount,
        SAFE_CAST(committed AS NUMERIC) AS committed_amount,
        SAFE_CAST(TRIM(createdAt) AS TIMESTAMP) AS created_at,
        SAFE_CAST(TRIM(updatedAt) AS TIMESTAMP) AS updated_at,
        SAFE_CAST(TRIM(statusChangedAt) AS TIMESTAMP) AS status_changed_at
    FROM {{ source('bronze', 'co') }}
)

SELECT
    change_order_id,
    change_order_number,
    change_order_name,
    change_order_scope,
    change_order_type,
    contract_id,
    estimated_amount,
    proposed_amount,
    submitted_amount,
    approved_amount,
    committed_amount,
    created_at,
    updated_at,
    status_changed_at,
    CURRENT_TIMESTAMP() AS processed_at
FROM source_change_orders 