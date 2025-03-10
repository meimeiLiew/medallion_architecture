{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_budgets AS (
    SELECT 
        id as budget_id,
        name as budget_name,
        contractId as contract_id,
        code as budget_code,
        scope as budget_scope,
        SAFE_CAST(plannedStartDate AS DATE) AS planned_start_date,
        SAFE_CAST(plannedEndDate AS DATE) AS planned_end_date,
        SAFE_CAST(actualStartDate AS DATE) AS actual_start_date,
        SAFE_CAST(actualEndDate AS DATE) AS actual_end_date,
        SAFE_CAST(originalAmount AS NUMERIC) AS original_amount,
        SAFE_CAST(actualCost AS NUMERIC) AS actual_cost,
        SAFE_CAST(TRIM(createdAt) AS TIMESTAMP) AS created_at,
        SAFE_CAST(TRIM(updatedAt) AS TIMESTAMP) AS updated_at
    FROM {{ source('bronze', 'budgets') }}
)

SELECT
    budget_id,
    budget_name,
    contract_id,
    budget_code,
    budget_scope,
    planned_start_date,
    planned_end_date,
    actual_start_date,
    actual_end_date,
    original_amount,
    actual_cost,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() AS processed_at
FROM source_budgets 