{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

WITH silver_contracts AS (
    SELECT *
    FROM {{ ref('contracts_silver') }}
),

silver_budgets AS (
    SELECT *
    FROM {{ ref('budgets_silver') }}
),

contract_metrics AS (
    SELECT
        c.client_name,
        COUNT(DISTINCT c.contract_id) AS total_contracts,
        SUM(c.contract_value) AS total_contract_value,
        AVG(c.contract_value) AS avg_contract_value,
        MIN(c.start_date) AS earliest_contract_date,
        MAX(c.end_date) AS latest_contract_end_date,
        SUM(b.original_amount) AS total_budget_allocated
    FROM silver_contracts c
    LEFT JOIN silver_budgets b ON c.contract_id = b.contract_id
    GROUP BY c.client_name
)

SELECT
    client_name,
    total_contracts,
    total_contract_value,
    avg_contract_value,
    earliest_contract_date,
    latest_contract_end_date,
    total_budget_allocated,
    (total_contract_value - total_budget_allocated) AS budget_variance,
    CURRENT_TIMESTAMP() AS generated_at
FROM contract_metrics
ORDER BY total_contract_value DESC 