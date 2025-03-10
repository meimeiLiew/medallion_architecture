{{ config(
    materialized='table',
    schema='gold'
) }}

WITH contracts AS (
    SELECT 
        contract_id,
        contract_name,
        client_name,
        start_date,
        end_date,
        contract_value,
        contract_status,
        DATE_DIFF(end_date, start_date, DAY) AS contract_duration_days
    FROM {{ ref('contracts_silver') }}
),

budgets AS (
    SELECT 
        budget_id,
        contract_id,
        budget_name,
        original_amount,
        actual_cost
    FROM {{ ref('budgets_silver') }}
),

change_orders AS (
    SELECT 
        change_order_id,
        contract_id,
        change_order_name,
        change_order_type,
        approved_amount
    FROM {{ ref('change_orders_silver') }}
),

client_contracts AS (
    SELECT
        client_name,
        COUNT(*) AS total_contracts,
        SUM(contract_value) AS total_contract_value,
        AVG(contract_value) AS avg_contract_value,
        MIN(contract_value) AS min_contract_value,
        MAX(contract_value) AS max_contract_value,
        AVG(contract_duration_days) AS avg_contract_duration_days,
        COUNT(CASE WHEN contract_status = 'active' THEN 1 END) AS active_contracts,
        COUNT(CASE WHEN contract_status = 'completed' THEN 1 END) AS completed_contracts,
        COUNT(CASE WHEN contract_status = 'pending' THEN 1 END) AS pending_contracts,
        COUNT(CASE WHEN contract_status = 'cancelled' THEN 1 END) AS cancelled_contracts
    FROM contracts
    GROUP BY client_name
),

client_budgets AS (
    SELECT
        c.client_name,
        COUNT(DISTINCT b.budget_id) AS total_budgets,
        SUM(b.original_amount) AS total_budget_amount,
        SUM(b.actual_cost) AS total_actual_cost,
        SUM(b.actual_cost) - SUM(b.original_amount) AS total_budget_variance,
        CASE
            WHEN SUM(b.original_amount) = 0 THEN 0
            ELSE (SUM(b.actual_cost) / SUM(b.original_amount)) * 100
        END AS budget_utilization_percentage
    FROM contracts c
    LEFT JOIN budgets b ON c.contract_id = b.contract_id
    GROUP BY c.client_name
),

client_change_orders AS (
    SELECT
        c.client_name,
        COUNT(DISTINCT co.change_order_id) AS total_change_orders,
        SUM(co.approved_amount) AS total_change_order_amount,
        CASE
            WHEN SUM(c.contract_value) = 0 THEN 0
            ELSE (SUM(co.approved_amount) / SUM(c.contract_value)) * 100
        END AS change_order_percentage
    FROM contracts c
    LEFT JOIN change_orders co ON c.contract_id = co.contract_id
    GROUP BY c.client_name
)

SELECT
    cc.client_name,
    -- Contract metrics
    cc.total_contracts,
    cc.total_contract_value,
    cc.avg_contract_value,
    cc.min_contract_value,
    cc.max_contract_value,
    cc.avg_contract_duration_days,
    cc.active_contracts,
    cc.completed_contracts,
    cc.pending_contracts,
    cc.cancelled_contracts,
    -- Budget metrics
    cb.total_budgets,
    cb.total_budget_amount,
    cb.total_actual_cost,
    cb.total_budget_variance,
    cb.budget_utilization_percentage,
    -- Change order metrics
    cco.total_change_orders,
    cco.total_change_order_amount,
    cco.change_order_percentage,
    -- Calculated metrics
    cc.total_contract_value + COALESCE(cco.total_change_order_amount, 0) AS adjusted_contract_value,
    CASE
        WHEN cc.total_contracts = 0 THEN 0
        ELSE cb.total_budgets / cc.total_contracts
    END AS avg_budgets_per_contract,
    CASE
        WHEN cc.total_contracts = 0 THEN 0
        ELSE cco.total_change_orders / cc.total_contracts
    END AS avg_change_orders_per_contract,
    CURRENT_TIMESTAMP() AS processed_at
FROM client_contracts cc
LEFT JOIN client_budgets cb ON cc.client_name = cb.client_name
LEFT JOIN client_change_orders cco ON cc.client_name = cco.client_name 