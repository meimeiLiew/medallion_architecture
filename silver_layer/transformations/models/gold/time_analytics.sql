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
        EXTRACT(YEAR FROM start_date) AS start_year,
        EXTRACT(MONTH FROM start_date) AS start_month,
        EXTRACT(YEAR FROM end_date) AS end_year,
        EXTRACT(MONTH FROM end_date) AS end_month,
        DATE_DIFF(end_date, start_date, DAY) AS contract_duration_days
    FROM {{ ref('contracts_silver') }}
),

budgets AS (
    SELECT 
        budget_id,
        contract_id,
        budget_name,
        original_amount,
        actual_cost,
        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,
        EXTRACT(YEAR FROM planned_start_date) AS planned_start_year,
        EXTRACT(MONTH FROM planned_start_date) AS planned_start_month,
        EXTRACT(YEAR FROM actual_start_date) AS actual_start_year,
        EXTRACT(MONTH FROM actual_start_date) AS actual_start_month,
        DATE_DIFF(actual_end_date, actual_start_date, DAY) AS actual_duration_days,
        DATE_DIFF(planned_end_date, planned_start_date, DAY) AS planned_duration_days,
        DATE_DIFF(actual_start_date, planned_start_date, DAY) AS start_delay_days
    FROM {{ ref('budgets_silver') }}
),

change_orders AS (
    SELECT 
        change_order_id,
        contract_id,
        change_order_name,
        change_order_type,
        approved_amount,
        created_at,
        updated_at,
        status_changed_at,
        EXTRACT(YEAR FROM created_at) AS created_year,
        EXTRACT(MONTH FROM created_at) AS created_month
    FROM {{ ref('change_orders_silver') }}
),

-- Monthly contract metrics
monthly_contracts AS (
    SELECT
        start_year,
        start_month,
        COUNT(*) AS new_contracts,
        SUM(contract_value) AS new_contract_value,
        AVG(contract_duration_days) AS avg_contract_duration
    FROM contracts
    GROUP BY start_year, start_month
),

-- Monthly budget metrics
monthly_budgets AS (
    SELECT
        planned_start_year,
        planned_start_month,
        COUNT(*) AS new_budgets,
        SUM(original_amount) AS new_budget_amount,
        AVG(planned_duration_days) AS avg_planned_duration,
        AVG(start_delay_days) AS avg_start_delay
    FROM budgets
    GROUP BY planned_start_year, planned_start_month
),

-- Monthly change order metrics
monthly_change_orders AS (
    SELECT
        created_year,
        created_month,
        COUNT(*) AS new_change_orders,
        SUM(approved_amount) AS new_change_order_amount,
        AVG(approved_amount) AS avg_change_order_amount
    FROM change_orders
    GROUP BY created_year, created_month
),

-- Generate date spine for the last 3 years
date_spine AS (
    SELECT
        year,
        month
    FROM UNNEST(GENERATE_ARRAY(
        EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)),
        EXTRACT(YEAR FROM CURRENT_DATE())
    )) AS year
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, 12)) AS month
    WHERE (year < EXTRACT(YEAR FROM CURRENT_DATE())) OR 
          (year = EXTRACT(YEAR FROM CURRENT_DATE()) AND month <= EXTRACT(MONTH FROM CURRENT_DATE()))
)

SELECT
    ds.year,
    ds.month,
    -- Contract metrics
    COALESCE(mc.new_contracts, 0) AS new_contracts,
    COALESCE(mc.new_contract_value, 0) AS new_contract_value,
    COALESCE(mc.avg_contract_duration, 0) AS avg_contract_duration,
    -- Budget metrics
    COALESCE(mb.new_budgets, 0) AS new_budgets,
    COALESCE(mb.new_budget_amount, 0) AS new_budget_amount,
    COALESCE(mb.avg_planned_duration, 0) AS avg_planned_duration,
    COALESCE(mb.avg_start_delay, 0) AS avg_start_delay,
    -- Change order metrics
    COALESCE(mco.new_change_orders, 0) AS new_change_orders,
    COALESCE(mco.new_change_order_amount, 0) AS new_change_order_amount,
    COALESCE(mco.avg_change_order_amount, 0) AS avg_change_order_amount,
    -- Calculated metrics
    CASE
        WHEN COALESCE(mc.new_contracts, 0) = 0 THEN 0
        ELSE COALESCE(mb.new_budgets, 0) / COALESCE(mc.new_contracts, 1)
    END AS budgets_per_contract,
    CASE
        WHEN COALESCE(mc.new_contracts, 0) = 0 THEN 0
        ELSE COALESCE(mco.new_change_orders, 0) / COALESCE(mc.new_contracts, 1)
    END AS change_orders_per_contract,
    CASE
        WHEN COALESCE(mc.new_contract_value, 0) = 0 THEN 0
        ELSE COALESCE(mco.new_change_order_amount, 0) / COALESCE(mc.new_contract_value, 1) * 100
    END AS change_order_percentage,
    CURRENT_TIMESTAMP() AS processed_at
FROM date_spine ds
LEFT JOIN monthly_contracts mc ON ds.year = mc.start_year AND ds.month = mc.start_month
LEFT JOIN monthly_budgets mb ON ds.year = mb.planned_start_year AND ds.month = mb.planned_start_month
LEFT JOIN monthly_change_orders mco ON ds.year = mco.created_year AND ds.month = mco.created_month
ORDER BY ds.year, ds.month 