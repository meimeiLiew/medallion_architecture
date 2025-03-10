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
        contract_status
    FROM {{ ref('contracts_silver') }}
),

budgets AS (
    SELECT 
        budget_id,
        contract_id,
        budget_name,
        budget_code,
        budget_scope,
        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,
        original_amount,
        actual_cost
    FROM {{ ref('budgets_silver') }}
),

change_orders AS (
    SELECT 
        change_order_id,
        contract_id,
        change_order_name,
        change_order_scope,
        change_order_type,
        estimated_amount,
        approved_amount,
        committed_amount
    FROM {{ ref('change_orders_silver') }}
),

budget_summary AS (
    SELECT
        contract_id,
        COUNT(*) AS budget_count,
        SUM(original_amount) AS total_original_budget,
        SUM(actual_cost) AS total_actual_cost,
        SUM(actual_cost) - SUM(original_amount) AS budget_variance
    FROM budgets
    GROUP BY contract_id
),

change_order_summary AS (
    SELECT
        contract_id,
        COUNT(*) AS change_order_count,
        SUM(estimated_amount) AS total_estimated_amount,
        SUM(approved_amount) AS total_approved_amount,
        SUM(committed_amount) AS total_committed_amount
    FROM change_orders
    GROUP BY contract_id
)

SELECT
    c.contract_id,
    c.contract_name,
    c.client_name,
    c.start_date,
    c.end_date,
    c.contract_value,
    c.contract_status,
    -- Budget metrics
    COALESCE(bs.budget_count, 0) AS budget_count,
    COALESCE(bs.total_original_budget, 0) AS total_original_budget,
    COALESCE(bs.total_actual_cost, 0) AS total_actual_cost,
    COALESCE(bs.budget_variance, 0) AS budget_variance,
    -- Change order metrics
    COALESCE(cos.change_order_count, 0) AS change_order_count,
    COALESCE(cos.total_estimated_amount, 0) AS total_estimated_co_amount,
    COALESCE(cos.total_approved_amount, 0) AS total_approved_co_amount,
    COALESCE(cos.total_committed_amount, 0) AS total_committed_co_amount,
    -- Calculated metrics
    c.contract_value + COALESCE(cos.total_approved_amount, 0) AS adjusted_contract_value,
    CASE
        WHEN c.contract_value = 0 THEN 0
        ELSE (COALESCE(cos.total_approved_amount, 0) / c.contract_value) * 100
    END AS change_order_percentage,
    CASE
        WHEN COALESCE(bs.total_original_budget, 0) = 0 THEN 0
        ELSE (COALESCE(bs.total_actual_cost, 0) / COALESCE(bs.total_original_budget, 0)) * 100
    END AS budget_utilization_percentage,
    CURRENT_TIMESTAMP() AS processed_at
FROM contracts c
LEFT JOIN budget_summary bs ON c.contract_id = bs.contract_id
LEFT JOIN change_order_summary cos ON c.contract_id = cos.contract_id 