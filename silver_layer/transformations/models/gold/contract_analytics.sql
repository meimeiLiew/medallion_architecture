{{ config(
    materialized='table',
    schema='gold'
) }}

WITH contract_data AS (
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
)

SELECT
    client_name,
    COUNT(contract_id) AS total_contracts,
    SUM(contract_value) AS total_contract_value,
    AVG(contract_value) AS avg_contract_value,
    MIN(contract_value) AS min_contract_value,
    MAX(contract_value) AS max_contract_value,
    AVG(contract_duration_days) AS avg_contract_duration_days,
    CURRENT_TIMESTAMP() AS processed_at
FROM contract_data
GROUP BY client_name
ORDER BY total_contract_value DESC 