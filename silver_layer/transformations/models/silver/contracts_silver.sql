{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_contracts AS (
    SELECT 
        contract_id,
        contract_name,
        client_name,
        CAST(start_date AS DATE) AS start_date,
        CAST(end_date AS DATE) AS end_date,
        CAST(contract_value AS NUMERIC) AS contract_value,
        contract_status
    FROM {{ source('bronze', 'contracts') }}
)

SELECT
    contract_id,
    contract_name,
    client_name,
    start_date,
    end_date,
    contract_value,
    contract_status,
    CURRENT_TIMESTAMP() AS processed_at
FROM source_contracts 