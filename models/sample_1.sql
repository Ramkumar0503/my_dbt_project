-- models/dummy_model.sql
{{ config(materialized='table1AND2') }}

SELECT 
    1 AS id,
    'Sample_model_1' AS name,
    current_timestamp AS created_at
