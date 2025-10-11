-- models/dummy_model.sql
{{ config(materialized='view') }}

-- This is a dummy dbt model that runs without any source tables.
-- It simply returns static data.

SELECT 
    1 AS id,
    'Sample_2' AS name,
    current_timestamp AS created_at
