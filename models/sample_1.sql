{{ config(
    materialized='incremental',
    unique_key='valuation_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with demo as (
    select 
        valuation_id,
        amount as demo_amount,
        valuation_date as demo_date
    from {{ source('snowflake_source_demo_schema', 'EXP_G20_SWP_VALUATIONSOUT') }}
),

abc as (
    select 
        valuation_id,
        amount as abc_amount,
        valuation_date as abc_date
    from {{ source('snowflake_source_2_abc', 'EXP_G20_SWP_VALUATIONSOUT_301') }}
)

select 
    d.valuation_id,
    d.demo_amount,
    a.abc_amount,
    -- expression logic: difference between amounts
    (a.abc_amount - d.demo_amount) as amount_diff,
    coalesce(a.abc_date, d.demo_date) as final_date
from demo d
left join abc a 
    on d.valuation_id = a.valuation_id

{% if is_incremental() %}
  where coalesce(a.abc_date, d.demo_date) > (select max(final_date) from {{ this }})
{% endif %}
