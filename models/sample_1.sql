{{ config(
    materialized='incremental',
    unique_key='_fivetran_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with demo as (
    select 
        _fivetran_id,
        NPV as demo_amount,
        VALUATION_TIMESTAMP as demo_date
    from {{ source('snowflake_source_demo_schema', 'EXP_G20_SWP_VALUATIONSOUT') }}
),

abc as (
    select 
        _fivetran_id,
        null as abc_amount,  -- No NPV in this table, placeholder
        START_TIME as abc_date
    from {{ source('snowflake_source_2_abc', 'EXP_G20_SWP_VALUATIONSOUT_301') }}
)

select 
    d._fivetran_id,
    d.demo_amount,
    a.abc_amount,
    (coalesce(a.abc_amount, 0) - coalesce(d.demo_amount, 0)) as amount_diff,
    coalesce(a.abc_date, d.demo_date) as final_date
from demo d
left join abc a 
    on d._fivetran_id = a._fivetran_id

{% if is_incremental() %}
  where coalesce(a.abc_date, d.demo_date) > (select max(final_date) from {{ this }})
{% endif %}
