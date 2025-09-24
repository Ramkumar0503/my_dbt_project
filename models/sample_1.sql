{{ config(
    materialized='table',
    alias='EXP_G20_SWP_VALUATIONSOUT_FINAL',
    schema='ANALYTICS'  -- <<-- replace with target schema
) }}

WITH base_data AS (
    SELECT
        'LEI_' || LPAD('abc', 5, '0') AS O_LEGAL_ENTITY_IDENTIFIER,
        LENGTH(PARTY_CODE_DISPLAY) AS CODE_DISPLAY,
        PARTY_CODE_PREFIX_ALT_DISPLAY,
        UTI,
        O_UPI,
        NPV,
        LENGTH(CURRENCY_CODE::STRING) AS CURRENCY_CODE_LEN,
        VALUATION_METHOD,
        VALUATION_TIMESTAMP,
        ACTION_TYPE,
        PROPRIETARY_UTI_INDICATOR
    FROM {{ source('FIVETRAN_DATABASE', 'EXP_G20_SWP_VALUATIONSOUT') }}
),

session_data AS (
    SELECT
        SESSION_ID,
        USER_ID,
        DEVICE,
        START_TIME,
        END_TIME,
        SUCCESS,
        IP_ADDRESS
    FROM {{ source('FIVETRAN_DATABASE_2', 'EXP_G20_SWP_VALUATIONSOUT_301') }}
),

summary AS (
    SELECT
        b.O_LEGAL_ENTITY_IDENTIFIER,
        COUNT(*) AS TOTAL_RECORDS,
        SUM(b.NPV) AS TOTAL_NPV,
        MAX(b.VALUATION_TIMESTAMP) AS LAST_VALUATION_TS,
        COUNT(DISTINCT s.USER_ID) AS UNIQUE_USERS,
        COUNT(DISTINCT s.SESSION_ID) AS TOTAL_SESSIONS
    FROM base_data b
    LEFT JOIN session_data s
        ON b.O_UPI = s.USER_ID  -- 🔗 example join key, adjust if different
    GROUP BY b.O_LEGAL_ENTITY_IDENTIFIER
)

SELECT * 
FROM summary;
