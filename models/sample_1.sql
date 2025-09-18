{{ config(
    materialized='table',
    alias='EXP_G20_SWP_VALUATIONSOUT_FINAL',
    schema=''
) }}

WITH base_data AS (
    SELECT
        'LEI_' || LPAD('abc', 5, '0') AS O_LEGAL_ENTITY_IDENTIFIER,
        LENGTH(PARTY_CODE_DISPLAY) AS CODE_DISPLAY,
        PARTY_CODE_PREFIX_ALT_DISPLAY,
        UTI,
        o_UPI,
        NPV,
        LENGTH(CURRENCY_CODE) AS CURRENCY_CODE,
        VALUATION_METHOD,
        VALUATION_TIMESTAMP,
        ACTION_TYPE,
        Proprietary_UTI_indicator
    FROM EXP_G20_SWP_VALUATIONSOut
),

summary AS (
    SELECT
        O_LEGAL_ENTITY_IDENTIFIER,
        COUNT(*) AS TOTAL_RECORDS,
        SUM(NPV) AS TOTAL_NPV,
        MAX(VALUATION_TIMESTAMP) AS LAST_VALUATION_TS
    FROM base_data
    GROUP BY O_LEGAL_ENTITY_IDENTIFIER
)

SELECT * FROM summary;
