{{ config(materialized='view', alias='EXP_G20_SWP_VALUATIONSOut_1') }}
Select
	'LEI_' || LPAD('abc', 5, '0') as O_LEGAL_ENTITY_IDENTIFIER,
    LENGTH(PARTY_CODE_DISPLAY) as CODE_DISPLAY,
    PARTY_CODE_PREFIX_ALT_DISPLAY,
    UTI,
    o_UPI,
    NPV,
    LENGTH(CURRENCY_CODE) as CURRENCY_CODE,
    VALUATION_METHOD,
    VALUATION_TIMESTAMP,
    ACTION_TYPE,
    Proprietary_UTI_indicator
from
	EXP_G20_SWP_VALUATIONSOut
