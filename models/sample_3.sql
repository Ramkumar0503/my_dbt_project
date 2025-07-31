{{ config(materialized='view', alias='EXP_G20_SWP_VALUATIONSOut_2') }}

Select
	'LEI_' || LPAD('abc', 5, '0') as O_LEGAL_ENTITY_IDENTIFIER,
    PARTY_CODE_DISPLAY,
    PARTY_CODE_PREFIX_ALT_DISPLAY,
    UTI,
    o_UPI,
    NPV,
    LENGTH(CURRENCY_CODE) as CURRENCY_CODE,
    VALUATION_METHOD,
	LENGTH(VALUATION_METHOD)  as Length_val_method
    VALUATION_TIMESTAMP,
    ACTION_TYPE,
    Proprietary_UTI_indicator
from
	EXP_G20_SWP_VALUATIONSOut
