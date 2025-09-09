{{
	config(
		materialized='incremental',
		alias='EXP_G20_SWP_VALUATIONSOut',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append'
	)
}}
Select
	'LEI_' || LPAD('abc', 5, '0') as O_LEGAL_ENTITY_IDENTIFIER,
    PARTY_CODE_DISPLAY,
    PARTY_CODE_PREFIX_ALT_DISPLAY,
    LENGTH(UTI) AS UTI_LENGTH,
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