{{
	config(
		materialized='incremental',
		alias='TEST_PD_CONN.LOCATION',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append'
	)
}}
Select
	PROFILE_ID,
    _FIVETRAN_SYNCED,
    CITY,
    COUNTRY,
    STREET,
    NPV,
    LENGTH(_FIVETRAN_DELETED) as _FIVETRAN_DELETED,
    STATE,
    POSTCODE
from
	FIVETRAN_DATABASE.TEST_PD_CONN.LOCATION
