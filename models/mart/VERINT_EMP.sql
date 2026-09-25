{{
  config(
    materialized='incremental',
    alias='VERINT_EMP',
    schema='',
    pre_hook="",
    post_hook="",
    incremental_strategy='merge',
    unique_key=['EMP_ID','EFF_DTTM'],
    merge_update_columns=['END_DTTM','AUD_UPD_BY_NM','AUD_UPD_DTTM','CURR_IND']
  )
}}

Select 
	EMP_ID as EMP_ID,
	EFF_DTTM as EFF_DTTM,
	END_DTTM as END_DTTM,
	AUD_UPD_BY_NM as AUD_UPD_BY_NM,
	AUD_UPD_DTTM as AUD_UPD_DTTM,
	CURR_IND as CURR_IND 
FROM {{ ref('int_VERINT_EMP') }} AS lnk_updateOut