{{
	config(
		materialized='incremental',
		alias='TEMP_DIR+VERINT_ACTV_MAP_INS',
		schema='tFileOutputDelimited_1',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='overwrite')
}}

Select 
	ACTV_MAP_ID as ACTV_MAP_ID,
	SOR_CD as SOR_CD,
	EFF_DTTM as EFF_DTTM,
	END_DTTM as END_DTTM,
	UNQ_KEY_TXT as UNQ_KEY_TXT,
	ACTV_ID as ACTV_ID,
	MAPPED_ACTV_ID as MAPPED_ACTV_ID,
	MOD_BY as MOD_BY,
	AUD_CRE_BY_NM as AUD_CRE_BY_NM,
	AUD_CRE_DTTM as AUD_CRE_DTTM,
	ETL_BATCH_ID as ETL_BATCH_ID,
	CURR_IND as CURR_IND 
FROM {{ ref('int_context.TEMP_DIR+VERINT_ACTV_MAP_INS') }} AS lnk_insertOut