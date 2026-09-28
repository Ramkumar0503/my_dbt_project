{{ config(materialized='view') }}

/* transType: "Source" */
SELECT ACTV_MAP_ID, 
		SOR_CD, 
		EFF_DTTM, 
		END_DTTM, 
		UNQ_KEY_TXT, 
		ACTV_ID, 
		MAPPED_ACTV_ID, 
		MOD_BY, 
		AUD_CRE_BY_NM, 
		AUD_CRE_DTTM, 
		ETL_BATCH_ID, 
		CURR_IND
 FROM 
{{ source ('tFileInputDelimited_2','{{ TEMP_DIR }}+VERINT_ACTV_MAP_UPD_INS') }}