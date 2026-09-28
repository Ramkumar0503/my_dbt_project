{{ config(materialized='table') }}

With tUnite_1Out as (
	/* transType: "UNION" */
	 (
	  SELECT
	   tFileInputDelimited_2Out.ACTV_MAP_ID,
	   tFileInputDelimited_2Out.SOR_CD,
	   tFileInputDelimited_2Out.EFF_DTTM,
	   tFileInputDelimited_2Out.END_DTTM,
	   tFileInputDelimited_2Out.UNQ_KEY_TXT,
	   tFileInputDelimited_2Out.ACTV_ID,
	   tFileInputDelimited_2Out.MAPPED_ACTV_ID,
	   tFileInputDelimited_2Out.MOD_BY,
	   tFileInputDelimited_2Out.AUD_CRE_BY_NM,
	   tFileInputDelimited_2Out.AUD_CRE_DTTM,
	   tFileInputDelimited_2Out.ETL_BATCH_ID,
	   tFileInputDelimited_2Out.CURR_IND
	  FROM
	   {{ ref('stg_tFileInputDelimited_2__context.TEMP_DIR+VERINT_ACTV_MAP_UPD_INS') }} AS tFileInputDelimited_2Out
	 )
	 UNION ALL
	 (
	  SELECT
	   tFileInputDelimited_3Out.ACTV_MAP_ID,
	   tFileInputDelimited_3Out.SOR_CD,
	   tFileInputDelimited_3Out.EFF_DTTM,
	   tFileInputDelimited_3Out.END_DTTM,
	   tFileInputDelimited_3Out.UNQ_KEY_TXT,
	   tFileInputDelimited_3Out.ACTV_ID,
	   tFileInputDelimited_3Out.MAPPED_ACTV_ID,
	   tFileInputDelimited_3Out.MOD_BY,
	   tFileInputDelimited_3Out.AUD_CRE_BY_NM,
	   tFileInputDelimited_3Out.AUD_CRE_DTTM,
	   tFileInputDelimited_3Out.ETL_BATCH_ID,
	   tFileInputDelimited_3Out.CURR_IND
	  FROM
	   {{ ref('stg_tFileInputDelimited_3__context.TEMP_DIR+VERINT_ACTV_MAP_INS') }} AS tFileInputDelimited_3Out
	 )
),

tMap_2Out as (
	/* transType: "Expression" */
	 SELECT
	  row6.ACTV_MAP_ID AS ACTV_MAP_ID,
	  row6.SOR_CD AS SOR_CD,
	  row6.EFF_DTTM AS EFF_DTTM,
	  row6.END_DTTM AS END_DTTM,
	  row6.UNQ_KEY_TXT AS UNQ_KEY_TXT,
	  row6.ACTV_ID AS ACTV_ID,
	  row6.MAPPED_ACTV_ID AS MAPPED_ACTV_ID,
	  NULLIF (row6.MOD_BY) AS MOD_BY,
	  row6.AUD_CRE_BY_NM AS AUD_CRE_BY_NM,
	  row6.AUD_CRE_DTTM AS AUD_CRE_DTTM,
	  row6.ETL_BATCH_ID AS ETL_BATCH_ID,
	  row6.CURR_IND AS CURR_IND
	 FROM
	  tUnite_1Out AS tUnite_1Out
)

SELECT *
FROM tMap_2Out