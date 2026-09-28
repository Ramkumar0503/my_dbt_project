{{ config(materialized='table') }}

With tUnite_1Out as (
	/* transType: "UNION" */
	 (
	  SELECT
	   tFileInputDelimited_2Out.EMP_ID,
	   tFileInputDelimited_2Out.SOR_CD,
	   tFileInputDelimited_2Out.EFF_DTTM,
	   tFileInputDelimited_2Out.END_DTTM,
	   tFileInputDelimited_2Out.UNQ_KEY_TXT,
	   tFileInputDelimited_2Out.EMP_TYPE_ID,
	   tFileInputDelimited_2Out.ASGN_PNT,
	   tFileInputDelimited_2Out.EMP_NBR,
	   tFileInputDelimited_2Out.EMP_STRT_DTTM,
	   tFileInputDelimited_2Out.EMP_END_DTTM,
	   tFileInputDelimited_2Out.CHG_CNTR,
	   tFileInputDelimited_2Out.MOD_BY,
	   tFileInputDelimited_2Out.SPVSR_FLG,
	   tFileInputDelimited_2Out.TEAM_LEAD_FLG,
	   tFileInputDelimited_2Out.PREF_STRT_FLG,
	   tFileInputDelimited_2Out.FRST_NM,
	   tFileInputDelimited_2Out.LAST_NM,
	   tFileInputDelimited_2Out.MDL_INITL,
	   tFileInputDelimited_2Out.SUFX,
	   tFileInputDelimited_2Out.BRTH_DT,
	   tFileInputDelimited_2Out.USR_NM,
	   tFileInputDelimited_2Out.USR_STS,
	   tFileInputDelimited_2Out.PROFILE_MOD_BY,
	   tFileInputDelimited_2Out.PROFILE_MOD_DTTM,
	   tFileInputDelimited_2Out.FAIL_LOGIN_CNT,
	   tFileInputDelimited_2Out.LAST_LOGIN_DTTM,
	   tFileInputDelimited_2Out.AUD_CRE_BY_NM,
	   tFileInputDelimited_2Out.AUD_CRE_DTTM,
	   tFileInputDelimited_2Out.CURR_IND,
	   tFileInputDelimited_2Out.ETL_BATCH_ID
	  FROM
	   {{ ref('stg_tFileInputDelimited_2__context.TEMP_DIR+VERINT_EMP_UPD_INS') }} AS tFileInputDelimited_2Out
	 )
	 UNION ALL
	 (
	  SELECT
	   tFileInputDelimited_3Out.EMP_ID,
	   tFileInputDelimited_3Out.SOR_CD,
	   tFileInputDelimited_3Out.EFF_DTTM,
	   tFileInputDelimited_3Out.END_DTTM,
	   tFileInputDelimited_3Out.UNQ_KEY_TXT,
	   tFileInputDelimited_3Out.EMP_TYPE_ID,
	   tFileInputDelimited_3Out.ASGN_PNT,
	   tFileInputDelimited_3Out.EMP_NBR,
	   tFileInputDelimited_3Out.EMP_STRT_DTTM,
	   tFileInputDelimited_3Out.EMP_END_DTTM,
	   tFileInputDelimited_3Out.CHG_CNTR,
	   tFileInputDelimited_3Out.MOD_BY,
	   tFileInputDelimited_3Out.SPVSR_FLG,
	   tFileInputDelimited_3Out.TEAM_LEAD_FLG,
	   tFileInputDelimited_3Out.PREF_STRT_FLG,
	   tFileInputDelimited_3Out.FRST_NM,
	   tFileInputDelimited_3Out.LAST_NM,
	   tFileInputDelimited_3Out.MDL_INITL,
	   tFileInputDelimited_3Out.SUFX,
	   tFileInputDelimited_3Out.BRTH_DT,
	   tFileInputDelimited_3Out.USR_NM,
	   tFileInputDelimited_3Out.USR_STS,
	   tFileInputDelimited_3Out.PROFILE_MOD_BY,
	   tFileInputDelimited_3Out.PROFILE_MOD_DTTM,
	   tFileInputDelimited_3Out.FAIL_LOGIN_CNT,
	   tFileInputDelimited_3Out.LAST_LOGIN_DTTM,
	   tFileInputDelimited_3Out.AUD_CRE_BY_NM,
	   tFileInputDelimited_3Out.AUD_CRE_DTTM,
	   tFileInputDelimited_3Out.CURR_IND,
	   tFileInputDelimited_3Out.ETL_BATCH_ID
	  FROM
	   {{ ref('stg_tFileInputDelimited_3__context.TEMP_DIR+VERINT_EMP_INS') }} AS tFileInputDelimited_3Out
	 )
),

tMap_2Out as (
	/* transType: "Expression" */
	 SELECT
	  row6.EMP_ID AS EMP_ID,
	  row6.SOR_CD AS SOR_CD,
	  row6.EFF_DTTM AS EFF_DTTM,
	  row6.END_DTTM AS END_DTTM,
	  row6.UNQ_KEY_TXT AS UNQ_KEY_TXT,
	  row6.EMP_TYPE_ID AS EMP_TYPE_ID,
	  row6.ASGN_PNT AS ASGN_PNT,
	  NULLIF (row6.EMP_NBR) AS EMP_NBR,
	  row6.EMP_STRT_DTTM AS EMP_STRT_DTTM,
	  row6.EMP_END_DTTM AS EMP_END_DTTM,
	  row6.CHG_CNTR AS CHG_CNTR,
	  NULLIF (row6.MOD_BY) AS MOD_BY,
	  NULLIF (row6.SPVSR_FLG) AS SPVSR_FLG,
	  NULLIF (row6.TEAM_LEAD_FLG) AS TEAM_LEAD_FLG,
	  NULLIF (row6.PREF_STRT_FLG) AS PREF_STRT_FLG,
	  NULLIF (row6.FRST_NM) AS FRST_NM,
	  NULLIF (row6.LAST_NM) AS LAST_NM,
	  NULLIF (row6.MDL_INITL) AS MDL_INITL,
	  NULLIF (row6.SUFX) AS SUFX,
	  row6.BRTH_DT AS BRTH_DT,
	  NULLIF (row6.USR_NM) AS USR_NM,
	  row6.USR_STS AS USR_STS,
	  NULLIF (row6.PROFILE_MOD_BY) AS PROFILE_MOD_BY,
	  row6.PROFILE_MOD_DTTM AS PROFILE_MOD_DTTM,
	  row6.FAIL_LOGIN_CNT AS FAIL_LOGIN_CNT,
	  row6.LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
	  row6.AUD_CRE_BY_NM AS AUD_CRE_BY_NM,
	  row6.AUD_CRE_DTTM AS AUD_CRE_DTTM,
	  row6.CURR_IND AS CURR_IND,
	  row6.ETL_BATCH_ID AS ETL_BATCH_ID
	 FROM
	  tUnite_1Out AS tUnite_1Out
)

SELECT *
FROM tMap_2Out