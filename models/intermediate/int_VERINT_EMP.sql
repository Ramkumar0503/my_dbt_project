{{ config(materialized='table') }}

With EMPLOYEEAMOut as (
	/* SubQuery FROM Source ==>EMPLOYEEAM */
	 /* transType: "Source" */
	 SELECT
	  a.ID AS EMP_ID,
	  a.EMPLOYEETYPEID AS EMP_TYPE_ID,
	  a.ASSIGNEDPOINTS AS ASGN_PNT,
	  a.EMPLOYEENUMBER AS EMP_NBR,
	  a.STARTTIME AS EMP_STRT_DTTM,
	  a.ENDTIME AS EMP_END_DTTM,
	  a.CHANGECOUNTER AS CHG_CNTR,
	  a.MODIFIEDBY AS MOD_BY,
	  a.ISSUPERVISOR AS SPVSR_FLG,
	  a.ISTEAMLEAD AS TEAM_LEAD_FLG,
	  a.PREFERREDSTART AS PREF_STRT_FLG,
	  b.FIRSTNAME AS FRST_NM,
	  b.LASTNAME AS LAST_NM,
	  b.MIDDLEINITIAL AS MDL_INITL,
	  b.SUFFIX AS SUFX,
	  b.BIRTHDATE AS BRTH_DT,
	  c.USERNAME AS USR_NM,
	  c.STATUS AS USR_STS,
	  c.MODIFIEDBY AS PROFILE_MOD_BY,
	  c.LASTMODIFIEDAT AS PROFILE_MOD_DTTM,
	  c.FAILEDLOGINCOUNT AS FAIL_LOGIN_CNT,
	  c.LASTLOGINTIME AS LAST_LOGIN_DTTM
	 FROM
	  {{ ref('stg_EMPLOYEEAMOut') }} AS a
	  JOIN dbo.PERSON AS b ON A.PERSONID = B.ID
	  LEFT JOIN dbo.BPUSER AS c ON A.ID = C.EMPLOYEEID
),

VERINT_EMPOut as (
	/* SubQuery FROM Source ==>VERINT_EMP */
	/* transType: "Source" */
	SELECT
		EMP_ID,
	  SOR_CD,
	  EFF_DTTM,
	  END_DTTM,
	  UNQ_KEY_TXT,
	  EMP_TYPE_ID,
	  ASGN_PNT,
	  EMP_NBR,
	  EMP_STRT_DTTM,
	  EMP_END_DTTM,
	  CHG_CNTR,
	  MOD_BY,
	  SPVSR_FLG,
	  TEAM_LEAD_FLG,
	  PREF_STRT_FLG,
	  FRST_NM,
	  LAST_NM,
	  MDL_INITL,
	  SUFX,
	  BRTH_DT,
	  USR_NM,
	  USR_STS,
	  PROFILE_MOD_BY,
	  PROFILE_MOD_DTTM,
	  FAIL_LOGIN_CNT,
	  LAST_LOGIN_DTTM
	FROM {{ ref('stg_VERINT_EMPOut') }}
	WHERE CURR_IND = 'Y'
),

LKP_CRCOut as (
	/* transType: "Expression" */
	 SELECT
	  EMP_ID AS EMP_ID,
	  SOR_CD AS SOR_CD,
	  EFF_DTTM AS EFF_DTTM,
	  END_DTTM AS END_DTTM,
	  UNQ_KEY_TXT AS UNQ_KEY_TXT,
	  EMP_TYPE_ID AS EMP_TYPE_ID,
	  ASGN_PNT AS ASGN_PNT,
	  EMP_NBR AS EMP_NBR,
	  EMP_STRT_DTTM AS EMP_STRT_DTTM,
	  EMP_END_DTTM AS EMP_END_DTTM,
	  CHG_CNTR AS CHG_CNTR,
	  MOD_BY AS MOD_BY,
	  SPVSR_FLG AS SPVSR_FLG,
	  TEAM_LEAD_FLG AS TEAM_LEAD_FLG,
	  PREF_STRT_FLG AS PREF_STRT_FLG,
	  FRST_NM AS FRST_NM,
	  LAST_NM AS LAST_NM,
	  MDL_INITL AS MDL_INITL,
	  SUFX AS SUFX,
	  BRTH_DT AS BRTH_DT,
	  USR_NM AS USR_NM,
	  USR_STS AS USR_STS,
	  PROFILE_MOD_BY AS PROFILE_MOD_BY,
	  PROFILE_MOD_DTTM AS PROFILE_MOD_DTTM,
	  FAIL_LOGIN_CNT AS FAIL_LOGIN_CNT,
	  LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
	  MD5 (
	   CONCAT (
	    COALESCE (EMP_TYPE_ID:: VARCHAR, ''),
	    '|',
	    COALESCE (ASGN_PNT:: VARCHAR, ''),
	    '|',
	    COALESCE (EMP_NBR:: VARCHAR, ''),
	    '|',
	    COALESCE (EMP_STRT_DTTM:: VARCHAR, ''),
	    '|',
	    COALESCE (EMP_END_DTTM:: VARCHAR, ''),
	    '|',
	    COALESCE (CHG_CNTR:: VARCHAR, ''),
	    '|',
	    COALESCE (MOD_BY:: VARCHAR, ''),
	    '|',
	    COALESCE (SPVSR_FLG:: VARCHAR, ''),
	    '|',
	    COALESCE (TEAM_LEAD_FLG:: VARCHAR, ''),
	    '|',
	    COALESCE (PREF_STRT_FLG:: VARCHAR, ''),
	    '|',
	    COALESCE (FRST_NM:: VARCHAR, ''),
	    '|',
	    COALESCE (LAST_NM:: VARCHAR, ''),
	    '|',
	    COALESCE (MDL_INITL:: VARCHAR, ''),
	    '|',
	    COALESCE (SUFX:: VARCHAR, ''),
	    '|',
	    COALESCE (BRTH_DT:: VARCHAR, ''),
	    '|',
	    COALESCE (USR_NM:: VARCHAR, ''),
	    '|',
	    COALESCE (USR_STS:: VARCHAR, ''),
	    '|',
	    COALESCE (PROFILE_MOD_BY:: VARCHAR, ''),
	    '|',
	    COALESCE (PROFILE_MOD_DTTM:: VARCHAR, ''),
	    '|',
	    COALESCE (FAIL_LOGIN_CNT:: VARCHAR, ''),
	    '|',
	    COALESCE (LAST_LOGIN_DTTM:: VARCHAR, '')
	   )
	  ) AS CRC
	 FROM
	  VERINT_EMPOut AS VERINT_EMPOut
),

tAddCRCRow_3_Lookup_LastMatchOut as (
	/* transType: "Expression" */
	 SELECT
	  DISTINCT EMP_ID,
	  LAST_VALUE (SOR_CD) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS SOR_CD,
	  LAST_VALUE (EFF_DTTM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS EFF_DTTM,
	  LAST_VALUE (END_DTTM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS END_DTTM,
	  LAST_VALUE (UNQ_KEY_TXT) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS UNQ_KEY_TXT,
	  LAST_VALUE (EMP_TYPE_ID) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS EMP_TYPE_ID,
	  LAST_VALUE (ASGN_PNT) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS ASGN_PNT,
	  LAST_VALUE (EMP_NBR) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS EMP_NBR,
	  LAST_VALUE (EMP_STRT_DTTM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS EMP_STRT_DTTM,
	  LAST_VALUE (EMP_END_DTTM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS EMP_END_DTTM,
	  LAST_VALUE (CHG_CNTR) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS CHG_CNTR,
	  LAST_VALUE (MOD_BY) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS MOD_BY,
	  LAST_VALUE (SPVSR_FLG) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS SPVSR_FLG,
	  LAST_VALUE (TEAM_LEAD_FLG) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS TEAM_LEAD_FLG,
	  LAST_VALUE (PREF_STRT_FLG) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS PREF_STRT_FLG,
	  LAST_VALUE (FRST_NM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS FRST_NM,
	  LAST_VALUE (LAST_NM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS LAST_NM,
	  LAST_VALUE (MDL_INITL) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS MDL_INITL,
	  LAST_VALUE (SUFX) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS SUFX,
	  LAST_VALUE (BRTH_DT) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS BRTH_DT,
	  LAST_VALUE (USR_NM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS USR_NM,
	  LAST_VALUE (USR_STS) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS USR_STS,
	  LAST_VALUE (PROFILE_MOD_BY) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS PROFILE_MOD_BY,
	  LAST_VALUE (PROFILE_MOD_DTTM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS PROFILE_MOD_DTTM,
	  LAST_VALUE (FAIL_LOGIN_CNT) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS FAIL_LOGIN_CNT,
	  LAST_VALUE (LAST_LOGIN_DTTM) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS LAST_LOGIN_DTTM,
	  LAST_VALUE (CRC) OVER (
	   PARTITION BY EMP_ID
	   ORDER BY
	    EMP_ID
	  ) AS CRC
	 FROM
	  LKP_CRCOut AS LKP_CRCOut
),

SRC_CRCOut as (
	/* transType: "Expression" */
	 SELECT
	  EMP_ID AS EMP_ID,
	  EMP_TYPE_ID AS EMP_TYPE_ID,
	  ASGN_PNT AS ASGN_PNT,
	  EMP_NBR AS EMP_NBR,
	  EMP_STRT_DTTM AS EMP_STRT_DTTM,
	  EMP_END_DTTM AS EMP_END_DTTM,
	  CHG_CNTR AS CHG_CNTR,
	  MOD_BY AS MOD_BY,
	  SPVSR_FLG AS SPVSR_FLG,
	  TEAM_LEAD_FLG AS TEAM_LEAD_FLG,
	  PREF_STRT_FLG AS PREF_STRT_FLG,
	  FRST_NM AS FRST_NM,
	  LAST_NM AS LAST_NM,
	  MDL_INITL AS MDL_INITL,
	  SUFX AS SUFX,
	  BRTH_DT AS BRTH_DT,
	  USR_NM AS USR_NM,
	  USR_STS AS USR_STS,
	  PROFILE_MOD_BY AS PROFILE_MOD_BY,
	  PROFILE_MOD_DTTM AS PROFILE_MOD_DTTM,
	  FAIL_LOGIN_CNT AS FAIL_LOGIN_CNT,
	  LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
	  MD5 (
	   CONCAT (
	    COALESCE (EMP_TYPE_ID:: VARCHAR, ''),
	    '|',
	    COALESCE (ASGN_PNT:: VARCHAR, ''),
	    '|',
	    COALESCE (EMP_NBR:: VARCHAR, ''),
	    '|',
	    COALESCE (EMP_STRT_DTTM:: VARCHAR, ''),
	    '|',
	    COALESCE (EMP_END_DTTM:: VARCHAR, ''),
	    '|',
	    COALESCE (CHG_CNTR:: VARCHAR, ''),
	    '|',
	    COALESCE (MOD_BY:: VARCHAR, ''),
	    '|',
	    COALESCE (SPVSR_FLG:: VARCHAR, ''),
	    '|',
	    COALESCE (TEAM_LEAD_FLG:: VARCHAR, ''),
	    '|',
	    COALESCE (PREF_STRT_FLG:: VARCHAR, ''),
	    '|',
	    COALESCE (FRST_NM:: VARCHAR, ''),
	    '|',
	    COALESCE (LAST_NM:: VARCHAR, ''),
	    '|',
	    COALESCE (MDL_INITL:: VARCHAR, ''),
	    '|',
	    COALESCE (SUFX:: VARCHAR, ''),
	    '|',
	    COALESCE (BRTH_DT:: VARCHAR, ''),
	    '|',
	    COALESCE (USR_NM:: VARCHAR, ''),
	    '|',
	    COALESCE (USR_STS:: VARCHAR, ''),
	    '|',
	    COALESCE (PROFILE_MOD_BY:: VARCHAR, ''),
	    '|',
	    COALESCE (PROFILE_MOD_DTTM:: VARCHAR, ''),
	    '|',
	    COALESCE (FAIL_LOGIN_CNT:: VARCHAR, ''),
	    '|',
	    COALESCE (LAST_LOGIN_DTTM:: VARCHAR, '')
	   )
	  ) AS CRC
	 FROM
	  EMPLOYEEAMOut AS EMPLOYEEAMOut
),

tMap_1Out as (
	/* transType: "Joiner" */
	 SELECT
	  lnk_src.EMP_NBR AS EMP_NBR,
	  lnk_src.ASGN_PNT AS ASGN_PNT,
	  lnk_src.USR_STS AS USR_STS,
	  lnk_src.EMP_TYPE_ID AS EMP_TYPE_ID,
	  lnk_src.MOD_BY AS MOD_BY,
	  lnk_src.FRST_NM AS FRST_NM,
	  lnk_src.SPVSR_FLG AS SPVSR_FLG,
	  lnk_src.SUFX AS SUFX,
	  lnk_ref.EMP_ID AS EMP_ID_2,
	  lnk_src.EMP_ID AS EMP_ID_1,
	  lnk_src.MDL_INITL AS MDL_INITL,
	  lnk_src.LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
	  lnk_src.FAIL_LOGIN_CNT AS FAIL_LOGIN_CNT,
	  lnk_date.UPDT_END_DTTM AS UPDT_END_DTTM,
	  lnk_date.EFF_DTTM AS EFF_DTTM_1,
	  lnk_src.BRTH_DT AS BRTH_DT,
	  lnk_ref.EFF_DTTM AS EFF_DTTM_2,
	  lnk_src.TEAM_LEAD_FLG AS TEAM_LEAD_FLG,
	  lnk_src.PROFILE_MOD_BY AS PROFILE_MOD_BY,
	  lnk_src.USR_NM AS USR_NM,
	  lnk_src.CRC AS CRC_1,
	  lnk_ref.CRC AS CRC_2,
	  lnk_src.EMP_STRT_DTTM AS EMP_STRT_DTTM,
	  lnk_src.LAST_NM AS LAST_NM,
	  lnk_src.PROFILE_MOD_DTTM AS PROFILE_MOD_DTTM,
	  lnk_src.CHG_CNTR AS CHG_CNTR,
	  lnk_date.AUD_CRE_DTTM AS AUD_CRE_DTTM,
	  lnk_src.EMP_END_DTTM AS EMP_END_DTTM,
	  lnk_src.PREF_STRT_FLG AS PREF_STRT_FLG
	 FROM
	  SRC_CRCOut AS SRC_CRCOut
	  CROSS JOIN {{ ref('stg_stage__CURRENT_TIMESTAMPOut') }} AS lnk_date
	  INNER JOIN tAddCRCRow_3_Lookup_LastMatchOut AS tAddCRCRow_3_Lookup_LastMatchOut ON EMP_ID = EMP_ID
),

Router_tMap_1Out1 as (
	/* transType: "Router" */
	 SELECT
	  tMap_1Out.lnk_src.EMP_ID AS lnk_src.EMP_ID,
	  tMap_1Out.lnk_src.EMP_TYPE_ID AS lnk_src.EMP_TYPE_ID,
	  tMap_1Out.lnk_src.ASGN_PNT AS lnk_src.ASGN_PNT,
	  tMap_1Out.lnk_src.EMP_NBR AS lnk_src.EMP_NBR,
	  tMap_1Out.lnk_src.EMP_STRT_DTTM AS lnk_src.EMP_STRT_DTTM,
	  tMap_1Out.lnk_src.EMP_END_DTTM AS lnk_src.EMP_END_DTTM,
	  tMap_1Out.lnk_src.CHG_CNTR AS lnk_src.CHG_CNTR,
	  tMap_1Out.lnk_src.MOD_BY AS lnk_src.MOD_BY,
	  tMap_1Out.lnk_src.SPVSR_FLG AS lnk_src.SPVSR_FLG,
	  tMap_1Out.lnk_src.TEAM_LEAD_FLG AS lnk_src.TEAM_LEAD_FLG,
	  tMap_1Out.lnk_src.PREF_STRT_FLG AS lnk_src.PREF_STRT_FLG,
	  tMap_1Out.lnk_src.FRST_NM AS lnk_src.FRST_NM,
	  tMap_1Out.lnk_src.LAST_NM AS lnk_src.LAST_NM,
	  tMap_1Out.lnk_src.MDL_INITL AS lnk_src.MDL_INITL,
	  tMap_1Out.lnk_src.SUFX AS lnk_src.SUFX,
	  tMap_1Out.lnk_src.BRTH_DT AS lnk_src.BRTH_DT,
	  tMap_1Out.lnk_src.USR_NM AS lnk_src.USR_NM,
	  tMap_1Out.lnk_src.USR_STS AS lnk_src.USR_STS,
	  tMap_1Out.lnk_src.PROFILE_MOD_BY AS lnk_src.PROFILE_MOD_BY,
	  tMap_1Out.lnk_src.PROFILE_MOD_DTTM AS lnk_src.PROFILE_MOD_DTTM,
	  tMap_1Out.lnk_src.FAIL_LOGIN_CNT AS lnk_src.FAIL_LOGIN_CNT,
	  tMap_1Out.lnk_src.LAST_LOGIN_DTTM AS lnk_src.LAST_LOGIN_DTTM,
	  tMap_1Out.lnk_src.CRC AS lnk_src.CRC,
	  tMap_1Out.lnk_date.UPDT_END_DTTM AS lnk_date.UPDT_END_DTTM,
	  tMap_1Out.lnk_date.EFF_DTTM AS lnk_date.EFF_DTTM,
	  tMap_1Out.lnk_date.AUD_CRE_DTTM AS lnk_date.AUD_CRE_DTTM,
	  tMap_1Out.lnk_ref.EMP_ID AS lnk_ref.EMP_ID,
	  tMap_1Out.lnk_ref.SOR_CD AS lnk_ref.SOR_CD,
	  tMap_1Out.lnk_ref.EFF_DTTM AS lnk_ref.EFF_DTTM,
	  tMap_1Out.lnk_ref.END_DTTM AS lnk_ref.END_DTTM,
	  tMap_1Out.lnk_ref.UNQ_KEY_TXT AS lnk_ref.UNQ_KEY_TXT,
	  tMap_1Out.lnk_ref.EMP_TYPE_ID AS lnk_ref.EMP_TYPE_ID,
	  tMap_1Out.lnk_ref.ASGN_PNT AS lnk_ref.ASGN_PNT,
	  tMap_1Out.lnk_ref.EMP_NBR AS lnk_ref.EMP_NBR,
	  tMap_1Out.lnk_ref.EMP_STRT_DTTM AS lnk_ref.EMP_STRT_DTTM,
	  tMap_1Out.lnk_ref.EMP_END_DTTM AS lnk_ref.EMP_END_DTTM,
	  tMap_1Out.lnk_ref.CHG_CNTR AS lnk_ref.CHG_CNTR,
	  tMap_1Out.lnk_ref.MOD_BY AS lnk_ref.MOD_BY,
	  tMap_1Out.lnk_ref.SPVSR_FLG AS lnk_ref.SPVSR_FLG,
	  tMap_1Out.lnk_ref.TEAM_LEAD_FLG AS lnk_ref.TEAM_LEAD_FLG,
	  tMap_1Out.lnk_ref.PREF_STRT_FLG AS lnk_ref.PREF_STRT_FLG,
	  tMap_1Out.lnk_ref.FRST_NM AS lnk_ref.FRST_NM,
	  tMap_1Out.lnk_ref.LAST_NM AS lnk_ref.LAST_NM,
	  tMap_1Out.lnk_ref.MDL_INITL AS lnk_ref.MDL_INITL,
	  tMap_1Out.lnk_ref.SUFX AS lnk_ref.SUFX,
	  tMap_1Out.lnk_ref.BRTH_DT AS lnk_ref.BRTH_DT,
	  tMap_1Out.lnk_ref.USR_NM AS lnk_ref.USR_NM,
	  tMap_1Out.lnk_ref.USR_STS AS lnk_ref.USR_STS,
	  tMap_1Out.lnk_ref.PROFILE_MOD_BY AS lnk_ref.PROFILE_MOD_BY,
	  tMap_1Out.lnk_ref.PROFILE_MOD_DTTM AS lnk_ref.PROFILE_MOD_DTTM,
	  tMap_1Out.lnk_ref.FAIL_LOGIN_CNT AS lnk_ref.FAIL_LOGIN_CNT,
	  tMap_1Out.lnk_ref.LAST_LOGIN_DTTM AS lnk_ref.LAST_LOGIN_DTTM,
	  tMap_1Out.lnk_ref.CRC AS lnk_ref.CRC
	 FROM
	  tMap_1Out AS tMap_1Out
	 WHERE
	  NOT CRC_1 = CRC_2
),

lnk_updateOut as (
	/* transType: "Expression" */
	 SELECT
	  EMP_ID AS EMP_ID,
	  EFF_DTTM AS EFF_DTTM,
	  UPDT_END_DTTM AS END_DTTM,
	  "ETL-UPDATE" AS AUD_UPD_BY_NM,
	  AUD_CRE_DTTM AS AUD_UPD_DTTM,
	  "N" AS CURR_IND
	 FROM
	  tMap_1Out AS tMap_1Out
	 WHERE
	  ! lnk_src.CRC.equals(lnk_ref.CRC)
)

SELECT *
FROM lnk_updateOut