{{ config(materialized='table') }}

With VERINT_ACTV_MAPOut as (
	/* SubQuery FROM Source ==>VERINT_ACTV_MAP */
	/* transType: "Source" */
	SELECT
		ACTV_MAP_ID,
	  EFF_DTTM,
	  ACTV_ID,
	  MAPPED_ACTV_ID,
	  MOD_BY
	FROM {{ ref('stg_VERINT_ACTV_MAPOut') }}
	WHERE CURR_IND = 'Y'
),

LKP_CRCOut as (
	/* transType: "Expression" */
	 SELECT
	  ACTV_MAP_ID AS ACTV_MAP_ID,
	  EFF_DTTM AS EFF_DTTM,
	  ACTV_ID AS ACTV_ID,
	  MAPPED_ACTV_ID AS MAPPED_ACTV_ID,
	  MOD_BY AS MOD_BY,
	  MD5 (
	   CONCAT (
	    COALESCE (ACTV_ID:: VARCHAR, ''),
	    '|',
	    COALESCE (MAPPED_ACTV_ID:: VARCHAR, ''),
	    '|',
	    COALESCE (MOD_BY:: VARCHAR, '')
	   )
	  ) AS CRC
	 FROM
	  VERINT_ACTV_MAPOut AS VERINT_ACTV_MAPOut
),

tAddCRCRow_3_Lookup_LastMatchOut as (
	/* transType: "Expression" */
	 SELECT
	  DISTINCT ACTV_MAP_ID,
	  LAST_VALUE (EFF_DTTM) OVER (
	   PARTITION BY ACTV_MAP_ID
	   ORDER BY
	    ACTV_MAP_ID
	  ) AS EFF_DTTM,
	  LAST_VALUE (ACTV_ID) OVER (
	   PARTITION BY ACTV_MAP_ID
	   ORDER BY
	    ACTV_MAP_ID
	  ) AS ACTV_ID,
	  LAST_VALUE (MAPPED_ACTV_ID) OVER (
	   PARTITION BY ACTV_MAP_ID
	   ORDER BY
	    ACTV_MAP_ID
	  ) AS MAPPED_ACTV_ID,
	  LAST_VALUE (MOD_BY) OVER (
	   PARTITION BY ACTV_MAP_ID
	   ORDER BY
	    ACTV_MAP_ID
	  ) AS MOD_BY,
	  LAST_VALUE (CRC) OVER (
	   PARTITION BY ACTV_MAP_ID
	   ORDER BY
	    ACTV_MAP_ID
	  ) AS CRC
	 FROM
	  LKP_CRCOut AS LKP_CRCOut
),

SRC_CRCOut as (
	/* transType: "Expression" */
	 SELECT
	  ACTV_MAP_ID AS ACTV_MAP_ID,
	  ACTV_ID AS ACTV_ID,
	  MAPPED_ACTV_ID AS MAPPED_ACTV_ID,
	  MOD_BY AS MOD_BY,
	  MD5 (
	   CONCAT (
	    COALESCE (ACTV_ID:: VARCHAR, ''),
	    '|',
	    COALESCE (MAPPED_ACTV_ID:: VARCHAR, ''),
	    '|',
	    COALESCE (MOD_BY:: VARCHAR, '')
	   )
	  ) AS CRC
	 FROM
	  {{ ref('stg_stage__ACTIVITYMAPPINGOut') }} AS ACTIVITYMAPPINGOut
),

tMap_1Out as (
	/* transType: "Joiner" */
	 SELECT
	  lnk_src.CRC AS CRC_1,
	  lnk_ref.CRC AS CRC_2,
	  lnk_date.EFF_DTTM AS EFF_DTTM_1,
	  lnk_ref.EFF_DTTM AS EFF_DTTM_2,
	  lnk_src.ACTV_ID AS ACTV_ID,
	  lnk_date.AUD_CRE_DTTM AS AUD_CRE_DTTM,
	  lnk_src.MOD_BY AS MOD_BY,
	  lnk_src.MAPPED_ACTV_ID AS MAPPED_ACTV_ID,
	  lnk_src.ACTV_MAP_ID AS ACTV_MAP_ID,
	  lnk_date.UPDT_END_DTTM AS UPDT_END_DTTM
	 FROM
	  SRC_CRCOut AS SRC_CRCOut
	  CROSS JOIN {{ ref('stg_stage__CURRENT_TIMESTAMPOut') }} AS lnk_date
	  INNER JOIN tAddCRCRow_3_Lookup_LastMatchOut AS tAddCRCRow_3_Lookup_LastMatchOut ON ACTV_MAP_ID = ACTV_MAP_ID
),

Router_tMap_1Out2 as (
	/* transType: "Router" */
	 SELECT
	  tMap_1Out.lnk_src.ACTV_MAP_ID AS lnk_src.ACTV_MAP_ID,
	  tMap_1Out.lnk_src.ACTV_ID AS lnk_src.ACTV_ID,
	  tMap_1Out.lnk_src.MAPPED_ACTV_ID AS lnk_src.MAPPED_ACTV_ID,
	  tMap_1Out.lnk_src.MOD_BY AS lnk_src.MOD_BY,
	  tMap_1Out.lnk_src.CRC AS lnk_src.CRC,
	  tMap_1Out.lnk_date.UPDT_END_DTTM AS lnk_date.UPDT_END_DTTM,
	  tMap_1Out.lnk_date.EFF_DTTM AS lnk_date.EFF_DTTM,
	  tMap_1Out.lnk_date.AUD_CRE_DTTM AS lnk_date.AUD_CRE_DTTM,
	  tMap_1Out.lnk_ref.ACTV_MAP_ID AS lnk_ref.ACTV_MAP_ID,
	  tMap_1Out.lnk_ref.EFF_DTTM AS lnk_ref.EFF_DTTM,
	  tMap_1Out.lnk_ref.ACTV_ID AS lnk_ref.ACTV_ID,
	  tMap_1Out.lnk_ref.MAPPED_ACTV_ID AS lnk_ref.MAPPED_ACTV_ID,
	  tMap_1Out.lnk_ref.MOD_BY AS lnk_ref.MOD_BY,
	  tMap_1Out.lnk_ref.CRC AS lnk_ref.CRC
	 FROM
	  tMap_1Out AS tMap_1Out
	 WHERE
	  NOT CRC_1 = CRC_2
),

lnk_upd_insOut as (
	/* transType: "Expression" */
	 SELECT
	  ACTV_MAP_ID AS ACTV_MAP_ID,
	  "VERINT" AS SOR_CD,
	  EFF_DTTM AS EFF_DTTM,
	  TO_TIMESTAMP_NTZ (
	   "yyyy-MM-dd HH:mm:ss.SSS",
	   "9999-12-31 23:59:59.9999999"
	  ) AS END_DTTM,
	  ACTV_MAP_ID || "~" || "VERINT" AS UNQ_KEY_TXT,
	  ACTV_ID AS ACTV_ID,
	  MAPPED_ACTV_ID AS MAPPED_ACTV_ID,
	  writeSplForNullString (MOD_BY) AS MOD_BY,
	  "ETL-INSERT" AS AUD_CRE_BY_NM,
	  AUD_CRE_DTTM AS AUD_CRE_DTTM,
	  ETL_BATCH_ID AS ETL_BATCH_ID,
	  "Y" AS CURR_IND
	 FROM
	  tMap_1Out AS tMap_1Out
	 WHERE
	  ! lnk_src.CRC.equals(lnk_ref.CRC)
)

SELECT *
FROM lnk_upd_insOut