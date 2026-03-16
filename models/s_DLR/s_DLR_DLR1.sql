/* Transformation Name ==>DLR1 ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='DLR',
		schema='',
		pre_hook ="CALL CORE_SVC.pkg_rename.truncate_load_table ( 'DLR' ) ",
		post_hook ="",
		incremental_strategy='append')
}}

With DLR_CVL_NET_FUNDINGOut as (
	Select DLR_ID, 
		ATTRIB_IND_TYPE_VAL, 
		EFF_DT, 
		EXPIRE_DT, 
		DLR_ATTRIB_IND_ID, 
		REF_ID, 
		in_DLR_ID
 from 
{{ source ('DBConnection_SVC_INFA_ETL_DW6','DLR_CVL_NET_FUNDING') }}
 ),

sq_DLROut as (
	Select DLR_NBR_CD, 
		DLR_NBR, 
		DLR_STATE, 
		COMPNY_NBR, 
		DLR_COUNTRY, 
		COST_CTR_NBR, 
		CURR_COST_CTR_NBR, 
		GL_COST_CTR_NBR, 
		UNIQ_ID, 
		CURR_RESRV_BAL, 
		RESRV_PAID_MTD, 
		RESRV_PAID_YTD, 
		DLR_RESRV_EARN_ITD_AMT, 
		DLR_CHARG_BK_ITD_AMT, 
		DLR_RESRV_PAID_BY_ITD_AMT, 
		DLR_RESRV_PAID_TO_ITD_AMT, 
		DLR_RESRV_WRT_OFF_ITD_AMT, 
		DLR_RESRV_EARN_MTD_AMT, 
		DLR_CHARG_BK_MTD_AMT, 
		DLR_RESRV_PAID_BY_MTD_AMT, 
		DLR_RESRV_WRT_OFF_MTD_AMT, 
		DLR_PREV_MNTH_RESRV_BAL_AMT, 
		DLR_RESRV_EARN_YTD_AMT, 
		DLR_CHARG_BK_YTD_AMT, 
		DLR_RESRV_PAID_BY_YTD_AMT, 
		DLR_RESRV_WRT_OFF_YTD_AMT, 
		DLR_PREV_YR_RESRV_BAL_AMT, 
		SOURCE_AS_OF_DT, 
		AS_OF_DT, 
		GM_OWNED, 
		BRAND_CD, 
		BRAND_NM_TXT, 
		DLR_NM, 
		DLR_PROG_TYPE_CD, 
		EFF_DT, 
		DLR_ID, 
		DLR_ADDR_LINE_1_TXT, 
		DLR_PRMRY_PHN_NBR, 
		DLR_CITY_NM, 
		DLR_ZIP_CD, 
		DLR_ZIP4_CD, 
		GM_ALLIANCE_IND, 
		BUSN_ASSOC_CD, 
		FACTRY_DLR_NBR
 from 
{{ source ('DBConnection_SVC_INFA_ETL_DW6','SNAP_DLR') }} 
 ),

sq_DLR_ADDROut as (
	Select EFF_DT, 
		REF_ID, 
		DLR_ID, 
		ADDR_TYPE_CD, 
		ADDR_TXT, 
		CITY_NM, 
		STATE_CD, 
		ZIP_CD, 
		ZIP4_CD, 
		COUNTRY_CD, 
		COUNTY_NM, 
		DELIVR_PT_NBR, 
		CARRIER_RTE_CD
 from 
{{ source ('DBConnection_SVC_INFA_ETL_DW6','SNAP_DLR_ADDR') }} 
 Where ADDR_TYPE_CD = 'PHYSICAL_ADDRESS' AND EFF_DT =(SELECT MAX(DLR_ADDR.EFF_DT) FROM {{ source ('PIXL', 'DLR_ADDR') }} A WHERE A.DLR_ID = DLR_ADDR.DLR_ID)
 ),

DSR_exp_Initial_DLROut as (
	
Select 
sq_DLROut.DLR_NBR_CD as DLR_NBR_CD_in,
sq_DLROut.DLR_NM as DLR_NM_in,
sq_DLROut.DLR_PRMRY_PHN_NBR as DLR_PRMRY_PHN_NBR_in,
sq_DLROut.BUSN_ASSOC_CD as BUSN_ASSOC_CD
From sq_DLROut as sq_DLROut),

DSR_exp_Initial_ADDROut as (
	
Select 
sq_DLR_ADDROut.DLR_ID as DLR_ID_in,
sq_DLR_ADDROut.ADDR_TXT as ADDR_TXT_in,
sq_DLR_ADDROut.CITY_NM as CITY_NM_in,
sq_DLR_ADDROut.STATE_CD as STATE_CD_in,
sq_DLR_ADDROut.ZIP_CD as ZIP_CD_in
From sq_DLR_ADDROut as sq_DLR_ADDROut),

exp_Initial_DLROut as (
	
Select 
( CASE WHEN DLR_NBR_CD_in IS NOT NULL THEN LTRIM ( RTRIM ( DLR_NBR_CD_in ) ) ELSE NULL END ) as DLR_NBR_CD,
( CASE WHEN DLR_NM_in IS NOT NULL THEN LTRIM ( RTRIM ( DLR_NM_in ) ) ELSE NULL END ) as DLR_NM,
( CASE WHEN DLR_PRMRY_PHN_NBR_in IS NOT NULL THEN REGEXP_REPLACE ( DLR_PRMRY_PHN_NBR_in,
'[^0-9]',
'' ) ELSE NULL END ) as DLR_PRMRY_PHN_NBR,
BUSN_ASSOC_CD as BUSN_ASSOC_CD
From DSR_exp_Initial_DLROut as DSR_exp_Initial_DLROut),

exp_Initial_ADDROut as (
	
Select 
( CASE WHEN DLR_ID_in IS NOT NULL THEN LTRIM ( RTRIM ( DLR_ID_in ) ) ELSE NULL END ) as DLR_ID,
( CASE WHEN ADDR_TXT_in IS NOT NULL THEN LTRIM ( RTRIM ( ADDR_TXT_in ) ) ELSE NULL END ) as ADDR_TXT,
( CASE WHEN CITY_NM_in IS NOT NULL THEN LTRIM ( RTRIM ( CITY_NM_in ) ) ELSE NULL END ) as CITY_NM,
( CASE WHEN STATE_CD_in IS NOT NULL THEN LTRIM ( RTRIM ( STATE_CD_in ) ) ELSE NULL END ) as STATE_CD,
( CASE WHEN ZIP_CD_in IS NOT NULL THEN LTRIM ( RTRIM ( ZIP_CD_in ) ) ELSE NULL END ) as ZIP_CD
From DSR_exp_Initial_ADDROut as DSR_exp_Initial_ADDROut),

srt_DLROut as (
	Select 
	DLR_NBR_CD,
	 DLR_NM,
	 DLR_PRMRY_PHN_NBR,
	 BUSN_ASSOC_CD 
From exp_Initial_DLROut as exp_Initial_DLROut Order By DLR_NBR_CD ASC),

srt_ADDROut as (
	Select 
	DLR_ID,
	 ADDR_TXT,
	 CITY_NM,
	 STATE_CD,
	 ZIP_CD 
From exp_Initial_ADDROut as exp_Initial_ADDROut Order By DLR_ID ASC),

jnr_DLR_ADDROut as (
Select 
	srt_DLROut.DLR_NBR_CD as DLR_NBR_CD,
	 srt_DLROut.DLR_NM as DLR_NM,
	 srt_DLROut.DLR_PRMRY_PHN_NBR as DLR_PRMRY_PHN_NBR,
	 srt_DLROut.BUSN_ASSOC_CD as BUSN_ASSOC_CD,
	 srt_ADDROut.DLR_ID as DLR_ID,
	 srt_ADDROut.ADDR_TXT as ADDR_TXT,
	 srt_ADDROut.CITY_NM as CITY_NM,
	 srt_ADDROut.STATE_CD as STATE_CD,
	 srt_ADDROut.ZIP_CD as ZIP_CD,
	 srt_ADDROut.jkey as jkey 
From
srt_ADDROut as srt_ADDROut inner JOIN srt_DLROut as srt_DLROut 
ON
srt_DLROut.DLR_NBR_CD = srt_ADDROut.DLR_ID 
),

jnr_DLR_ADDROut1 as (
Select 
	srt_DLROut.DLR_NBR_CD as DLR_NBR_CD,
	 srt_DLROut.DLR_NM as DLR_NM,
	 srt_DLROut.DLR_PRMRY_PHN_NBR as DLR_PRMRY_PHN_NBR,
	 srt_DLROut.BUSN_ASSOC_CD as BUSN_ASSOC_CD,
	 srt_ADDROut.DLR_ID as DLR_ID,
	 srt_ADDROut.ADDR_TXT as ADDR_TXT,
	 srt_ADDROut.CITY_NM as CITY_NM,
	 srt_ADDROut.STATE_CD as STATE_CD,
	 srt_ADDROut.ZIP_CD as ZIP_CD,
	 srt_ADDROut.jkey as jkey 
From
srt_ADDROut as srt_ADDROut inner JOIN srt_DLROut as srt_DLROut 
ON
srt_DLROut.DLR_NBR_CD = srt_ADDROut.DLR_ID 
),

DSR_exp_FinalOut as (
	
Select 
jnr_DLR_ADDROut.DLR_ID as DLR_ID,
jnr_DLR_ADDROut.DLR_NM as DLR_NM,
jnr_DLR_ADDROut.DLR_PRMRY_PHN_NBR as DLR_PRMRY_PHN_NBR_in,
jnr_DLR_ADDROut.BUSN_ASSOC_CD as BUSN_ASSOC_CD,
jnr_DLR_ADDROut.ADDR_TXT as ADDR_TXT,
jnr_DLR_ADDROut.CITY_NM as CITY_NM,
jnr_DLR_ADDROut.STATE_CD as STATE_CD,
jnr_DLR_ADDROut.ZIP_CD as ZIP_CD
From jnr_DLR_ADDROut as jnr_DLR_ADDROut),

lkp_DLR_CVL_NET_FUNDINGOut as (
	Select 
	ATTRIB_IND_TYPE_VAL as ATTRIB_IND_TYPE_VAL,
	 EFF_DT as EFF_DT,
	 EXPIRE_DT as EXPIRE_DT,
	 jnr_DLR_ADDROut.in_DLR_ID as in_DLR_ID_1,
	 jnr_DLR_ADDR.DLR_ID AS in_DLR_ID_2 
FROM
	jnr_DLR_ADDROut as jnr_DLR_ADDROut 
 LEFT JOIN ( select * from (
select 
*,DLR_ID,row_number() over(partition by DLR_ID order by rnk_fst desc nulls last) as rnk_lst
from
(
select *,DLR_ID,row_number() over(partition by DLR_ID order by DLR_ID nulls last) as rnk_fst
from ( SELECT
    DLR_CVL_NET_FUNDING.ATTRIB_IND_TYPE_VAL as ATTRIB_IND_TYPE_VAL,
    DLR_CVL_NET_FUNDING.EFF_DT as EFF_DT,
    DLR_CVL_NET_FUNDING.EXPIRE_DT as EXPIRE_DT,
    DLR_CVL_NET_FUNDING.DLR_ID as DLR_ID
 FROM {{ source('yml_Source_Name','{{ source ('PIXL', 'DLR_CVL_NET_FUNDING') }}') }} DLR_CVL_NET_FUNDING
 ORDER BY
    DLR_CVL_NET_FUNDING.DLR_ID,
    DLR_ATTRIB_IND_ID,
    DLR_CVL_NET_FUNDING.EFF_DT ASC )
) lkp_inner
) lkp_outer
where rnk_lst=1 ) 
ON
	DLR_ID = in_DLR_ID
 
),

exp_FinalOut as (
	
Select 
DLR_ID as DLR_ID,
DLR_NM as DLR_NM,
SUBSTRING ( DLR_PRMRY_PHN_NBR_in,
1,
10 ) as DLR_PRMRY_PHN_NBR,
BUSN_ASSOC_CD as BUSN_ASSOC_CD,
ADDR_TXT as ADDR_TXT,
CITY_NM as CITY_NM,
STATE_CD as STATE_CD,
ZIP_CD as ZIP_CD
From DSR_exp_FinalOut as DSR_exp_FinalOut),

lkp_DLR_CVL_NET_FUNDING_details as (
  Select
  	lkp_DLR_CVL_NET_FUNDINGOut.EFF_DT as NET_FUND_START_DT,
	 lkp_DLR_CVL_NET_FUNDINGOut.EXPIRE_DT as NET_FUND_END_DT,
	 lkp_DLR_CVL_NET_FUNDINGOut.ATTRIB_IND_TYPE_VAL as NET_FUND_IND
  From lkp_DLR_CVL_NET_FUNDINGOut as lkp_DLR_CVL_NET_FUNDINGOut
),

exp_Final_master as (
  Select
  	exp_FinalOut.DLR_ID as DLR_NBR,
	 exp_FinalOut.DLR_NM as NM_TXT,
	 exp_FinalOut.DLR_PRMRY_PHN_NBR as PHN_NBR,
	 exp_FinalOut.ADDR_TXT as ADDR_TXT,
	 exp_FinalOut.CITY_NM as CITY_NM,
	 exp_FinalOut.STATE_CD as STATE_CD,
	 exp_FinalOut.ZIP_CD as POST_CD,
	 exp_FinalOut.BUSN_ASSOC_CD as BUSN_ASSOC_CD
  From exp_FinalOut as exp_FinalOut
),
dsJoiner_DLR129Out as (
Select 
	exp_Final_master.DLR_NBR as DLR_NBR,
	 exp_Final_master.NM_TXT as NM_TXT,
	 exp_Final_master.PHN_NBR as PHN_NBR,
	 exp_Final_master.ADDR_TXT as ADDR_TXT,
	 exp_Final_master.CITY_NM as CITY_NM,
	 exp_Final_master.STATE_CD as STATE_CD,
	 exp_Final_master.POST_CD as POST_CD,
	 exp_Final_master.BUSN_ASSOC_CD as BUSN_ASSOC_CD,
	 lkp_DLR_CVL_NET_FUNDING_details.NET_FUND_START_DT as NET_FUND_START_DT,
	 lkp_DLR_CVL_NET_FUNDING_details.NET_FUND_END_DT as NET_FUND_END_DT,
	 lkp_DLR_CVL_NET_FUNDING_details.NET_FUND_IND as NET_FUND_IND,
	 exp_Final_master.jkey as jkey 
From
exp_Final_master as exp_Final_master inner JOIN lkp_DLR_CVL_NET_FUNDING_details as lkp_DLR_CVL_NET_FUNDING_details 
ON
lkp_DLR_CVL_NET_FUNDING_details.jkey = exp_Final_master.jkey 
)


Select 
	DLR_NBR as DLR_NBR,
	NM_TXT as NM_TXT,
	PHN_NBR as PHN_NBR,
	ADDR_TXT as ADDR_TXT,
	CITY_NM as CITY_NM,
	STATE_CD as STATE_CD,
	POST_CD as POST_CD,
	BUSN_ASSOC_CD as BUSN_ASSOC_CD,
	NET_FUND_START_DT as NET_FUND_START_DT,
	NET_FUND_END_DT as NET_FUND_END_DT,
	NET_FUND_IND as NET_FUND_IND 
From dsJoiner_DLR129Out as dsJoiner_DLR129Out