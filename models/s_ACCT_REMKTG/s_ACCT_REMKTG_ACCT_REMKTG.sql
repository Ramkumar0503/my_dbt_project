/* Transformation Name ==>ACCT_REMKTG ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='ACCT_REMKTG',
		schema='',
		pre_hook ="CALL CORE_SVC.pkg_rename.truncate_load_table ( 'ACCT_REMKTG' ) ",
		post_hook ="",
		incremental_strategy='append')
}}

With sq_ACCT_AUCTN_VEH_SALEOut as (
	Select ACCT_NBR, 
		AUCTN_HLD_IND, 
		LITIGATN_HLD_IND, 
		REMRKTG_HLD_IND
 from 
{{ source ('DBConnection_SVC_INFA_ETL_DW6','SNAP_ACCT_AUCTN_VEH_SALE') }} 
 ),

sq_ACCT_BKRPTOut as (
	Select ACCT_NBR, 
		LEGAL_HLD_IND
 from 
{{ source ('DBConnection_SVC_INFA_ETL_DW6','SNAP_ACCT_BKRPT') }} 
 Where LEGAL_HLD_IND = 'Y'
 ),

srt_ACCT_BKRPTOut as (
	Select 
	ACCT_NBR,
	 LEGAL_HLD_IND 
From sq_ACCT_BKRPTOut as sq_ACCT_BKRPTOut Order By ACCT_NBR ASC),

srt_AUCTN_VEH_SALEOut as (
	Select 
	ACCT_NBR,
	 AUCTN_HLD_IND,
	 LITIGATN_HLD_IND,
	 REMRKTG_HLD_IND 
From sq_ACCT_AUCTN_VEH_SALEOut as sq_ACCT_AUCTN_VEH_SALEOut Order By ACCT_NBR ASC),

srt_ACCT_BKRPT_details as (
  Select
  	srt_ACCT_BKRPTOut.ACCT_NBR as ACCT_NBR1,
	 srt_ACCT_BKRPTOut.LEGAL_HLD_IND as LEGAL_HLD_IND
  From srt_ACCT_BKRPTOut as srt_ACCT_BKRPTOut
),
jnr_1Out as (
Select 
	srt_AUCTN_VEH_SALEOut.ACCT_NBR as ACCT_NBR,
	 srt_AUCTN_VEH_SALEOut.AUCTN_HLD_IND as AUCTN_HLD_IND,
	 srt_AUCTN_VEH_SALEOut.LITIGATN_HLD_IND as LITIGATN_HLD_IND,
	 srt_AUCTN_VEH_SALEOut.REMRKTG_HLD_IND as REMRKTG_HLD_IND,
	 srt_ACCT_BKRPT_details.ACCT_NBR1 as ACCT_NBR1,
	 srt_ACCT_BKRPT_details.LEGAL_HLD_IND as LEGAL_HLD_IND,
	 srt_AUCTN_VEH_SALEOut.jkey as jkey 
From
srt_ACCT_BKRPT_details as srt_ACCT_BKRPT_details right JOIN srt_ACCT_BKRPT_details as srt_ACCT_BKRPT_details 
ON
srt_ACCT_BKRPT_details.ACCT_NBR1 = srt_AUCTN_VEH_SALEOut.ACCT_NBR 
),

exp_Initial_variableOut as (
	
Select 
ACCT_NBR as ACCT_NBR,
AUCTN_HLD_IND as AUCTN_HLD_IND,
LITIGATN_HLD_IND as LITIGATN_HLD_IND,
REMRKTG_HLD_IND as REMRKTG_HLD_IND,
LEGAL_HLD_IND as LEGAL_HLD_IND,
( CASE WHEN AUCTN_HLD_IND = 'SS' THEN 'Y' ELSE SUBSTRING ( LTRIM ( RTRIM ( AUCTN_HLD_IND ) ),
1,
1 ) END ) as AUCTN_HLD_IND_v
From jnr_1Out as jnr_1Out),

exp_InitialOut as (
	
Select 
TO_DECIMAL ( SUBSTRING ( ACCT_NBR,
-14 ) ) as ACCT_NBR_out,
AUCTN_HLD_IND_v as AUCTN_HLD_IND_out,
SUBSTRING ( LTRIM ( RTRIM ( LITIGATN_HLD_IND ) ),
1,
1 ) as LITIGATN_HLD_IND_out,
LTRIM ( RTRIM ( REMRKTG_HLD_IND ) ) as REMRKTG_HLD_IND_out,
LTRIM ( RTRIM ( LEGAL_HLD_IND ) ) as LEGAL_HLD_IND_out
From exp_Initial_variableOut as exp_Initial_variableOut),

DSR_ACCT_REMKTGOut as (
	
Select 
exp_InitialOut.ACCT_NBR_out as CONCISE_ACCT_NBR,
exp_InitialOut.AUCTN_HLD_IND_out as AUCTN_HLD_IND,
exp_InitialOut.LITIGATN_HLD_IND_out as LITIGATN_HLD_IND,
exp_InitialOut.REMRKTG_HLD_IND_out as REMKTG_HLD_IND,
exp_InitialOut.LEGAL_HLD_IND_out as BKRPT_HLD_IND
From exp_InitialOut as exp_InitialOut)


Select 
	CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
	AUCTN_HLD_IND as AUCTN_HLD_IND,
	LITIGATN_HLD_IND as LITIGATN_HLD_IND,
	REMKTG_HLD_IND as REMKTG_HLD_IND,
	BKRPT_HLD_IND as BKRPT_HLD_IND 
From DSR_ACCT_REMKTGOut as DSR_ACCT_REMKTGOut