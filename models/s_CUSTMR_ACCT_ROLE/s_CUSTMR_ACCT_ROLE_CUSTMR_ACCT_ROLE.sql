/* Transformation Name ==>CUSTMR_ACCT_ROLE ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='CUSTMR_ACCT_ROLE',
		schema='',
		pre_hook ="CALL CORE_SVC.pkg_rename.truncate_load_table ( 'CUSTMR_ACCT_ROLE' ) ",
		post_hook ="",
		incremental_strategy='append')
}}

With SQ_CUSTMR_LOAD_MAPPINGOut as (
	Select CUSTMR_KEY, 
		TAX_ID_NBR, 
		CUSTMR_NBR, 
		TRANSACT_CUSTMR_NBR, 
		CUSTMR_TYPE_CD, 
		ADDR_LINE_1_TXT, 
		ADDR_LINE_2_TXT, 
		CITY_NM, 
		STATE_CD, 
		COUNTRY_CD, 
		POST_CD, 
		FULL_NM, 
		FIRST_NM, 
		MIDL_NM, 
		LAST_NM, 
		GEN_CD, 
		LEGACY_CUSTMR_NBR, 
		BIRTH_DT, 
		HM_PHN_NBR, 
		HM_PHN_AUTH_DT, 
		HM_PHN_VALID_FLAG, 
		BUSN_PHN_NBR, 
		BUSN_PHN_AUTH_DT, 
		OTHR_PHN_NBR, 
		OTHR_PHN_AUTH_DT, 
		VALID_ADDR_IND, 
		PREF_LANG_CD
 from 
{{ source ('DBConnection_SVC_INFA_ETL_CORE','CUSTMR_LOAD_MAPPING') }} 
 ),

SQ_RELTN_LOAD_MAPPINGOut as (
	Select CONCISE_ACCT_NBR, 
		CUSTMR_KEY, 
		CUSTMR_RELTN_CD
 from 
{{ source ('DBConnection_SVC_INFA_ETL_CORE','RELTN_LOAD_MAPPING') }} 
 ),

exp_CUSTMROut as (
	
Select 
CUSTMR_KEY as CUSTMR_KEY
From SQ_CUSTMR_LOAD_MAPPINGOut as SQ_CUSTMR_LOAD_MAPPINGOut),

exp_RELTNOut as (
	
Select 
CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
CUSTMR_KEY as CUSTMR_KEY,
LTRIM ( RTRIM ( CUSTMR_RELTN_CD ) ) as CUSTMR_RELTN_CD_out
From SQ_RELTN_LOAD_MAPPINGOut as SQ_RELTN_LOAD_MAPPINGOut),

srt_CUSTMROut as (
	Select 
	CUSTMR_KEY 
From exp_CUSTMROut as exp_CUSTMROut Order By CUSTMR_KEY ASC),

DSR_srt_RELTNOut as (
	
Select 
exp_RELTNOut.CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
exp_RELTNOut.CUSTMR_KEY as CUSTMR_KEY,
exp_RELTNOut.CUSTMR_RELTN_CD_out as CUSTMR_RELTN_CD
From exp_RELTNOut as exp_RELTNOut),

srt_RELTNOut as (
	Select 
	CONCISE_ACCT_NBR,
	 CUSTMR_KEY,
	 CUSTMR_RELTN_CD 
From DSR_srt_RELTNOut as DSR_srt_RELTNOut Order By CUSTMR_KEY ASC),

srt_RELTN_details as (
  Select
  	srt_RELTNOut.CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
	 srt_RELTNOut.CUSTMR_KEY as CUSTMR_KEY1,
	 srt_RELTNOut.CUSTMR_RELTN_CD as CUSTMR_RELTN_CD
  From srt_RELTNOut as srt_RELTNOut
),
jnr_CUSTMR_RELTNOut as (
Select 
	srt_RELTN_details.CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
	 srt_RELTN_details.CUSTMR_KEY1 as CUSTMR_KEY1,
	 srt_RELTN_details.CUSTMR_RELTN_CD as CUSTMR_RELTN_CD,
	 srt_CUSTMROut.CUSTMR_KEY as CUSTMR_KEY,
	 srt_CUSTMROut.jkey as jkey 
From
srt_CUSTMROut as srt_CUSTMROut inner JOIN srt_RELTN_details as srt_RELTN_details 
ON
srt_RELTN_details.CUSTMR_KEY1 = srt_CUSTMROut.CUSTMR_KEY 
),

DSR_exp_FinalOut as (
	
Select 
jnr_CUSTMR_RELTNOut.CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
jnr_CUSTMR_RELTNOut.CUSTMR_KEY1 as CUSTMR_KEY,
jnr_CUSTMR_RELTNOut.CUSTMR_RELTN_CD as CUSTMR_RELTN_CD
From jnr_CUSTMR_RELTNOut as jnr_CUSTMR_RELTNOut),

exp_FinalOut as (
	
Select 
CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
CUSTMR_KEY as CUSTMR_KEY,
CUSTMR_RELTN_CD as CUSTMR_RELTN_CD,
CURRENT_TIMESTAMP as INSERT_DT
From DSR_exp_FinalOut as DSR_exp_FinalOut)


Select 
	CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
	CUSTMR_KEY as CUSTMR_KEY,
	CUSTMR_RELTN_CD as CUSTMR_RELTN_CD,
	INSERT_DT as INSERT_DT 
From exp_FinalOut as exp_FinalOut