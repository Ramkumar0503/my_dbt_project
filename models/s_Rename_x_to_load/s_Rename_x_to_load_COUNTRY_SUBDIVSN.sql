/* Transformation Name ==>COUNTRY_SUBDIVSN ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='COUNTRY_SUBDIVSN',
		schema='',
		pre_hook ="CALL CORE_SVC.pkg_rename.truncate_load_table ( 'ACCT_LOAN_EXTENSN' ) ",
		post_hook ="CALL CORE_SVC.pkg_rename.gather_table_stats ( 'ACCT_LOAN_EXTENSN' ) ",
		incremental_strategy='append')
}}

With SQ_COUNTRY_SUBDIVSNOut as (
	Select COUNTRY_CD, 
		SUBDIVSN_CD, 
		SUBDIVSN_NM
 from 
{{ source ('DBConnection_SVC_INFA_ETL_CORE','COUNTRY_SUBDIVSN') }} 
 ),

flt_FalseOut as (
	Select 
 * 
From SQ_COUNTRY_SUBDIVSNOut as SQ_COUNTRY_SUBDIVSNOut Where FALSE)


Select 
	COUNTRY_CD as COUNTRY_CD 
From flt_FalseOut as flt_FalseOut