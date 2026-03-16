/* Transformation Name ==>ACCT_ADDR ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='ACCT_ADDR',
		schema='',
		pre_hook ="CALL CORE_SVC.pkg_rename.truncate_load_table ( 'ACCT_ADDR' ) ",
		post_hook ="",
		incremental_strategy='append')
}}

With SQ_ACCT_ADDROut as (
	Select ACCT_NBR, 
		ADDR_TYPE_DESCR, 
		ADDR_DT, 
		ADDR_LINE_1_TXT, 
		ADDR_LINE_2_TXT, 
		CITY_NM, 
		STATE_CD, 
		POST_CD, 
		COUNTRY_CD
 from 
{{ source ('DBConnection_SVC_INFA_ETL_CORE','ACCT_ADDR') }} 
 )


Select 
	ACCT_NBR as ACCT_NBR,
	ADDR_TYPE_DESCR as ADDR_TYPE_DESCR,
	ADDR_DT as ADDR_DT,
	ADDR_LINE_1_TXT as ADDR_LINE_1_TXT,
	ADDR_LINE_2_TXT as ADDR_LINE_2_TXT,
	CITY_NM as CITY_NM,
	STATE_CD as STATE_CD,
	POST_CD as POST_CD,
	COUNTRY_CD as COUNTRY_CD 
From SQ_ACCT_ADDROut as SQ_ACCT_ADDROut