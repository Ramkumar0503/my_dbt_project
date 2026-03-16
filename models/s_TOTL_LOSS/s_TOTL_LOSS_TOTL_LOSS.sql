/* Transformation Name ==>TOTL_LOSS ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='TOTL_LOSS',
		schema='',
		pre_hook ="CALL CORE_SVC.pkg_rename.truncate_load_table ( 'TOTL_LOSS' ) ",
		post_hook ="",
		incremental_strategy='append')
}}

With sq_FIS_AS_TOTL_LOSSOut as (
	Select CONCISE_ACCT_NBR
 from 
{{ source ('DBConnection_SVC_INFA_ETL_CORE','FIS_AS_TOTL_LOSS') }} 
 )


Select 
	CONCISE_ACCT_NBR as CONCISE_ACCT_NBR 
From sq_FIS_AS_TOTL_LOSSOut as sq_FIS_AS_TOTL_LOSSOut