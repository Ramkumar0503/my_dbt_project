/* Transformation Name ==>CORE_SVC_ACCT_RTE ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='CORE_SVC_ACCT_RTE',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append')
}}

With sq_ACCT_RTEOut as (
	Select CONCISE_ACCT_NBR, 
		BILL_GRP_ID, 
		POOL_ID
 from 
{{ source ('DBConnection_SVC_INFA_ETL_CORE','ACCT_RTE') }} 
 ),

exp_TGTOut as (
	
Select 
CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
BILL_GRP_ID as BILL_GRP_ID,
POOL_ID as POOL_ID
From sq_ACCT_RTEOut as sq_ACCT_RTEOut)


Select 
	CONCISE_ACCT_NBR as CONCISE_ACCT_NBR,
	BILL_GRP_ID as BILL_GRP_ID,
	POOL_ID as POOL_ID 
From exp_TGTOut as exp_TGTOut