/* Transformation Name ==>ACCT_PSEUDO_NBR_XREF_dsGen ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='ACCT_PSEUDO_NBR_XREF',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append')
}}

With SQ_ACCT_PSEUDO_NBR_XREFOut as (
	/* SubQuery from Source ==>SQ_SQ_ACCT_PSEUDO_NBR_XREF */
Select 
ACCT_NBR,
ACCT_PSEUDO_SEQ_ID
 from 
(SELECT
 distinct
    ACCT_PSEUDO_NBR_XREF.ACCT_NBR,
    ACCT_PSEUDO_NBR_XREF.ACCT_PSEUDO_SEQ_ID
 FROM {{ source('yml_Source_Name','{{ source ('PIXL_SHARED', 'ACCT_PSEUDO_NBR_XREF') }}') }} ACCT_PSEUDO_NBR_XREF
 ORDER BY
    ACCT_PSEUDO_NBR_XREF.ACCT_PSEUDO_SEQ_ID -)
 ),

exp_PassThruOut as (
	
Select 
ACCT_NBR as ACCT_NBR,
ACCT_PSEUDO_SEQ_ID as ACCT_PSEUDO_SEQ_ID
From SQ_ACCT_PSEUDO_NBR_XREFOut as SQ_ACCT_PSEUDO_NBR_XREFOut)


Select 
	ACCT_NBR as ACCT_NBR,
	ACCT_PSEUDO_SEQ_ID as ACCT_PSEUDO_SEQ_ID 
From exp_PassThruOut as exp_PassThruOut