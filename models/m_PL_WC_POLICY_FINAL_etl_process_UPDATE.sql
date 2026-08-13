{{
  config(
    materialized='incremental',
    alias='etl_process',
    schema='',
    pre_hook="",
    post_hook="",
    incremental_strategy='merge',
    unique_key=['etl_job_id'],
    merge_update_columns=['etl_job_end']
  )
}}
With sql_etl_processOut as (
	/* SubQuery from Source ==>SQ_sql_etl_process */
/* transType: "Source" */
Select 
 * 
 from 
(SELECT
    MAX(etl_process.etl_job_id)
 FROM
    etl_process
 WHERE  
  ETL_PROCESS.ETL_JOB_NAME= 'DSDOLLARPMSESSIONNAME' AND 
  ETL_PROCESS.ETL_JOB_END IS NULL)
 ),

exp_CONSOLIDATE_1Out as (
	/* transType: "Expression" */
Select 
	etl_job_id as etl_job_id,
	 dsCurrentTimestamp ( ) as oSYSDATE
From sql_etl_processOut as sql_etl_processOut)

Select 
	etl_job_id as etl_job_id,
	etl_process_seq.nextval as etl_source_system_code,
	etl_process_seq.nextval as extract_stamp,
	oSYSDATE as etl_job_end 
From exp_CONSOLIDATE_1Out as exp_CONSOLIDATE_1Out