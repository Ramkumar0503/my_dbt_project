/* Transformation Name ==>etl_process_INSERT ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='etl_process',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append')
}}

With sql_etl_file_controlOut as (
	/* SubQuery from Source ==>SQ_sql_etl_file_control */
/* transType: "Source" */
Select 
etl_source_system_code,
extract_stamp
 from 
(SELECT
    etl_file_control.etl_source_system_code,
    etl_file_control.extract_stamp
 FROM
    etl_file_control
 WHERE  
  ETL_FILE_CONTROL.ETL_LOAD_END IS NULL AND 
  ETL_FILE_CONTROL.ETL_LOAD_START IS DSERROR_NOT NULL AND 
  ETL_FILE_CONTROL.ETL_SOURCE_SYSTEM_CODE= '{{ var("SOURCESYSTEMCODE") }}' AND 
   etl_file_control.extract_stamp =
 (SELECT
    MAX(etl_file_control.extract_stamp)
 FROM
    etl_file_control
 WHERE 
  ETL_FILE_CONTROL.ETL_SOURCE_SYSTEM_CODE= '{{ var("SOURCESYSTEMCODE") }}'))
 ),

Shortcut_To_seq_ETL_JOB_IDOut as (
	/* transType: "Sequence" */
Select 
	sql_etl_file_controlOut.etl_source_system_code as etl_source_system_code,
	 sql_etl_file_controlOut.extract_stamp as extract_stamp 
FROM sql_etl_file_controlOut as sql_etl_file_controlOut),

Target_Table_Name_SK_Max_Value AS (
  SELECT
    MAX(etl_job_id) AS max_etl_job_id
  FROM {{ this }}
),

 DSR_exp_CONSOLIDATEOut as (
	/* transType: "Expression" */
Select 
	Shortcut_To_seq_ETL_JOB_IDOut.(SELECT max_etl_job_id FROM Target_Table_Name_SK_Max_Value) + ROW_NUMBER() OVER() AS etl_job_id,
	 Shortcut_To_seq_ETL_JOB_IDOut.etl_source_system_code as etl_source_system_code,
	 Shortcut_To_seq_ETL_JOB_IDOut.extract_stamp as extract_stamp
From Shortcut_To_seq_ETL_JOB_IDOut as Shortcut_To_seq_ETL_JOB_IDOut),

exp_CONSOLIDATEOut as (
	/* transType: "Expression" */
Select 
	etl_job_id as etl_job_id,
	 etl_source_system_code as etl_source_system_code,
	 extract_stamp as extract_stamp,
	 dsCurrentTimestamp ( ) as o_SYSDATE,
	 '{{ var("PMSessionName") }}' as o_JOBNAME
From DSR_exp_CONSOLIDATEOut as DSR_exp_CONSOLIDATEOut),

DSR_etl_process_INSERTOut as (
	/* transType: "Expression" */
Select 
	exp_CONSOLIDATEOut.o_JOBNAME as etl_job_name,
	 exp_CONSOLIDATEOut.etl_job_id as etl_job_id,
	 exp_CONSOLIDATEOut.etl_source_system_code as etl_source_system_code,
	 exp_CONSOLIDATEOut.extract_stamp as extract_stamp,
	 exp_CONSOLIDATEOut.o_SYSDATE as etl_job_start
From exp_CONSOLIDATEOut as exp_CONSOLIDATEOut)

Select 
	etl_job_id as etl_job_id,
	etl_source_system_code as etl_source_system_code,
	extract_stamp as extract_stamp,
	etl_job_start as etl_job_start,
	etl_job_end as etl_job_end,
	etl_job_name as etl_job_name 
From DSR_etl_process_INSERTOut as DSR_etl_process_INSERTOut