/* Transformation Name ==>COMP_TIME_DATE_FILE ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='comp_time_date_file',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='overwrite')
}}

With SQ_PAY_PERIODOut as (
	Select PP_NUM, 
		PP_END_YEAR, 
		PP_START_DTE, 
		PP_END_DTE, 
		LV_NUM, 
		LV_YEAR, 
		PAY_DTE, 
		CURR_PP_FLAG
 from 
{{ source ('INFO_TARGET','PAY_PERIOD') }} 
 Where CURR_PP_FLAG = 'Y'
 ),

exp_Build_Pay_Period_variableOut as (
	
Select 
	PP_NUM as PP_NUM,
	 PP_END_YEAR as PP_END_YEAR,
	 ( CASE WHEN PP_NUM < 10 THEN LPAD ( TO_VARCHAR ( PP_NUM ),
	 2,
	 '0' ) ELSE TO_VARCHAR ( PP_NUM ) END ) as v_PP_NUM,
	 TO_VARCHAR ( PP_END_YEAR ) || v_PP_NUM as v_PAY_PERIOD
From SQ_PAY_PERIODOut as SQ_PAY_PERIODOut),

exp_Build_Pay_PeriodOut as (
	
Select 
	TO_VARCHAR ( PP_END_YEAR ) || v_PP_NUM as o_PAY_PERIOD,
	 { { var ( 'MAP_PP_YEAR_NUM' ) } } as o_MAP_PP_YEAR_NUM,
	 { { var ( 'MAP_PP_END_YEAR' ) } } as o_PP_END_YEAR,
	 { { var ( 'MAP_PP_NUM' ) } } as o_PP_NUM
From exp_Build_Pay_Period_variableOut as exp_Build_Pay_Period_variableOut),

DSR_exp_FinalOut as (
	
Select 
	exp_Build_Pay_PeriodOut.o_PAY_PERIOD as PAY_PERIOD,
	 exp_Build_Pay_PeriodOut.o_MAP_PP_YEAR_NUM as MAP_PP_YEAR_NUM,
	 exp_Build_Pay_PeriodOut.o_PP_END_YEAR as PP_END_YEAR,
	 exp_Build_Pay_PeriodOut.o_PP_NUM as PP_NUM
From exp_Build_Pay_PeriodOut as exp_Build_Pay_PeriodOut),

exp_FinalOut as (
	
Select 
	PAY_PERIOD as PAY_PERIOD
From DSR_exp_FinalOut as DSR_exp_FinalOut)


Select 
	PAY_PERIOD as PAY_PERIOD 
From exp_FinalOut as exp_FinalOut