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

With SQ_PAY_PERIODOut AS ( SELECT PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG FROM {{ source ('INFO_TARGET','PAY_PERIOD') }} WHERE CURR_PP_FLAG = 'Y' ),

exp_Build_Pay_Period_variableOut AS ( SELECT PP_NUM AS PP_NUM, PP_END_YEAR AS PP_END_YEAR, ( CASE WHEN PP_NUM < 10 THEN LPAD ( TO_VARCHAR ( PP_NUM ), 2, '0' ) ELSE TO_VARCHAR ( PP_NUM ) END ) AS v_PP_NUM, TO_VARCHAR ( PP_END_YEAR ) || v_PP_NUM AS v_PAY_PERIOD FROM SQ_PAY_PERIODOut AS SQ_PAY_PERIODOut),

exp_Build_Pay_PeriodOut AS ( SELECT TO_VARCHAR ( PP_END_YEAR ) || v_PP_NUM AS o_PAY_PERIOD, '{{ var("map_pp_year_num") }}' AS o_MAP_PP_YEAR_NUM, '{{ var("map_pp_end_year") }}' AS o_PP_END_YEAR, '{{ var("map_pp_num") }}' AS o_PP_NUM FROM exp_Build_Pay_Period_variableOut AS exp_Build_Pay_Period_variableOut),

DSR_exp_FinalOut AS ( SELECT exp_Build_Pay_PeriodOut.o_PAY_PERIOD AS PAY_PERIOD, exp_Build_Pay_PeriodOut.o_MAP_PP_YEAR_NUM AS MAP_PP_YEAR_NUM, exp_Build_Pay_PeriodOut.o_PP_END_YEAR AS PP_END_YEAR, exp_Build_Pay_PeriodOut.o_PP_NUM AS PP_NUM FROM exp_Build_Pay_PeriodOut AS exp_Build_Pay_PeriodOut),

exp_FinalOut AS ( SELECT PAY_PERIOD AS PAY_PERIOD FROM DSR_exp_FinalOut AS DSR_exp_FinalOut)


Select 
	PAY_PERIOD as PAY_PERIOD 
From exp_FinalOut as exp_FinalOut