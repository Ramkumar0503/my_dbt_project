/* Transformation Name ==>COMPTIME_MESSAGE_FILE ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='comptime_message_file',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='overwrite')
}}

With SQ_U0287D01Out AS ( SELECT SSN, NAME, CURRENT_ACCT, CURRENT_ORG, FLSA_STATUS, COMP_TIME_CUR_BAL, COMP_TIME_YEAR_EARNED, PP_END_DATE, DAILY_DATE_EARNED, COMP_TIME_RATE, COMP_TIME_HOURS, COMP_TIME_UNDEF FROM {{ source ('FlatFile_catalog_1','U0287D01') }} ),

PAY_PERIODOut AS ( SELECT in_CURR_PP_FLAG, PP_NUM, PP_END_YEAR, PP_START_DTE, PP_END_DTE, LV_NUM, LV_YEAR, PAY_DTE, CURR_PP_FLAG FROM {{ source ('lkpSource_1','PAY_PERIOD') }} ),

exp_InitialOut AS ( SELECT SSN AS SSN, NAME AS NAME, DECODE ( TRUE, TRY_TO_NUMBER ( SSN ) IS NOT NULL, 'D', 'NO' ) AS o_RECORD_TYPE_FLAG FROM SQ_U0287D01Out AS SQ_U0287D01Out),

DSR_fil_DetailOut AS ( SELECT exp_InitialOut.SSN AS SSN, exp_InitialOut.o_RECORD_TYPE_FLAG AS RECORD_TYPE_FLAG FROM exp_InitialOut AS exp_InitialOut),

fil_DetailOut AS ( SELECT * FROM DSR_fil_DetailOut AS DSR_fil_DetailOut WHERE RECORD_TYPE_FLAG = 'D'),

agg_ALL_RECORDSOut AS ( with agg_ALL_RECORDS_Out AS ( SELECT COUNT ( SSN ) AS o_DETAIL_RECORD_COUNT FROM fil_DetailOut ), agg_ALL_RECORDS_NonGroupBy AS ( SELECT SSN, RECORD_TYPE_FLAG, RECORD_TYPE FROM fil_DetailOut qualify row_number() over (ORDER BY SSN DESC) = 1 ), agg_ALL_RECORDSOut AS ( SELECT dsf_1.o_DETAIL_RECORD_COUNT, dsf_2.SSN, dsf_2.RECORD_TYPE_FLAG, dsf_2.RECORD_TYPE FROM agg_ALL_RECORDS_Out dsf_1 CROSS JOIN agg_ALL_RECORDS_NonGroupBy dsf_2 ) SELECT * FROM agg_final),

DSR_exp_Detail_CountOut AS ( SELECT agg_ALL_RECORDSOut.o_DETAIL_RECORD_COUNT AS DETAIL_RECORD_COUNT FROM agg_ALL_RECORDSOut AS agg_ALL_RECORDSOut),

exp_Detail_CountOut AS ( SELECT DETAIL_RECORD_COUNT AS DETAIL_RECORD_COUNT, 'Y' AS o_CURR_PP_FLAG FROM DSR_exp_Detail_CountOut AS DSR_exp_Detail_CountOut),

lkp_PAY_PERIODOut AS ( SELECT PP_NUM AS PP_NUM, PP_END_YEAR AS PP_END_YEAR, exp_Detail_Count.o_CURR_PP_FLAG AS in_CURR_PP_FLAG FROM exp_Detail_CountOut AS exp_Detail_CountOut LEFT JOIN PAY_PERIODOut ON CURR_PP_FLAG = in_CURR_PP_FLAG ),

lkp_PAY_PERIOD_details AS ( SELECT lkp_PAY_PERIODOut.PP_NUM AS lkp_PP_NUM, lkp_PAY_PERIODOut.PP_END_YEAR AS lkp_PP_END_YEAR FROM lkp_PAY_PERIODOut AS lkp_PAY_PERIODOut ), dsJoiner_exp_CountersOut AS ( SELECT exp_Detail_CountOut.DETAIL_RECORD_COUNT AS DETAIL_RECORD_COUNT, lkp_PAY_PERIOD_details.lkp_PP_NUM AS lkp_PP_NUM, lkp_PAY_PERIOD_details.lkp_PP_END_YEAR AS lkp_PP_END_YEAR, exp_Detail_CountOut.jkey AS jkey FROM exp_Detail_CountOut AS exp_Detail_CountOut INNER JOIN lkp_PAY_PERIOD_details AS lkp_PAY_PERIOD_details ON lkp_PAY_PERIOD_details.jkey = exp_Detail_CountOut.jkey ),

exp_CountersOut AS ( SELECT 'Number of detail records FROM the COMP TIME file.' AS o_COUNTER_DESCRIPTION_1, DETAIL_RECORD_COUNT AS DETAIL_RECORD_COUNT, lkp_PP_NUM AS lkp_PP_NUM, lkp_PP_END_YEAR AS lkp_PP_END_YEAR FROM dsJoiner_exp_CountersOut AS dsJoiner_exp_CountersOut),

exp_Build_Message_variableOut AS ( SELECT o_COUNTER_DESCRIPTION_1 AS COUNTER_DESCRIPTION_1, DETAIL_RECORD_COUNT AS COUNTER_1, lkp_PP_NUM AS PP_NUM, lkp_PP_END_YEAR AS PP_END_YEAR, ( CASE WHEN PP_NUM < 10 THEN LPAD ( TO_VARCHAR ( PP_NUM ), 2, '0' ) ELSE TO_VARCHAR ( PP_NUM ) END ) AS v_PP_NUM, DECODE ( SUBSTRING ( $PMRepositoryServiceName, 1, 4 ), 'Dev_', 'Dev:', 'Test', 'Test:', 'Prod', 'Prod:' ) AS v_ENVIRONMENT, v_ENVIRONMENT || 'Comp Time File loaded successfully for Pay Period:' || TO_VARCHAR ( PP_END_YEAR ) || '-' || v_PP_NUM AS v_SUBJECT, 'Number of Detail Records FROM Comp Time file = ' || TO_VARCHAR ( COUNTER_1 ) AS v_MESSAGE FROM exp_CountersOut AS exp_CountersOut),

exp_Build_MessageOut AS ( SELECT '{{ var("map_subject") }}' AS o_SUBJECT, '{{ var("map_message") }}' AS o_MESSAGE FROM exp_Build_Message_variableOut AS exp_Build_Message_variableOut),

DSR_exp_Final_MessageOut AS ( SELECT exp_Build_MessageOut.o_SUBJECT AS SUBJECT, exp_Build_MessageOut.o_MESSAGE AS MESSAGE FROM exp_Build_MessageOut AS exp_Build_MessageOut),

exp_Final_MessageOut AS ( SELECT SUBJECT AS SUBJECT, MESSAGE AS MESSAGE FROM DSR_exp_Final_MessageOut AS DSR_exp_Final_MessageOut)


Select 
	SUBJECT as SUBJECT,
	MESSAGE as MESSAGE 
From exp_Final_MessageOut as exp_Final_MessageOut