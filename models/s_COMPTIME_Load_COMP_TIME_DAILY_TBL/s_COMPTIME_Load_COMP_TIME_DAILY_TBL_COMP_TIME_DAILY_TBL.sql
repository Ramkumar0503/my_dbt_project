/* Transformation Name ==>COMP_TIME_DAILY_TBL ,Transformation Type ==>Target */

{{
	config(
		materialized='incremental',
		alias='COMP_TIME_DAILY_TBL',
		schema='',
		pre_hook ="",
		post_hook ="",
		incremental_strategy='append')
}}

With SQ_U0287D01Out as (
	Select SSN, 
		NAME, 
		CURRENT_ACCT, 
		CURRENT_ORG, 
		FLSA_STATUS, 
		COMP_TIME_CUR_BAL, 
		COMP_TIME_YEAR_EARNED, 
		PP_END_DATE, 
		DAILY_DATE_EARNED, 
		COMP_TIME_RATE, 
		COMP_TIME_HOURS, 
		COMP_TIME_UNDEF
 from 
{{ source ('FlatFile_catalog_1','U0287D01') }} 
 ),

PAY_PERIODOut as (
	Select in_CURR_PP_FLAG, 
		PP_NUM, 
		PP_END_YEAR, 
		PP_START_DTE, 
		PP_END_DTE, 
		LV_NUM, 
		LV_YEAR, 
		PAY_DTE, 
		CURR_PP_FLAG
 from 
{{ source ('lkpSource_1_1','PAY_PERIOD') }}
 ),

exp_InitialOut as (
	
Select 
	SSN as SSN,
	 NAME as NAME,
	 CURRENT_ACCT as CURRENT_ACCT,
	 CURRENT_ORG as CURRENT_ORG,
	 FLSA_STATUS as FLSA_STATUS,
	 COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL,
	 COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED,
	 PP_END_DATE as PP_END_DATE,
	 DAILY_DATE_EARNED as DAILY_DATE_EARNED,
	 COMP_TIME_RATE as COMP_TIME_RATE,
	 COMP_TIME_HOURS as COMP_TIME_HOURS,
	 COMP_TIME_UNDEF as COMP_TIME_UNDEF,
	 'Y' as o_CURR_PP_FLAG,
	 DECODE ( TRUE,
	 IS_NUMERIC ( SSN ),
	 1,
	 0 ) as o_VALID_RECORD_FLAG
From SQ_U0287D01Out as SQ_U0287D01Out),

DSR_fil_Valid_RecordsOut as (
	
Select 
	exp_InitialOut.SSN as SSN,
	 exp_InitialOut.NAME as NAME,
	 exp_InitialOut.CURRENT_ACCT as CURRENT_ACCT,
	 exp_InitialOut.CURRENT_ORG as CURRENT_ORG,
	 exp_InitialOut.FLSA_STATUS as FLSA_STATUS,
	 exp_InitialOut.COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL,
	 exp_InitialOut.COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED,
	 exp_InitialOut.PP_END_DATE as PP_END_DATE,
	 exp_InitialOut.DAILY_DATE_EARNED as DAILY_DATE_EARNED,
	 exp_InitialOut.COMP_TIME_RATE as COMP_TIME_RATE,
	 exp_InitialOut.COMP_TIME_HOURS as COMP_TIME_HOURS,
	 exp_InitialOut.COMP_TIME_UNDEF as COMP_TIME_UNDEF,
	 exp_InitialOut.o_CURR_PP_FLAG as CURR_PP_FLAG,
	 exp_InitialOut.o_VALID_RECORD_FLAG as VALID_RECORD_FLAG
From exp_InitialOut as exp_InitialOut),

fil_Valid_RecordsOut as (
	Select 
 * 
From DSR_fil_Valid_RecordsOut as DSR_fil_Valid_RecordsOut Where VALID_RECORD_FLAG = TRUE),

lkp_PAY_PERIODOut as (
	Select 
	PP_NUM as PP_NUM,
	 PP_END_YEAR as PP_END_YEAR,
	 fil_Valid_Records.CURR_PP_FLAG AS in_CURR_PP_FLAG 
FROM
	fil_Valid_RecordsOut as fil_Valid_RecordsOut 
 LEFT JOIN PAY_PERIODOut
ON
	CURR_PP_FLAG = in_CURR_PP_FLAG
 
),

lkp_PAY_PERIOD_details as (
  Select
  	lkp_PAY_PERIODOut.PP_NUM as lkp_PP_NUM,
	 lkp_PAY_PERIODOut.PP_END_YEAR as lkp_PP_END_YEAR
  From lkp_PAY_PERIODOut as lkp_PAY_PERIODOut
),
dsJoiner_exp_Convert2Out as (
Select 
	fil_Valid_RecordsOut.SSN as SSN,
	 fil_Valid_RecordsOut.NAME as NAME,
	 fil_Valid_RecordsOut.CURRENT_ACCT as CURRENT_ACCT,
	 fil_Valid_RecordsOut.CURRENT_ORG as CURRENT_ORG,
	 fil_Valid_RecordsOut.FLSA_STATUS as FLSA_STATUS,
	 fil_Valid_RecordsOut.COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL,
	 fil_Valid_RecordsOut.COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED,
	 fil_Valid_RecordsOut.PP_END_DATE as PP_END_DATE,
	 fil_Valid_RecordsOut.DAILY_DATE_EARNED as DAILY_DATE_EARNED,
	 fil_Valid_RecordsOut.COMP_TIME_RATE as COMP_TIME_RATE,
	 fil_Valid_RecordsOut.COMP_TIME_HOURS as COMP_TIME_HOURS,
	 fil_Valid_RecordsOut.COMP_TIME_UNDEF as COMP_TIME_UNDEF,
	 lkp_PAY_PERIOD_details.lkp_PP_NUM as lkp_PP_NUM,
	 lkp_PAY_PERIOD_details.lkp_PP_END_YEAR as lkp_PP_END_YEAR,
	 fil_Valid_RecordsOut.jkey as jkey 
From
fil_Valid_RecordsOut as fil_Valid_RecordsOut inner JOIN lkp_PAY_PERIOD_details as lkp_PAY_PERIOD_details 
ON
lkp_PAY_PERIOD_details.jkey = fil_Valid_RecordsOut.jkey 
),

exp_ConvertOut as (
	
Select 
	SSN as SSN,
	 NAME as NAME,
	 CURRENT_ACCT as CURRENT_ACCT,
	 CURRENT_ORG as CURRENT_ORG,
	 FLSA_STATUS as FLSA_STATUS,
	 COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL,
	 COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED,
	 ( CASE WHEN TRY_TO_DATE ( PP_END_DATE,
	 'YYYYMMDD' ) THEN TRY_TO_DATE ( PP_END_DATE,
	 'YYYYMMDD' ) ELSE null END ) as o_PP_END_DATE,
	 ( CASE WHEN TRY_TO_DATE ( DAILY_DATE_EARNED,
	 'YYYYMMDD' ) THEN TRY_TO_DATE ( DAILY_DATE_EARNED,
	 'YYYYMMDD' ) ELSE null END ) as o_DAILY_DATE_EARNED,
	 COMP_TIME_RATE as COMP_TIME_RATE,
	 COMP_TIME_HOURS as COMP_TIME_HOURS,
	 COMP_TIME_UNDEF as COMP_TIME_UNDEF,
	 lkp_PP_END_YEAR as o_PP_END_YEAR,
	 lkp_PP_NUM as o_PP_NUM,
	 TO_DECIMAL ( TO_VARCHAR ( lkp_PP_END_YEAR ) || LPAD ( TO_VARCHAR ( lkp_PP_NUM ),
	 2,
	 '0' ) ) as o_PP_YEAR_NUM
From dsJoiner_exp_Convert2Out as dsJoiner_exp_Convert2Out),

DSR_exp_FinalOut as (
	
Select 
	exp_ConvertOut.o_PP_END_YEAR as PP_END_YEAR,
	 exp_ConvertOut.o_PP_NUM as PP_NUM,
	 exp_ConvertOut.o_PP_YEAR_NUM as PP_YEAR_NUM,
	 exp_ConvertOut.SSN as SSN,
	 exp_ConvertOut.NAME as NAME,
	 exp_ConvertOut.CURRENT_ACCT as CURRENT_ACCT,
	 exp_ConvertOut.CURRENT_ORG as CURRENT_ORG,
	 exp_ConvertOut.FLSA_STATUS as FLSA_STATUS,
	 exp_ConvertOut.COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL,
	 exp_ConvertOut.COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED,
	 exp_ConvertOut.o_PP_END_DATE as PP_END_DATE,
	 exp_ConvertOut.o_DAILY_DATE_EARNED as DAILY_DATE_EARNED,
	 exp_ConvertOut.COMP_TIME_RATE as COMP_TIME_RATE,
	 exp_ConvertOut.COMP_TIME_HOURS as COMP_TIME_HOURS,
	 exp_ConvertOut.COMP_TIME_UNDEF as COMP_TIME_UNDEF
From exp_ConvertOut as exp_ConvertOut),

exp_FinalOut as (
	
Select 
	PP_END_YEAR as PP_END_YEAR,
	 PP_NUM as PP_NUM,
	 PP_YEAR_NUM as PP_YEAR_NUM,
	 SSN as SSN,
	 NAME as NAME,
	 CURRENT_ACCT as CURRENT_ACCT,
	 CURRENT_ORG as CURRENT_ORG,
	 FLSA_STATUS as FLSA_STATUS,
	 COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL,
	 COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED,
	 PP_END_DATE as PP_END_DATE,
	 DAILY_DATE_EARNED as DAILY_DATE_EARNED,
	 COMP_TIME_RATE as COMP_TIME_RATE,
	 COMP_TIME_HOURS as COMP_TIME_HOURS,
	 COMP_TIME_UNDEF as COMP_TIME_UNDEF
From DSR_exp_FinalOut as DSR_exp_FinalOut)


Select 
	PP_END_YEAR as PP_END_YEAR,
	PP_NUM as PP_NUM,
	PP_YEAR_NUM as PP_YEAR_NUM,
	SSN as SSN,
	NAME as NAME,
	CURRENT_ACCT as CURRENT_ACCT,
	CURRENT_ORG as CURRENT_ORG,
	FLSA_STATUS as FLSA_STATUS,
	COMP_TIME_CUR_BAL as COMP_TIME_CUR_BAL,
	COMP_TIME_YEAR_EARNED as COMP_TIME_YEAR_EARNED,
	PP_END_DATE as PP_END_DATE,
	DAILY_DATE_EARNED as DAILY_DATE_EARNED,
	COMP_TIME_RATE as COMP_TIME_RATE,
	COMP_TIME_HOURS as COMP_TIME_HOURS,
	COMP_TIME_UNDEF as COMP_TIME_UNDEF 
From exp_FinalOut as exp_FinalOut