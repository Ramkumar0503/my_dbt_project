-- models/informatica_transform.sql

SELECT 
  raw:"field1"::STRING AS field1,
  raw:"field2"::NUMBER AS field2,
  raw:"nested"."childField"::STRING AS child_field
FROM your_schema.stg_informatica_json