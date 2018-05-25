
add jar /project/edu_edcc/xrding/jars/hive-udf/hive-stored-demo.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS test_table_hive_stored_demo
(
  user_id BIGINT,
  event STRING
)
ROW FORMAT SERDE 'com.iflytek.edmp.demo.SerDeDemo'
STORED AS
  INPUTFORMAT 'com.iflytek.edmp.demo.InputFormatDemo'
  OUTPUTFORMAT 'com.iflytek.edmp.demo.OutputFormatDemo'
LOCATION '/project/edu_edcc/xrding/data/test_table_hive'