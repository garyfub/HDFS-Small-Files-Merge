use temp;
drop table tmp_gongzi_event_reg_mr;
CREATE TABLE `tmp_gongzi_event_reg_mr`(
`page_level_id` string,
`gu_id` string,
`page_id` string,
`page_value` string,
`page_lvl2_value` string,
`event_id` string,
`event_value` string,
`event_lvl2_value` string,
`rule_id` string,
`test_id` string,
`select_id` string,
`starttime` string,
`pit_type` string,
`sortdate` string,
`sorthour` string,
`lplid` string,
`ptplid` string
  )
PARTITIONED BY (`gu_hash` string)
-- LOCATION 'hdfs://nameservice1/user/hadoop/offline_reg_mr/'
;

use temp;
drop table tmp_gongzi_page_ref_reg_mr;
CREATE TABLE `tmp_gongzi_page_ref_reg_mr`(
`page_level_id` string,
`gu_id` string,
`page_id` string,
`page_value` string,
`page_lvl2_value` string,
`event_id` string,
`event_value` string,
`event_lvl2_value` string,
`rule_id` string,
`test_id` string,
`select_id` string,
`starttime` string,
`pit_type` string,
`sortdate` string,
`sorthour` string,
`lplid` string,
`ptplid` string
  )
PARTITIONED BY (`gu_hash` string)
-- LOCATION 'hdfs://nameservice1/user/hadoop/offline_reg_mr/'
;