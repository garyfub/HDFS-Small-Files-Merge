use test;

drop table test_fct_path_list_real;

CREATE EXTERNAL TABLE test_fct_path_list_real(
entrance struct<page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,pit_type:string,pit_value:string,pit_no:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,gid:string,testid:string,selectid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>,
entrance2 struct<page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,pit_type:string,pit_value:string,pit_no:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,gid:string,testid:string,selectid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>,
guide struct<page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,pit_type:string,pit_value:string,pit_no:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,gid:string,testid:string,selectid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>,
guide2 struct<page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,pit_type:string,pit_value:string,pit_no:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,gid:string,testid:string,selectid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>,
before_goods struct<page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,pit_type:string,pit_value:string,pit_no:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,gid:string,testid:string,selectid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>,
  `gu_id` string,
  `user_id` string,
  `utm` string,
  `gu_create_time` string,
  `session_id` string,
  `terminal_id` int,
  `app_version` string,
  `site_id` int,
  `ref_site_id` int,
  `ctag` string,
  `location` string,
  `jpk` int,
  `ugroup` string,
  `date_id` string,
  `hour` string,
  `page_id` int,
  `page_value` string,
  `ref_page_id` int,
  `ref_page_value` string,
  `shop_id` string,
  `ref_shop_id` string,
  `page_level_id` int,
  `starttime` string,
  `endtime` string,
  `hot_goods_id` string,
  `page_lvl2_value` string,
  `ref_page_lvl2_value` string,
  `pit_type` int,
  `sortdate` string,
  `sorthour` string,
  `lplid` string,
  `ptplid` string,
  `gid` string,
  `table_source` string,
  `source` string,
  `ip` string,
  `url` string,
  `urlref` string,
  `deviceid` string,
  `to_switch` string,
  `event_id` string,
  `event_value` string,
  `event_lvl2_value` string,
  `rule_id` string,
  `test_id` string,
  `select_id` string,
  `x_page_value` string,
  `ref_x_page_value` string)
PARTITIONED BY (
  `date` string,
  `gu_hash` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t'
COLLECTION ITEMS TERMINATED BY '#'
lines terminated by '\n';

-- stored as orc tblproperties ("orc.compress"="SNAPPY")

use temp;
ALTER TABLE path_list_real CHANGE entrance entrance struct<
page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,testid:string,selectid:string,pittype:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>;

ALTER TABLE path_list_real CHANGE entrance2 entrance struct<
page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,testid:string,selectid:string,pittype:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>;

ALTER TABLE path_list_real CHANGE guide guide struct<
page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,testid:string,selectid:string,pittype:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>;

ALTER TABLE path_list_real CHANGE guide2 guide2 struct<
page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,testid:string,selectid:string,pittype:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>;

ALTER TABLE path_list_real CHANGE before before struct<
page_id:int,page_value:string,page_lvl2_value:string,event_id:int,event_value:string,event_lvl2_value:string,starttime:string,loadtime:string,testid:string,selectid:string,pittype:string,sortdate:string,sorthour:string,lplid:string,ptplid:string,ug_id:string,rule_id:string,x_page_value:string,ref_x_page_value:string>;


