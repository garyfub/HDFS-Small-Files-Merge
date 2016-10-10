
/*
-- 5级结构
entrance_page_id
entrance_page_value
entrance_page_lvl2_value
entrance_event_id
entrance_event_value
entrance_event_lvl2_value
entrance_starttime
entrance_loadtime
entrance2_page_id
entrance2_page_value
entrance2_page_lvl2_value
entrance2_event_id
entrance2_event_value
entrance2_event_lvl2_value
entrance2_starttime
entrance2_loadtime
guide_page_id
guide_page_value
guide_page_lvl2_value
guide_event_id
guide_event_value
guide_event_lvl2_value
guide_starttime
guide_loadtime
guide2_page_id
guide2_page_value
guide2_page_lvl2_value
guide2_event_id
guide2_event_value
guide2_event_lvl2_value
guide2_starttime
guide2_loadtime
before_goods_page_id
before_goods_page_value
before_goods_page_lvl2_value
before_goods_event_id
before_goods_event_value
before_goods_event_lvl2_value
before_goods_starttime
before_goods_loadtime
*/

-- 删除测试分区的数据
use test;
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="0");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="1");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="2");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="3");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="4");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="5");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="6");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="7");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="8");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="9");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="a");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="b");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="c");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="d");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="e");
alter table dw_path_list_new drop partition(date="2016-09-28", gu_hash="f");

alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="0");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="1");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="2");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="3");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="4");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="5");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="6");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="7");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="8");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="9");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="a");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="b");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="c");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="d");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="e");
alter table dw_path_list_new drop partition(date="2016-09-29", gu_hash="f");

select entrance_page_id,entrance_page_value, page_id, page_value, table_source from dw_path_list_new where
date = "2016-09-08"
and gu_hash="0"
and entrance_page_id=219
and page_id = 158
limit 20;

select * from test.dw_path_list_new where date = "2016-09-20" and gu_hash="0" limit 20;

select
page_level_id
,entrance_page_id
,entrance_page_value
from dw_path_list_new
where date = '2016-09-20'
and gu_hash = "8"
and gu_id='00000000-0000-0030-0000-0030012514a8'
and session_id='1474374017027_zhe_1474374016709'
limit 200;


select
gu_id
,table_source
,entrance_page_value
,entrance_event_value
from test.dw_path_list_new
where date = '2016-09-22'
and gu_hash = "8"
limit 200;

select
entrance_loadtime
,entrance2_loadtime
,guide_loadtime
,guide2_loadtime
,before_goods_loadtime
from test.dw_path_list_new
where date = '2016-09-26'
and gu_hash = "8"
and table_source = "mb_event"
limit 200;

select
page_id
,page_value
,entrance_page_id
,entrance_page_value
,table_source
from test.dw_path_list_new
where date = '2016-09-28'
and entrance_page_id = 254
and entrance_page_value = 61
and page_value = 61
limit 200
;


select
table_source
,page_id
,page_value
,entrance_page_id
,entrance_page_value
from test.dw_path_list_new
where date = '2016-09-28'
and table_source = "mb_event"
limit 200
;


select
event_value
from test.dw_path_list_new
where date = '2016-09-29'
and event_value like "%pit_info%"
limit 200
;
