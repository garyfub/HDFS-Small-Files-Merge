set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=8192;
set hive.groupby.skewindata =true;
set mapreduce.input.fileinputformat.split.maxsize=16000000;
set mapreduce.job.reduces=100;

use dw;

---------------------------------------------------------------------------------------------------
-- 计算path_list数据，并创建临时表
---------------------------------------------------------------------------------------------------
ALTER TABLE fct_path_list DROP IF EXISTS PARTITION (date='{$date}');

INSERT overwrite table fct_path_list partition(date='{$date}')
SELECT
gu_id,
starttime,
case when cast(entrance2_page_id as int) > 0 then entrance2_page_id else entrance2_page_id end last_entrance_page_id,
guide_page_id as last_guide_page_id,
before_goods_page_id as last_before_goods_page_id,
case when cast(entrance2_page_id as int) > 0 then  entrance2_page_value else entrance_page_value end last_entrance_page_value,
guide_page_value as last_guide_page_value,
before_goods_page_value as last_before_goods_page_value,
case when cast(entrance2_page_id as int) > 0 then entrance2_event_id else entrance_event_id end last_entrance_event_id,
case when cast(entrance2_page_id as int) > 0 then guide2_event_id else guide_event_id end last_guide_event_id,
before_goods_event_id last_before_goods_event_id,
case when cast(entrance2_page_id as int) > 0 then entrance2_event_value else entrance_event_value end last_entrance_event_value,
guide_event_value as last_guide_event_value,
before_goods_event_value as last_before_goods_event_value,
case when cast(entrance2_page_id as int) > 0 then entrance_starttime end last_entrance_timestamp,
guide_starttime as last_guide_timestamp,
before_goods_starttime as last_before_goods_timestamp,
guide2_page_id as guide_lvl2_page_id,
guide2_page_value as guide_lvl2_page_value,
guide2_event_id as guide_lvl2_event_id,
guide2_event_value as guide_lvl2_event_value,
guide2_starttime as guide_lvl2_timestamp,
0 guide_is_del,
0 guide_lvl2_is_del,
0 before_goods_is_del,
case when cast(entrance2_page_id as int) > 0 then entrance2_page_lvl2_value else entrance_page_lvl2_value end entrance_page_lvl2_value,
guide_page_lvl2_value,
guide2_page_lvl2_value as guide_lvl2_page_lvl2_value,
before_goods_page_lvl2_value,
case when cast(entrance2_page_id as int) > 0 then entrance2_event_lvl2_value else entrance_event_lvl2_value end entrance_event_lvl2_value,
guide_event_lvl2_value,
guide2_event_lvl2_value as guide_lvl2_event_lvl2_value,
before_goods_event_lvl2_value,
NUll rule_id,
case when cast(entrance2_page_id as int) > 0 then entrance2_test_id else entrance_test_id end test_id,
case when cast(entrance2_page_id as int) > 0 then entrance2_select_id else entrance_select_id end select_id,
case when cast(entrance2_page_id as int) > 0 then entrance2_pit_type else entrance_pit_type end last_entrance_pit_type,
case when cast(entrance2_page_id as int) > 0 then entrance2_sortdate else entrance_sortdate end last_entrance_sortdate,
case when cast(entrance2_page_id as int) > 0 then entrance2_sorthour else entrance_sorthour end last_entrance_sorthour,
case when cast(entrance2_page_id as int) > 0 then entrance2_lplid else entrance_lplid end last_entrance_lplid,
case when cast(entrance2_page_id as int) > 0 then entrance2_ptplid else entrance_ptplid end last_entrance_ptplid
from dw.fct_for_path_list
and date = '{$date}';