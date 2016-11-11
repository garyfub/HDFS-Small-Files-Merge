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
last_entrance_page_id,
last_guide_page_id,
last_before_goods_page_id,
last_entrance_page_value,
last_guide_page_value,
last_before_goods_page_value,
last_entrance_event_id,
last_guide_event_id,
last_before_goods_event_id,
last_entrance_event_value,
last_guide_event_value,
last_before_goods_event_value,
last_entrance_timestamp,
last_guide_timestamp,
last_before_goods_timestamp,
guide_lvl2_page_id,
guide_lvl2_page_value,
guide_lvl2_event_id,
guide_lvl2_event_value,
guide_lvl2_timestamp,
guide_is_del,
guide_lvl2_is_del,
before_goods_is_del,
entrance_page_lvl2_value,
guide_page_lvl2_value,
guide_lvl2_page_lvl2_value,
before_goods_page_lvl2_value,
entrance_event_lvl2_value,
guide_event_lvl2_value,
guide_lvl2_event_lvl2_value,
before_goods_event_lvl2_value,
rule_id,
test_id,
select_id,
last_entrance_pit_type,
last_entrance_sortdate,
last_entrance_sorthour,
last_entrance_lplid,
last_entrance_ptplid,
date
from dw.fct_path_list_offline;