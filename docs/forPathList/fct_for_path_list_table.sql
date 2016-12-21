-- hive dw.fct_for_path_list
use dw;
drop table fct_for_path_list;
create external table fct_for_path_list (
    -- 入口页
    entrance_page_id int,
    entrance_page_value string,
    entrance_page_lvl2_value string,
    entrance_event_id int,
    entrance_event_value string,
    entrance_event_lvl2_value string,
    entrance_starttime string,
    entrance_pit_type string,
    entrance_sortdate string,
    entrance_sorthour string,
    entrance_lplid string,
    entrance_ptplid string,
    entrance_select_id string,
    entrance_test_id string,
    entrance_ug_id string,
    -- 二级入口页
    entrance2_page_id int,
    entrance2_page_value string,
    entrance2_page_lvl2_value string,
    entrance2_event_id int,
    entrance2_event_value string,
    entrance2_event_lvl2_value string,
    entrance2_starttime string,
    entrance2_pit_type string,
    entrance2_sortdate string,
    entrance2_sorthour string,
    entrance2_lplid string,
    entrance2_ptplid string,
    entrance2_select_id string,
    entrance2_test_id string,
    entrance2_ug_id string,
    -- 导航页
    guide_page_id int,
    guide_page_value string,
    guide_page_lvl2_value string,
    guide_event_id int,
    guide_event_value string,
    guide_event_lvl2_value string,
    guide_starttime string,
    guide_pit_type string,
    guide_sortdate string,
    guide_sorthour string,
    guide_lplid string,
    guide_ptplid string,
    guide_select_id string,
    guide_test_id string,
    guide_ug_id string,
    -- 二级导航
    guide2_page_id int,
    guide2_page_value string,
    guide2_page_lvl2_value string,
    guide2_event_id int,
    guide2_event_value string,
    guide2_event_lvl2_value string,
    guide2_starttime string,
    guide2_pit_type string,
    guide2_sortdate string,
    guide2_sorthour string,
    guide2_lplid string,
    guide2_ptplid string,
    guide2_select_id string,
    guide2_test_id string,
    guide2_ug_id string,
    -- 前站
    before_goods_page_id int,
    before_goods_page_value string,
    before_goods_page_lvl2_value string,
    before_goods_event_id int,
    before_goods_event_value string,
    before_goods_event_lvl2_value string,
    before_goods_starttime string,
    before_goods_pit_type string,
    before_goods_sortdate string,
    before_goods_sorthour string,
    before_goods_lplid string,
    before_goods_ptplid string,
    before_goods_select_id string,
    before_goods_test_id string,
    before_goods_ug_id string,
    page_level_id int,
    gu_id string,
    page_id string,
    page_value string,
    page_lvl2_value string,
    event_id string,
    event_value string,
    event_lvl2_value string,
    rule_id string,
    test_id string,
    select_id string,
    starttime string,
    pit_type string,
    sortdate string,
    sorthour string,
    lplid string,
    ptplid string,
    ug_id string
)
PARTITIONED BY (`gu_hash` string)
row format delimited fields terminated by '\t'
lines terminated by '\n';

alter table fct_for_path_list add partition (gu_hash="0");
alter table fct_for_path_list add partition (gu_hash="1");
alter table fct_for_path_list add partition (gu_hash="2");
alter table fct_for_path_list add partition (gu_hash="3");
alter table fct_for_path_list add partition (gu_hash="4");
alter table fct_for_path_list add partition (gu_hash="5");
alter table fct_for_path_list add partition (gu_hash="6");
alter table fct_for_path_list add partition (gu_hash="7");
alter table fct_for_path_list add partition (gu_hash="8");
alter table fct_for_path_list add partition (gu_hash="9");
alter table fct_for_path_list add partition (gu_hash="a");
alter table fct_for_path_list add partition (gu_hash="b");
alter table fct_for_path_list add partition (gu_hash="c");
alter table fct_for_path_list add partition (gu_hash="d");
alter table fct_for_path_list add partition (gu_hash="e");
alter table fct_for_path_list add partition (gu_hash="f");