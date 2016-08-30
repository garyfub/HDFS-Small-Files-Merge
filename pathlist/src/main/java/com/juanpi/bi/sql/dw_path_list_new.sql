use test;
drop table dw_path_list_new;
create external table dw_path_list_new (
    -- 入口页
    entrance_page_id int,
    entrance_page_value string,
    entrance_page_lvl2_value string,
    entrance_event_id int,
    entrance_event_value string,
    entrance_event_lvl2_value string,
    entrance_starttime string,
    entrance_loadtime string,
    -- 二级入口页
    entrance2_page_id int,
    entrance2_page_value string,
    entrance2_page_lvl2_value string,
    entrance2_event_id int,
    entrance2_event_value string,
    entrance2_event_lvl2_value string,
    entrance2_starttime string,
    entrance2_loadtime string,
    -- 导航页
    guide_page_id int,
    guide_page_value string,
    guide_page_lvl2_value string,
    guide_event_id int,
    guide_event_value string,
    guide_event_lvl2_value string,
    guide_starttime string,
    guide_loadtime string,
    -- 二级导航
    guide2_page_id int,
    guide2_page_value string,
    guide2_page_lvl2_value string,
    guide2_event_id int,
    guide2_event_value string,
    guide2_event_lvl2_value string,
    guide2_starttime string,
    guide2_loadtime string,
    -- 前站
    before_goods_page_id int,
    before_goods_page_value string,
    before_goods_page_lvl2_value string,
    before_goods_event_id int,
    before_goods_event_value string,
    before_goods_event_lvl2_value string,
    before_goods_starttime string,
    before_goods_loadtime string,
    gu_id string,
    user_id string,
    utm string,
    gu_create_time string,
    session_id string,
    terminal_id int,
    app_version string,
    site_id int,
    ref_site_id int,
    ctag string,
    location string,
    jpk int,
    ugroup string,
    date_id string,
    hour string,
    page_id int,
    page_value string,
    ref_page_id int,
    ref_page_value string,
    shop_id string,
    ref_shop_id string,
    page_level_id int,
    starttime string,
    endtime string,
    hot_goods_id string,
    page_lvl2_value string,
    ref_page_lvl2_value string,
    pit_type int,
    sortdate string,
    sorthour string,
    lplid string,
    ptplid string,
    gid string,
    table_source string,
    source string,
    ip string,
    url string,
    urlref string,
    deviceid string,
    to_switch string,
    event_id string,
    event_value string,
    event_lvl2_value string,
    rule_id string,
    test_id string,
    select_id string
)
PARTITIONED BY (`date` string, `gu_hash` string)
row format delimited fields terminated by '\t'
lines terminated by '\n';
-- 指定了这个貌似没啥用
-- location 'hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list';

-- 这两个没法用
load data inpath 'hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=0/part-r-00000' overwrite into table dw_path_list_new partition (date="2016-08-18",gu_hash="0");
load data inpath '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=0/part-r-00000' overwrite into table dw_path_list_new partition (date="2016-08-18",gu_hash="0");

-- 此方案是ok的
use test;
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="0") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=0/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="1") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=1/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="2") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=2/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="3") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=3/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="4") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=4/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="5") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=5/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="6") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=6/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="7") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=7/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="8") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=8/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="9") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=9/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="a") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=a/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="b") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=b/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="c") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=c/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="d") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=d/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="e") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=e/';
alter table dw_path_list_new add partition (date="2016-08-18", gu_hash="f") location '/user/hadoop/gongzi/dw_real_path_list/date=2016-08-18/gu_hash=f/';
-- 看看效果
 > show partitions test.dw_path_list_new;
