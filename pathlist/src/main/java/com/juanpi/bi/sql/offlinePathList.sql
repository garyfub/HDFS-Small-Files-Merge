set hive.auto.convert.join=false;
set hive.ignore.mapjoin.hint=false;
set hive.groupby.skewindata=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=2000;

use temp;
insert overwrite table tmp_gongzi_event_reg_mr partition (gu_hash)
select
  x.page_level_id,
  x.gu_id,
  x.page_id,
  x.page_value,
  x.page_lvl2_value,
  x.event_id,
  x.event_value,
  x.event_lvl2_value,
  x.rule_id,
  x.test_id,
  x.select_id,
  x.starttime,
  x.pit_type,
  x.sortdate,
  x.sorthour,
  x.lplid,
  x.ptplid,
  x.gu_hash
from (
    SELECT
        case when event_id in (481,10041) and page_id in (158, 167, 250, 26) then 1 else page_level_id end page_level_id,
        gu_id,
        page_id,
        page_value,
        page_lvl2_value,
        event_id,
        event_value,
        event_lvl2_value,
        rule_id,
        test_id,
        select_id,
        starttime,
        pit_type,
        sortdate,
        sorthour,
        lplid,
        ptplid,
        -- 以gu_id 最后一个字母作为分区字段
        lower(substring(gu_id, -1)) as gu_hash
    FROM  dw.fct_event_reg
    where date = '2016-11-10'
        and gu_id is not null
        and length(gu_id) > 0
        and substring(gu_id, -1) rlike '[A-Za-z0-9]'

    UNION ALL

    SELECT
      page_level_id,
      gu_id,
      page_id,
      page_value,
      page_lvl2_value,
      '' event_id,
      '' event_value,
      '' event_lvl2_value,
      '' rule_id,
      '' test_id,
      '' select_id,
      starttime,
      '' pit_type,
      '' sortdate,
      '' sorthour,
      '' lplid,
      '' ptplid,
      -- 以gu_id 最后一个字母作为分区字段
      lower(substring(gu_id, -1)) as gu_hash
    FROM  dw.fct_page_ref_reg
    where date = '2016-11-10'
      and gu_id is not null
      and length(gu_id) > 0
      and substring(gu_id, -1) rlike '[A-Za-z0-9]'
    ) x;


gu_id                   string
endtime                 starttime
last_entrance_page_id   int
last_guide_page_id      int
last_before_goods_page_id       int
last_entrance_page_value        string
last_guide_page_value   string
last_before_goods_page_value    string
last_entrance_event_id  int
last_guide_event_id     int
last_before_goods_event_id      int
last_entrance_event_value       string
last_guide_event_value  string
last_before_goods_event_value   string
last_entrance_timestamp bigint
last_guide_timestamp    bigint
last_before_goods_timestamp     bigint
guide_lvl2_page_id      int
guide_lvl2_page_value   string
guide_lvl2_event_id     int
guide_lvl2_event_value  string
guide_lvl2_timestamp    bigint
guide_is_del            int
guide_lvl2_is_del       int
before_goods_is_del     int
entrance_page_lvl2_value        string
guide_page_lvl2_value   string
guide_lvl2_page_lvl2_value      string
before_goods_page_lvl2_value    string
entrance_event_lvl2_value       string
guide_event_lvl2_value  string
guide_lvl2_event_lvl2_value     string
before_goods_event_lvl2_value   string
rule_id                 string
test_id                 string
select_id               string
last_entrance_pit_type  int
last_entrance_sortdate  string
last_entrance_sorthour  int
last_entrance_lplid     int
last_entrance_ptplid    int
date                    string