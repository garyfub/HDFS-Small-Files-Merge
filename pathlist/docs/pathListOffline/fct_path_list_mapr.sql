set hive.auto.convert.join=false;
set hive.ignore.mapjoin.hint=false;
set hive.groupby.skewindata=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=2000;

use dw;
-- 删除时间分区
alter table fct_path_list_mapr DROP if exists partition (gu_hash="0");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="1");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="2");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="3");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="4");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="5");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="6");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="7");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="8");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="9");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="a");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="b");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="c");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="d");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="e");
alter table fct_path_list_mapr DROP if exists partition (gu_hash="f");

insert overwrite table fct_path_list_mapr partition (gu_hash)
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
  x.ug_id,
  x.gu_hash
from (
    SELECT
        case when event_id in (481,10041) and page_id in (158, 167, 250, 26) then 1 else page_level_id end page_level_id,
        gu_id,
        page_id,
        case when length(page_value) > 0 then page_value else NUll end page_value,
        case when length(page_lvl2_value) > 0 then page_lvl2_value else NUll end page_lvl2_value,
        event_id,
        case when length(event_value) > 0 then event_value else NUll end event_value,
        case when length(event_lvl2_value) > 0 then event_lvl2_value else NUll end event_lvl2_value,
        NULL rule_id,
        case when length(test_id) > 0 then test_id else NUll end test_id,
        case when length(select_id) > 0 then select_id else NUll end select_id,
        starttime,
        case when length(pit_type) > 0 then pit_type else NUll end pit_type,
        case when length(sortdate) > 0 then sortdate else NUll end sortdate,
        case when length(sorthour) > 0 then sorthour else NUll end sorthour,
        case when length(lplid) > 0 then lplid else NUll end lplid,
        case when length(ptplid) > 0 then ptplid else NUll end ptplid,
        -- 以gu_id 最后一个字母作为分区字段
        lower(substring(gu_id, -1)) as gu_hash,
        case when length(ug_id) > 0 then ug_id else NUll end ug_id
    FROM  dw.fct_event_reg
    where date = '${date}'
        and gu_id is not null
        and length(gu_id) > 0
        and substring(gu_id, -1) rlike '[A-Za-z0-9]'

    UNION ALL

    SELECT
      page_level_id,
      gu_id,
      page_id,
      case when length(page_value) > 0 then page_value else NUll end page_value,
      case when length(page_lvl2_value) > 0 then page_lvl2_value else NUll end page_lvl2_value,
      NUll event_id,
      NUll event_value,
      NUll event_lvl2_value,
      NUll rule_id,
      NUll test_id,
      NUll select_id,
      starttime,
      NUll pit_type,
      NUll sortdate,
      NUll sorthour,
      NUll lplid,
      NUll ptplid,
      -- 以gu_id 最后一个字母作为分区字段
      lower(substring(gu_id, -1)) as gu_hash,
      NULL ug_id
    FROM  dw.fct_page_ref_reg
    where date = '${date}'
      and gu_id is not null
      and length(gu_id) > 0
      and substring(gu_id, -1) rlike '[A-Za-z0-9]'
    ) x;