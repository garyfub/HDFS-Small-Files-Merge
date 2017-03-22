-- @Author: gongzi
-- @DateTime: 2016-09-26 17:18:54
-- @Description: 实时分区脚本
use ${dbName};
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="0");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="1");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="2");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="3");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="4");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="5");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="6");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="7");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="8");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="9");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="a");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="b");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="c");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="d");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="e");
alter table fct_path_list_real add if not exists partition (date="${date}", gu_hash="f");