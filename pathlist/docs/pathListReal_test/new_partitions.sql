-- @Author: gongzi
-- @DateTime: 2016-09-26 17:18:54
-- @Description: 实时分区脚本
use test;
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="0");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="1");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="2");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="3");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="4");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="5");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="6");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="7");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="8");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="9");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="a");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="b");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="c");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="d");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="e");
alter table test_fct_path_list_real add if not exists partition (date="2017-03-26", gu_hash="f");