-- @Author: gongzi
-- @DateTime: 2016-09-26 17:18:54
-- @Description: 实时分区脚本

use ${dbName};

alter table dw_path_list_new add partition (date="${date}", gu_hash="0");
alter table dw_path_list_new add partition (date="${date}", gu_hash="1");
alter table dw_path_list_new add partition (date="${date}", gu_hash="2");
alter table dw_path_list_new add partition (date="${date}", gu_hash="3");
alter table dw_path_list_new add partition (date="${date}", gu_hash="4");
alter table dw_path_list_new add partition (date="${date}", gu_hash="5");
alter table dw_path_list_new add partition (date="${date}", gu_hash="6");
alter table dw_path_list_new add partition (date="${date}", gu_hash="7");
alter table dw_path_list_new add partition (date="${date}", gu_hash="8");
alter table dw_path_list_new add partition (date="${date}", gu_hash="9");
alter table dw_path_list_new add partition (date="${date}", gu_hash="a");
alter table dw_path_list_new add partition (date="${date}", gu_hash="b");
alter table dw_path_list_new add partition (date="${date}", gu_hash="c");
alter table dw_path_list_new add partition (date="${date}", gu_hash="d");
alter table dw_path_list_new add partition (date="${date}", gu_hash="e");
alter table dw_path_list_new add partition (date="${date}", gu_hash="f");
