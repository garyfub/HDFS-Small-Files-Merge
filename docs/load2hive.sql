
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
-- 或者
use test;
alter table dw_path_list_new_jobs add partition (date="2016-08-31", gu_hash="0");
alter table dw_path_list_new_jobs partition(date="2016-08-31", gu_hash="0") set location 'hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list_jobs/date=2016-08-31/gu_hash=0/';
-- 看看效果
 > show partitions test.dw_path_list_new;