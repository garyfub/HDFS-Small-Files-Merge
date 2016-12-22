use ${dbName};

alter table ${tableName} partition (gu_hash="0") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=0';
alter table ${tableName} partition (gu_hash="1") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=1';
alter table ${tableName} partition (gu_hash="2") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=2';
alter table ${tableName} partition (gu_hash="3") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=3';
alter table ${tableName} partition (gu_hash="4") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=4';
alter table ${tableName} partition (gu_hash="5") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=5';
alter table ${tableName} partition (gu_hash="6") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=6';
alter table ${tableName} partition (gu_hash="7") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=7';
alter table ${tableName} partition (gu_hash="8") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=8';
alter table ${tableName} partition (gu_hash="9") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=9';
alter table ${tableName} partition (gu_hash="a") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=a';
alter table ${tableName} partition (gu_hash="b") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=b';
alter table ${tableName} partition (gu_hash="c") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=c';
alter table ${tableName} partition (gu_hash="d") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=d';
alter table ${tableName} partition (gu_hash="e") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=e';
alter table ${tableName} partition (gu_hash="f") set location 'hdfs://nameservice1/user/hadoop/dw_realtime/fct_for_path_list_offline/gu_hash=f';