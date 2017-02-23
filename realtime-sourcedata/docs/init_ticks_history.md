#### 数据解析思路

##### 数据初始化
1. ticks_history : 历史所有gu_id + app_name 对应的 utm_id 和 gu_create_time
2. dim_page : 解析 pageinfo 和 event 都需要用到。以 page_exp1+page_exp2 为key，其他字段为value（也是Map的形式）
3. dim_event :解析 event 需要用到。event_exp1+event_exp2 为key，其他字段为value（也是Map的形式）
4. `问题`：怎么定时去同步 dim_event 和 dim_page ?

##### base层 -> dw 层
由于 page 和 event 数据不同的kafka topic，只能分开解析，且解析逻辑也是不一样的；解析后的 page 和 event 写同一张表。包括公共字段和各自独有的字段。
最终的字段参见文件 com.juanpi.bi.bean.PageAndEvent
> 1. 首先从kafka解析源数据
  2. 得到的数据中查询ticks
  3. 解析 page 时，通过 page_exp1+page_exp2 组成的key去取值
  4. 解析 event 时，通过 event_exp1+event_exp2 组成的key去取值
  
```
use temp;
drop table temp.temp_gz_ticks_history;
create table temp_gz_ticks_history(
app_gu_id string,
utm string,
gu_create_time string);

insert into table temp.temp_gz_ticks_history
select
concat(a.gu_id,a.app_name) app_gu_id,
a.utm_id,
to_date(a.init_date)
FROM 
(
select
case when deviceid is not null then deviceid
else jpid end gu_id,
utm_id,
app_name,
init_date
from base.ticks_history_new1
) a;


-- hive下temp库temp_gz_ticks_history，hbase表名叫ticks_history ，列族用dw，rowkey用hive表里面的app_gu_id
```  