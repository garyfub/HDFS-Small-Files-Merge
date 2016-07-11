#### base.mb_pageinfo_log
```
hive> desc base.mb_pageinfo_log;
OK
ticks                   string                                      
session_id              string                                      
pagename                string                                      
starttime               bigint                                      
endtime                 bigint                                      
pre_page                string                                      
uid                     int                                         
extend_params           string                                      
app_name                string                                      
app_version             string                                      
os_version              string                                      
os                      string                                      
utm                     string                                      
source                  string                                      
pageid                  int                                         
starttime_origin        bigint                                      
endtime_origin          bigint                                      
pre_extend_params       string                                      
wap_url                 string                                      
wap_pre_url             string                                      
gj_page_names           string                                      
gj_ext_params           string                                      
deviceid                string                                      
init_date               string                                      
is_new                  int                                         
jpid                    string                                      
ip                      string                                      
to_switch               int                                         
location                string                                      
c_label                 string                                      
server_jsonstr          string                                      
c_server                string                                      
date                    string                                      
hour                    int                                         
                 
# Partition Information          
# col_name              data_type               comment             
                 
date                    string                                      
hour                    int    
```

#### base.mb_pageinfo
```
 hive> desc base.mb_pageinfo;    
 OK
 ticks                   string                                      
 session_id              string                                      
 pagename                string                                      
 starttime               bigint                                      
 endtime                 bigint                                      
 pre_page                string                                      
 uid                     int                                         
 extend_params           string                                      
 app_name                string                                      
 app_version             string                                      
 os_version              string                                      
 os                      string                                      
 utm                     string                                      
 source                  string                                      
 starttime_origin        bigint                                      
 endtime_origin          bigint                                      
 pre_extend_params       string                                      
 wap_url                 string                                      
 wap_pre_url             string                                      
 gj_page_names           string                                      
 gj_ext_params           string                                      
 deviceid                string                                      
 jpid                    string                                      
 ip                      string                                      
 to_switch               int                                         
 location                string                                      
 c_label                 string                                      
 server_jsonstr          string                                      
 c_server                string                                      
 date                    string                                      
 hour                    int                                         
                  
 # Partition Information          
 # col_name              data_type               comment             
                  
 date                    string                                      
 hour                    int  
```