---------------------------------------------------------------------------------------------------
-- 插入APP端EVENT数据
---------------------------------------------------------------------------------------------------
INSERT INTO table fct_event_reg partition(date='{$date}')
SELECT
          a.terminal_id,
          a.app_version,
          CASE
                    WHEN b.event_id IS NULL THEN -1
                    WHEN a.app_version >= '3.2.3'
                    OR        b.event_id <> 279 THEN b.event_id
                    ELSE - 999
          end event_id,
          a.utm_id,
          a.site_id,
          a.gu_id,
          a.session_id,
          -- gognzi && lielie,过滤掉商品流坑位数据中非当天的数据
          case when b.event_type_id = 10 and a.activityname like '%click_cube%' then a.extend_params
          when b.event_type_id = 10 and
          (
            substr(from_unixtime(cast(get_json_object(server_jsonstr,'$._t') as bigint)), 1, 10) <> a.date
            or substr(from_unixtime(cast(get_json_object(server_jsonstr,'$._t') as bigint)), 1, 10) is null
            or get_json_object(server_jsonstr,'$.cid') is NULL
          ) then null
          else a.extend_params end event_value,
          CASE
                    WHEN p2.page_id IS NULL THEN - 999
                    -- H5页面按照url规则去解析，其他按照移动的规则去解析
                    WHEN P2.page_id in (154,289) and getpageid(a.pre_extends_param) > 0 THEN getpageid(a.pre_extends_param)
                    ELSE p2.page_id
          end ref_page_id,
          -- H5页面按照url规则去解析，其他按照移动的规则去解析
          case when P2.page_id in (154,289) then getdwpcpagevalue(a.pre_extends_param)
               else getdwmbpagevalue(
                    CASE
                              WHEN p2.page_type_id IN (1,4,10) THEN p2.page_value
                              -- mod by gognzi on 2016-04-24 17:10
                              -- app端品牌页面id = 250, pre_extends_param 格式：加密brandid_shopid_引流款id,或者 加密brandid_shopid
                              WHEN p2.page_id = 250 THEN getgoodsid(Split(a.pre_extends_param,'_')[0])
                              ELSE a.pre_extends_param
                    end, p2.page_type_id)
          end ref_page_value,
          a.site_id ref_site_id,
          0 key_page_id1,
          0 key_page_id2,
          0 key_page_id3,
          0 key_page_id4,
          a.user_id,
          CASE
                    WHEN p1.page_id IS NULL THEN - 999
-- H5页面按照url规则去解析，其他按照移动的规则去解析
                    WHEN P1.page_id in (154,289) and getpageid(a.page_extends_param) > 0 THEN getpageid(a.page_extends_param)
                    ELSE p1.page_id
          end page_id,
-- H5页面按照url规则去解析，其他按照移动的规则去解析
          case when P1.page_id in (154,289) then getdwpcpagevalue(a.page_extends_param)
               else getdwmbpagevalue(
                    CASE
                              when p1.page_id = 254 then cast(get_json_object(a.server_jsonstr, '$.cid') as int)
                              WHEN p1.page_type_id IN (1,4,10) THEN p1.page_value
                              -- mod by gognzi on 2016-04-24 17:10
                              -- app端品牌页面id = 250, page_extends_param 格式：加密brandid_shopid_引流款id,或者 加密brandid_shopid
                              WHEN p1.page_id = 250 THEN getgoodsid(split(a.page_extends_param,'_')[0])
                              ELSE a.page_extends_param
                    end , p1.page_type_id)
          end page_value,
          '' key_page_value1,
          '' key_page_value2,
          '' key_page_value3,
          '' key_page_value4,
          CASE
                    WHEN p1.page_id = 250 THEN getgoodsid(split(a.page_extends_param,'_')[1])
                    ELSE NULL
          end shop_id,
          CASE
                    WHEN p2.page_id = 250 THEN getgoodsid(split(a.pre_extends_param,'_')[1])
                    ELSE NULL
          end ref_shop_id,
          a.starttime,
          a.endtime,
          CASE WHEN P1.page_id not in (154,289) THEN p1.page_level_id
               WHEN getpageid(a.extend_params) in (34,65) then 2
               when getpageid(a.extend_params) = 10069 then 3
             else 0 end page_level_id,
          location,
          ctag,
          CASE
                    WHEN p1.page_id = 250 THEN getgoodsid(NVL(split(a.page_extends_param,'_')[2],''))
                    ELSE ''
          end hot_goods_id,
          a.rule_id,
          a.test_id,
          a.select_id,
          CASE
                    WHEN p1.page_id = 250 THEN getgoodsid(NVL(split(a.page_extends_param,'_')[2],''))
                    ELSE ''
          end page_lvl2_value,
          CASE
                    WHEN p2.page_id = 250 THEN getgoodsid(NVL(split(a.pre_extends_param,'_')[2],''))
                    ELSE ''
          end ref_page_lvl2_value,
-- 品宣页点击存储质检类型
          CASE
                    WHEN b.event_id = 360 THEN get_json_object(server_jsonstr,'$.item')
                    when b.event_id in (482,481,480,479) then get_json_object(server_jsonstr,'$._rmd')
                    ELSE ''
          end event_lvl2_value,
          0 as jpk,
          a.pit_type,
          a.sortdate,
          a.sorthour,
          a.lplid,
          a.ptplid,
          a.gid,
          a.ugroup
FROM      (
                 SELECT
                        CASE
                               WHEN lower(os) = 'android' THEN 2
                               WHEN lower(os) = 'ios' THEN 3
                               ELSE - 999
                        end terminal_id,
                        app_version,
                        utm utm_id,
                        CASE
                               WHEN lower(app_name) = 'jiu' THEN 2
                               WHEN lower(app_name) = 'zhe' THEN 1
                               ELSE - 999
                        end   site_id,
                        ticks gu_id,
                        CASE
                              -- gongzi
                               when activityname = 'click_cube_banner' and get_json_object(extend_params,'$.ads_id') is not null THEN concat('banner','::',get_json_object(extend_params,'$.ads_id'),'::',cube_position)
                               when activityname = 'click_cube_banner' and (get_json_object(extend_params,'$.ads_id') is null) THEN concat('banner','::',extend_params,'::',cube_position)
                               when get_json_object(server_jsonstr,'$.pit_info') is not null then get_json_object(server_jsonstr,'$.pit_info')
                               WHEN get_json_object(extend_params,'$.pit_info') is not null then get_json_object(extend_params,'$.pit_info')

                               when activityname = 'click_cube_block' and server_jsonstr<>'{}' then server_jsonstr
                               ELSE extend_params
                        end extend_params,
                        nvl(session_id, ticks) session_id,
                        CASE
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -6 then 'click_yugao_recommendation'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -100 then 'click_shoppingbag_recommendation'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -101 then 'click_orderdetails_recommendation'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -102 then 'click_detail_recommendation'
                               WHEN lower(activityname) <> 'click_navigation' THEN lower(activityname)
                               ELSE lower(concat(activityname, extend_params))
                        end for_eventid,
                        pre_extends_param,
                        endtime,
                        uid user_id,
                        page_extends_param,
                        starttime,
                        CASE
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -1 then 'page_taball'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -2 then 'page_tabpast_zhe'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -3 then 'page_tabcrazy_zhe'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) = -4 then 'page_tabjiu'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) in (-5,-6) then 'page_tabyugao'
                               when cast(get_json_object(server_jsonstr, '$.cid') as int) > 0 then 'page_tab'
                               WHEN cast(get_json_object(server_jsonstr, '$.cid') as int) = 0 and page_extends_param in ('all','past_zhe','crazy_zhe','jiu','yugao') then ''
                               when pagename = 'page_h5' and getpageid(page_extends_param) in (34,65,10069) then 'page_active'
                               WHEN lower(pagename) <> 'page_tab' THEN lower(pagename)
                               ELSE lower(concat(pagename, page_extends_param))
                        end for_pageid,
                        CASE
                               when pre_page = 'page_h5' and getpageid(pre_extends_param) in (34,65,10069) then 'page_active'
                               WHEN lower(pre_page) <> 'page_tab' THEN lower(pre_page)

                               ELSE lower(concat(pre_page, pre_extends_param))
                        end for_pre_pageid,
                        location,
                        c_label ctag,
                        CASE
                               WHEN substr(extend_params,1,1) = '{' then get_json_object(get_json_object(extend_params,'$.ab_info'),'$.rule_id')
                               else null
                        end                    rule_id,
                        CASE
                               WHEN substr(extend_params,1,1) = '{' then get_json_object(get_json_object(extend_params,'$.ab_info'),'$.test_id')
                               else null
                        end                    test_id,
                        -- ab测试，选择A还是B
                        CASE
                               WHEN substr(extend_params,1,1) = '{' then get_json_object(get_json_object(extend_params,'$.ab_info'),'$.select')
                               else null
                        end                    select_id,
                        activityname,
                        server_jsonstr,
                        -- bibi
                        cast(get_json_object(server_jsonstr, '$._pit_type') as int) pit_type,
                        concat_ws('-',substr(split(get_json_object(server_jsonstr, '$._gsort_key'),'_')[3],1,4),
                                      substr(split(get_json_object(server_jsonstr, '$._gsort_key'),'_')[3],5,2),
                                      substr(split(get_json_object(server_jsonstr, '$._gsort_key'),'_')[3],7,2)) sortdate,
                        cast(split(get_json_object(server_jsonstr, '$._gsort_key'),'_')[4] as int) sorthour,
                        cast(split(get_json_object(server_jsonstr, '$._gsort_key'),'_')[5] as int) lplid,
                        cast(split(get_json_object(server_jsonstr, '$._gsort_key'),'_')[6] as int) ptplid,
                        get_json_object(c_server, '$.gid') gid,
                        get_json_object(c_server, '$.ugroup') ugroup,
                        date,
                        hour
                 FROM   base.mb_event_log
                 WHERE  date = '{$date}'
                 -- 2016-03-20过滤掉API返回的埋点数据(太大了，每天1Y+，重刷3月16号后数据)
                 and    activityname <> 'collect_api_responsetime') a
LEFT JOIN dw.dim_event b
ON        b.terminal_lvl1_id = 2
AND       b.del_flag = 0
AND       a.for_eventid = concat(b.event_exp1, b.event_exp2)
LEFT JOIN dw.dim_page p1
ON        a.for_pageid = concat(p1.page_exp1, p1.page_exp2)
AND       p1.terminal_lvl1_id = 2
LEFT JOIN dw.dim_page p2
ON        a.for_pre_pageid = concat(p2.page_exp1, p2.page_exp2)
AND       p2.terminal_lvl1_id = 2;