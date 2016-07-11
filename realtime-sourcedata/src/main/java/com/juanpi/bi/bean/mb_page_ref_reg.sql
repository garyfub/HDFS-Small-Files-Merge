INSERT INTO table fct_page_ref_reg partition (date='{$date}')
SELECT
          CASE
                    WHEN p1.page_id IS NULL THEN -1
-- H5页面按照url规则去解析，其他按照移动的规则去解析
                    WHEN P1.page_id in (154,289) and getpageid(a.extend_params) > 0 THEN getpageid(a.extend_params)
                    ELSE p1.page_id
          end page_id,
-- H5页面按照url规则去解析，其他按照移动的规则去解析
          case when P1.page_id in (154,289) then getdwpcpagevalue(a.extend_params)
               else getdwmbpagevalue(
                        CASE
                              when p1.page_id = 254 then a.extend_params
                              WHEN p1.page_type_id IN (1 ,4,10) THEN p1.page_value
                              -- mod by gognzi on 2016-04-24 17:10
                              -- app端品牌页面id = 250,extend_params格式：加密brandid_shopid_引流款id,或者 加密brandid_shopid
                              WHEN p1.page_id = 250 THEN getgoodsid(split(a.extend_params,'_')[0])
                              ELSE a.extend_params
                        end,p1.page_type_id) end page_value,
          CASE
                    WHEN p2.page_id IS NULL THEN -1
-- H5页面按照url规则去解析，其他按照移动的规则去解析
                    WHEN P2.page_id in (154,289) and getpageid(a.pre_extend_params) > 0 THEN getpageid(a.pre_extend_params)
                    ELSE p2.page_id
          end ref_page_id,
-- H5页面按照url规则去解析，其他按照移动的规则去解析
          case when P2.page_id in (154,289) then getdwpcpagevalue(a.pre_extend_params)
               else getdwmbpagevalue(
                    CASE
                        when p2.page_id = 254 then a.pre_extend_params
                        WHEN p2.page_type_id IN (1 ,4,10) THEN p2.page_value
                        -- app端品牌页面id = 250, pre_extend_params 格式：加密brandid_shopid_引流款id,或者 加密brandid_shopid
                        WHEN p2.page_id = 250 THEN getgoodsid(split(a.pre_extend_params,'_')[0])
                        ELSE a.pre_extend_params
                    end, p2.page_type_id)
          end ref_page_value,
          a.site_id,
          a.ref_site_id,
          a.terminal_id,
          a.utm_id,
          a.app_version,
          a.gu_id,
          a.session_id,
          0 as key_page_id1,
          0 as key_page_id2,
          0 as key_page_id3,
          0 as key_page_id4,
          '' as key_page_value1,
          '' as key_page_value2,
          '' as key_page_value3,
          '' as key_page_value4,
          a.source,
          uid user_id,
          a.hour,
          CASE
                    WHEN p1.page_id = 250 THEN getgoodsid(split(a.extend_params,'_')[1])
                    ELSE NULL
          end shop_id,
          CASE
                    WHEN p2.page_id = 250 THEN getgoodsid(split(a.pre_extend_params,'_')[1])
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
                    WHEN p1.page_id = 250 THEN getgoodsid(NVL(split(a.extend_params,'_')[2],''))
                    ELSE ''
          end hot_goods_id,
-- 二级页面值(品牌页：引流款ID|||)
          CASE
                    WHEN p1.page_id = 250 THEN getgoodsid(NVL(split(a.extend_params,'_')[2],''))
                    when P1.page_id in (154,289) and getpageid(a.extend_params) = 10104 then getskcid(a.extend_params)
                    when P1.page_id in (154,289) and getpageid(a.extend_params) = 10102 then getshopid(a.extend_params)
                    -- 'page_temai_orderdetails'
                    WHEN P1.page_id = 169 then get_json_object(a.server_jsonstr,'$.order_status')
                    ELSE ''
          end page_lvl2_value,
          CASE
                    WHEN p2.page_id = 250 THEN getgoodsid(NVL(split(a.pre_extend_params,'_')[2],''))
                    when P2.page_id in (154,289) and getpageid(a.pre_extend_params) = 10104 then getskcid(a.pre_extend_params)
                    when P2.page_id in (154,289) and getpageid(a.pre_extend_params) = 10102 then getshopid(a.pre_extend_params)
                    -- 'page_temai_orderdetails'
                    WHEN P2.page_id = 169 then get_json_object(a.server_jsonstr,'$.order_status')
                    ELSE ''
          end ref_page_lvl2_value,
          0 jpk,
          ip,
          a.gu_create_time,
          a.url,
          a.urlref,
          a.deviceid,
          a.to_switch,
          a.pit_type,
          a.sortdate,
          a.sorthour,
          a.lplid,
          a.ptplid,
          a.gid,
          a.ugroup
FROM      (
-- page_h5活动页的链接要按照活动页去解析
               SELECT   pagename,
                        extend_params,
                        pre_page,
                        pre_extend_params,
                        CASE
                               WHEN lower(app_name) = 'jiu' THEN 2
                               WHEN lower(app_name) = 'zhe' THEN 1
                               ELSE - 999
                        end site_id,
                        CASE
                               WHEN lower(app_name) = 'jiu' THEN 2
                               WHEN lower(app_name) = 'zhe' THEN 1
                               ELSE - 999
                        end ref_site_id,
                        CASE
                               WHEN lower(os) = 'android' THEN 2
                               WHEN lower(os) = 'ios' THEN 3
                               ELSE - 999
                        end terminal_id,
                        utm utm_id,
                        app_version,
                        ticks gu_id,
                        CASE
                               WHEN session_id IS NULL
                               OR     length(session_id) = 0
                               OR     session_id = 'null' THEN ticks
                               ELSE session_id
                        end session_id,
                        CASE
                               when lower(pagename) = 'page_tab' and extend_params<9999999 and extend_params > 0 then 'page_tab'
                                  -- page_name='page_tab' and extend_params 是数字，那么代表是tab页，而数字对应dw.dim_front_cate 的id表示类目页
                               WHEN lower(pagename) <> 'page_tab' THEN lower(pagename)
                               when lower(pagename) = 'page_tab'
                                  -- and app_version > '3.4.3'
                                  and get_json_object(server_jsonstr, '$.cid') is not null
                                  and get_json_object(server_jsonstr, '$.cid') < 0 then lower(concat(pagename, get_json_object(server_jsonstr, '$.cid')))
                               ELSE lower(concat(pagename, extend_params))

                        end for_pageid,
                        CASE
                               when lower(pre_page) = 'page_tab' and pre_extend_params<9999999 and pre_extend_params > 0 then 'page_tab'
                               WHEN lower(pre_page) <> 'page_tab' THEN lower(pre_page)
                               when lower(pre_page) = 'page_tab'
                                  -- and app_version > '3.4.3' 为了区分购物袋和空购物袋页  page_id=10106
                                  and get_json_object(server_jsonstr, '$.cid') is not null
                                  and get_json_object(server_jsonstr, '$.cid') < 0 then lower(concat(pre_page, get_json_object(server_jsonstr, '$.cid')))
                               ELSE lower(concat(pre_page, pre_extend_params))
                        end for_pre_pageid,
                        CASE
                               WHEN source LIKE '%订单%' THEN '用户个人订单信息推送'
                               WHEN source LIKE '%售后%'
                               OR     source LIKE '%退货%'
                               OR     source LIKE '%退款%' THEN '用户售后信息推送'
                               WHEN source LIKE '%你好%' THEN '用户个人消息通知推送'
                               WHEN source LIKE '%有货就赶紧抢%' THEN '有货提醒'
                               WHEN source LIKE '%收藏的商品%' THEN '用户收藏商品最新消息推送'
                               WHEN source NOT LIKE '%push%'
                               OR     source IS NULL
                               OR     length(source) = 0
                               OR     source = 'null' THEN '未知'
                               ELSE substr(source,6)
                        end source,
                        uid,
                        starttime,
                        endtime,
                        hour,
                        location,
                        c_label ctag,
                        ip,
                        init_date gu_create_time,
                        wap_url url,
                        wap_pre_url urlref,
                        deviceid,
                        to_switch,
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
                        get_json_object(c_server, '$.ugroup') ugroup
                 FROM   base.mb_pageinfo_log a
                 WHERE  date = '{$date}') a
LEFT JOIN dw.dim_page p1
ON        a.for_pageid = concat(p1.page_exp1, p1.page_exp2)
AND       p1.terminal_lvl1_id = 2
LEFT JOIN dw.dim_page p2
ON        a.for_pre_pageid = concat(p2.page_exp1, p2.page_exp2)
AND       p2.terminal_lvl1_id = 2;
