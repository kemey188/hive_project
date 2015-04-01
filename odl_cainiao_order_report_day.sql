-- //菜鸟web后台订单流量监控日报基础表
use security_eco;
set odps.service.mode=all;
CREATE TABLE IF NOT EXISTS security_eco.odl_cainiao_order_report_day
(
   event_date  bigint comment '动作日期',
   src_ip  string comment '拉取订单的ip',
   city  string comment 'ip所在城市',
   order_id_cnt_uv  bigint comment '拉取订单数',
   owner_id_cnt  bigint comment  '覆盖卖家数',
   new_owner_cnt  bigint comment  '日环比新增覆盖卖家数',
   new_owner_ratio  string comment  '覆盖卖家中新增卖家占比',
   diff_owner_cnt  bigint comment  '拉订单id != 卖家id的订单数',
   max_order_url  string comment  '订单流量来源最大的url',
   diff_order_cnt_uv  bigint comment  '订单涨（跌）幅',
   diff_ratio  string comment  '订单涨（跌）幅比'
) 
partitioned by (ds string);

alter table security_eco.odl_cainiao_order_report_day
add if not exists partition (ds=${bizdate});

INSERT OVERWRITE TABLE security_eco.odl_cainiao_order_report_day partition (ds=${bizdate})
select tta.event_date,   
       tta.src_ip,             -- //拉取订单的ip
       tta.city,               -- //ip所在城市
       tta.order_id_cnt_uv,    -- //拉取订单数
       tta.owner_id_cnt,       -- //覆盖卖家数
       ttb.new_owner_cnt,      -- //日环比新增覆盖卖家数
       case when (tta.owner_id_cnt = 0 or ttb.new_owner_cnt is null) then 'MISS_OWNER_INFO'
            when (tta.owner_id_cnt <> 0 and ttb.new_owner_cnt is not null) 
                 then concat(to_char(round(ttb.new_owner_cnt/tta.owner_id_cnt,2)*100),'%') 
       end as new_owner_ratio, -- //覆盖卖家中新增卖家占比
       tta.diff_owner_cnt,     -- //拉订单id != 卖家id的订单数
       tta.max_order_url,      -- //订单流量来源最大的url
       tta.diff_order_cnt_uv,  -- //订单涨（跌）幅
       tta.diff_ratio          -- //订单涨（跌）幅比
from
    (       
      select t2.event_date, t1.src_ip, t1.city, t1.order_id_cnt_uv, t1.owner_id_cnt, t1.max_order_url,
             t1.diff_owner_cnt, t2.diff_order_cnt_uv, t2.diff_ratio
      from odl_cainiao_order_monitor_day t1 
      left outer join
      (
         select t.src_ip,
                t.event_date,
                (t.order_id_cnt_uv - t.max_order_cnt_uv) as diff_order_cnt_uv,
                case when t.max_order_cnt_uv = 0 then '+∞'
                     else concat(to_char(round((t.order_id_cnt_uv-t.max_order_cnt_uv)/t.max_order_cnt_uv,2)*100),'%') 
                end as diff_ratio     
         from
              (
                select b.src_ip, b.event_date, b.order_id_cnt_uv,
                       case when a.max_order_cnt_uv is null then 0
                            else a.max_order_cnt_uv
                       end as  max_order_cnt_uv
                from        
                     (
                       select src_ip, event_date, order_id_cnt_uv 
                       from security_eco.odl_cainiao_order_tsm_day
                       where event_date=${bizdate}
                       limit 999999999
                     ) b 
                left outer join
                     (
                       select src_ip, max(order_id_cnt_uv+0) as max_order_cnt_uv 
                       from security_eco.odl_cainiao_order_tsm_day
                       where event_date >= ${date_start} and 
                             event_date < ${bizdate}
                       group by src_ip
                       limit 999999999
                     ) a
                on b.src_ip = a.src_ip
              ) t 
         limit 999999999
      )  t2
      on t1.src_ip = t2.src_ip and t1.ds = t2.event_date
      where t1.ds = ${bizdate}
      limit 999999999
    ) tta
left outer join
    (
      select bt.src_ip, 
             count(distinct bt.owner_id) as new_owner_cnt  -- //日环比新增覆盖卖家数
      from 
            (
              select dst_addr as src_ip,
                     owner_id 
               from  security_eco.odl_orderid_event_view
              where  type = 'order_id' and dest_type = 'IP' and 
                     src_channel = 'CAINIAO_WEB' and  
                     ext_type = 'CAINIAO_WEB_URL' and   
                     ds=${yesterday} -- //前一天
              group by dst_addr, owner_id
              limit 999999999         
            ) at
       right outer join
            (
              select dst_addr as src_ip,
                     owner_id 
               from  security_eco.odl_orderid_event_view
              where  type = 'order_id' and dest_type = 'IP' and 
                     src_channel = 'CAINIAO_WEB' and  
                     ext_type = 'CAINIAO_WEB_URL' and   
                     ds=${bizdate} -- //当日
              group by dst_addr, owner_id
              limit 999999999         
            ) bt    
      on at.src_ip = bt.src_ip and at.owner_id = bt.owner_id
      where at.src_ip is null
      group by  bt.src_ip 
      limit 999999999    
     ) ttb
on tta.src_ip = ttb.src_ip
where (tta.order_id_cnt_uv >= 5000 or 
      tta.owner_id_cnt    >= 800  or
      tta.diff_owner_cnt  >= 100  or
      ttb.new_owner_cnt   >= 50) and
      tta.event_date is not null
order by tta.order_id_cnt_uv desc
limit 999999999;
