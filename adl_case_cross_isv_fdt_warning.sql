-- 案件告警数据
use security_eco;
set odps.service.mode=all;
set odps.sql.mapjoin.memory.max = 1024;

CREATE TABLE IF NOT EXISTS security_eco.adl_case_cross_isv_fdt_warning
(
   event_date string comment '订单创建时间',
   appkey  string comment 'appkey',
   title  string comment 'appkey-title',
   appkey_total_order_day  bigint comment '该appkey当日涉案订单数',
   total_order_day  bigint comment '当日案件订单总数',
   caseo_ratio  string comment '案件占比',
   total_seller_day  bigint comment '涉及卖家总数',
   earliest_call_time  string comment '最早调用时间',
   avg_call_time  string comment '平均调用时间',
   median_call_time  string comment '调用时间中位数'
) 
partitioned by (ds string);

alter table security_eco.adl_case_cross_isv_fdt_warning
add if not exists partition (ds=${bizdate});

INSERT OVERWRITE TABLE security_eco.adl_case_cross_isv_fdt_warning partition (ds=${bizdate})

-- SQL
select taa.event_date,
            taa.appkey,
            taa.title,
            taa.appkey_total_order_day,
            taa.total_order_day,
            taa.caseo_ratio,
            taa.total_seller_day,
            taa.earliest_call_time,
            taa.avg_call_time,
            taa.median_call_time 
from
    (
     select event_date,
            appkey,
            title,
            appkey_total_order_day,
            total_order_day,
            caseo_ratio,
            total_seller_day,
            earliest_call_time,
            avg_call_time,
            median_call_time 
     from security_eco.adl_case_appkey_report_day
     where ds = ${bizdate}
     limit 999999999   
    ) taa
inner join 
    (
     select appkey,title
     from 
         (
          select appkey,title
          from  security_eco.adl_case_appkey_report_day
          where  (total_seller_day >= 100  or split_part(caseo_ratio,'%',1) >= 20) and 
                 event_date = ${base_date}  -- // ds-2
                 and ds = ${bizdate}
          union all
          select appkey,title
          from 
              (
               select case when beforr.appkey is null then afterr.appkey
                           else beforr.appkey
                      end as appkey,
                      case when beforr.title is null then afterr.title
                           else beforr.title
                      end as title,
                      case when beforr.appkey is null then '+∞'
                           when afterr.appkey is null then 0
                           else round((split_part(afterr.caseo_ratio,'%',1)-split_part(beforr.caseo_ratio,'%',1))/split_part(beforr.caseo_ratio,'%',1),2)
                      end as raise_ratio
               from 
                   (
                    select appkey, title,caseo_ratio
                    from  security_eco.adl_case_appkey_report_day
                    where event_date = ${base_date}  and split_part(caseo_ratio,'%',1)  >= 15 
                          and ds = ${bizdate}
                    limit 999999999
                   ) beforr
               join 
                   (
                    select appkey, title,caseo_ratio 
                    from  security_eco.adl_case_appkey_report_day
                    where event_date = ${base_date_tomr}  and split_part(caseo_ratio,'%',1)  >= 15 
                          and  ds= ${bizdate}
                    limit 999999999    -- bsae_date + 1    
                   ) afterr
               on  (beforr.appkey = afterr.appkey and beforr.title = afterr.title)
              ) tb1
          where raise_ratio = '+∞' or 
                raise_ratio >= 0.2
         ) tb
     group by appkey,title
    ) tbb
on taa.appkey = tbb.appkey and taa.title = tbb.title
where taa.appkey <> '21600715' -- 过滤内部应用'淘宝交易'
order by taa.event_date desc, taa.caseo_ratio desc , taa.total_seller_day desc
limit 999999999;

