-- 涉案卖家其app调用${order_create}的先验概率
set odps.service.mode=all;
set odps.sql.mapper.merge.limit.size=2048;
set odps.sql.mapper.split.size=2048;


CREATE TABLE IF NOT EXISTS security_eco.adl_case_app_bayes_priorprob_by_seller
(
   event_date string comment '订单创建时间',
   seller_id  bigint comment '涉案卖家id',
   seller_nick string comment '涉案卖家nick',
   appkey  string comment 'top调用的appkey',  
   app_prior_prob  string comment 'app调用的先验概率'
) 
partitioned by (ds string);

alter table security_eco.adl_case_app_bayes_priorprob_by_seller
add if not exists partition (ds=${bizdate});

INSERT OVERWRITE TABLE security_eco.adl_case_app_bayes_priorprob_by_seller partition (ds=${bizdate})

-- SQL：app的先验概率
select event_date, seller_id,seller_nick, appkey, 
       round((app_so_cnt+0)/(total_call+0),5) as app_prior_prob
    from 
        (
         select tb1.event_date, tb1.seller_id, tb1.seller_nick,
                tb1.appkey,tb1.app_so_cnt,tb2.total_call
         from 
             (
              select event_date, seller_id, seller_nick,appkey, 
                     count(distinct order_id) as app_so_cnt 
              from  
                  (
                   select to_char(dateadd(GETDATE(), -1, 'dd'), 'yyyymmdd') as event_date,
                          appkey, title, seller_id, seller_nick,order_id
                   from   security_eco.adl_case_cross_isv_fdt_filter
                   where  ds <= ${base_date}       -- 20141025    --// 全量更新
                   group by to_char(dateadd(GETDATE(), -1, 'dd'), 'yyyymmdd'),
                            appkey, title,seller_id,seller_nick,order_id           
                  ) tb11
              group by event_date, seller_id,seller_nick, appkey    
             ) tb1
         left outer join
            ( -- 总app调用数
    		  select event_date, seller_id, seller_nick,sum(app_so_cnt) as total_call
    		  from
    			  (
                   select event_date, seller_id, seller_nick, appkey, count(distinct order_id) as app_so_cnt 
                   from  
                       (
                        select to_char(dateadd(GETDATE(), -1, 'dd'), 'yyyymmdd') as event_date,
                               appkey, title, seller_id, seller_nick,order_id
                        from   security_eco.adl_case_cross_isv_fdt_filter
                        where  ds <= ${base_date} -- 20141023    --// 全量更新
                        group by to_char(dateadd(GETDATE(), -1, 'dd'), 'yyyymmdd'),
                                 appkey, title,seller_id,seller_nick,order_id           
                       ) tb11
                   group by event_date, seller_id,seller_nick, appkey    			  			  
    			  ) sst
    		  group by event_date, seller_id,seller_nick
            ) tb2
         on (tb1.event_date = tb2.event_date and tb1.seller_id = tb2.seller_id)
        ) tb3
limit 999999999;


-- SQL：当日案件卖家app调用概率信息  中间tmp表
create table  security_eco.adl_case_app_bayes_priorprob_seller_mid  as
select  create_date, seller_id,seller_nick,
        case_appkey, app_base_prob,appkey,app_prior_prob,
        round((app_base_prob+0)*(app_prior_prob+0),3) as s_bayes_app_prob
from
(
select  case_prob.create_date, case_prob.seller_id,case_prob.seller_nick,
        case_prob.appkey as case_appkey, case_prob.app_base_prob,
        case when prior_prob.appkey is null then case_prob.appkey
             else prior_prob.appkey
        end as appkey,
        case when prior_prob.app_prior_prob is null then  1    --对于某卖家新增app的概率打标 
             else prior_prob.app_prior_prob
        end as app_prior_prob   
from 
   ( -- 涉案订单被app调用概率
    select a.create_date, a.seller_id,a.seller_nick, a.appkey, 
           round((a.new_app_so_ccnt+0)/(b.sum_oa_ccnt+0),5) as  app_base_prob
    from
       (
          select to_char(gmt_create,'yyyymmdd') as create_date,
                 seller_id,seller_nick, appkey, count(distinct order_id) new_app_so_ccnt
          from   security_eco.adl_case_cross_isv_fdt_filter
          where  ds >= ${case_date}  -- 20141026
                 and  to_char(gmt_create,'yyyymmdd') >= ${case_date}   -- 20141026
          group by to_char(gmt_create,'yyyymmdd'),
                   seller_id,seller_nick, appkey      
       ) a
    left outer join
       (
         select create_date, seller_id,seller_nick, sum(new_app_so_ccnt) as sum_oa_ccnt
         from 
             (
              select to_char(gmt_create,'yyyymmdd') as create_date,
                     seller_id,seller_nick, appkey, count(distinct order_id) new_app_so_ccnt
              from   security_eco.adl_case_cross_isv_fdt_filter
              where  ds >= ${case_date} -- 20141026 
                     and  to_char(gmt_create,'yyyymmdd') >= ${case_date} --20141026
              group by to_char(gmt_create,'yyyymmdd'),
                       seller_id,seller_nick, appkey  		
         	  ) aa	
    	 group by create_date, seller_id ,seller_nick
       ) b
    on (a.create_date = b.create_date and a.seller_id = b.seller_id)    
   )  case_prob
left outer join 
   ( -- 卖家其被调app先验涉案概率
    select *
    from  security_eco.adl_case_app_bayes_priorprob_by_seller  
    where ds=${bizdate}
    limit 999999999
   ) prior_prob
on (case_prob.seller_id = prior_prob.seller_id and case_prob.appkey = prior_prob.appkey )  
) tttt
limit 999999999;	


-- 涉案卖家其app调用${order_create}的后验概率
set odps.service.mode=all;
set odps.sql.mapper.merge.limit.size=2048;
set odps.sql.mapper.split.size=2048;

CREATE TABLE IF NOT EXISTS security_eco.adl_case_seller_app_bayesian_poster_prob
(
   event_date string comment '订单创建时间',
   seller_id  bigint comment '涉案卖家id',
   seller_nick string comment '涉案卖家nick',
   appkey  string comment 'top调用的appkey', 
   s_bayes_app_prob  string comment '卖家被app调用后验概率分子',
   s_bayes_prob  string comment '卖家被app调用的概率和',
   app_poster_prob  string comment 'app调用的后验概率'
) 
partitioned by (ds string);

alter table security_eco.adl_case_seller_app_bayesian_poster_prob
add if not exists partition (ds=${bizdate});

INSERT OVERWRITE TABLE security_eco.adl_case_seller_app_bayesian_poster_prob partition (ds=${bizdate})

-- SQL: 计算每个seller_id的app的后验概率
select o_app_bayes.create_date,
       o_app_bayes.seller_id,
       o_app_bayes.seller_nick,
       o_app_bayes.case_appkey,
       o_app_bayes.s_bayes_app_prob,
       o_bayes.s_bayes_prob,
       round((o_app_bayes.s_bayes_app_prob+0)/(o_bayes.s_bayes_prob+0),3) as app_poster_prob
       -- app后验概率      
from 
   ( -- Bayesian 分子
	select  *
	from  security_eco.adl_case_app_bayes_priorprob_seller_mid
   ) o_app_bayes
inner join 
   ( -- Bayesian 分母
    select create_date, seller_id ,seller_nick,
           sum(s_bayes_app_prob) as s_bayes_prob
    from 
        (
         select  *
         from  security_eco.adl_case_app_bayes_priorprob_seller_mid		
    	)  bayes_total_prob
    group by create_date, seller_id,seller_nick 
   ) o_bayes
on ( o_app_bayes.create_date = o_bayes.create_date and 
     o_app_bayes.seller_id = o_bayes.seller_id )
limit 999999999;

drop table  security_eco.adl_case_app_bayes_priorprob_seller_mid;


