#!/bin/sh

echo "Start task..."


# 用户城市ip dingwei 分布

sql1="set hive.map.aggr=true;
set mapred.job.priority=VERY_HIGH;
use udw2ares;
select  cuid, city  from (
  select  cuid, substring(location_city, 1, length(location_city)-1) as city 
  from  insight_lbs_mobile_push_mid 
  where event_day=20141226 and event_product='lbs' and cuid is not null and cuid != '' ) t
where city is not null and city != ''
group by cuid ,city;"

 /home/map/tools/online_hive/bin/hive -e "$sql1" > /home/map/lukaimin/data_file/tuisong_city_iploc.txt
