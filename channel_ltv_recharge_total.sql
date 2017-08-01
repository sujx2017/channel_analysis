# channel_analysis
create table sujx.channel_ltv_recharge_total
(userid int,
agent int,
reg_date string,
login_days int,
total_recharge_cents int
)
partitioned by (year string,month string,day string)
row format delimited
fields terminated by ','
stored as textfile;

set  hive.exec.dynamic.partition=true; 
set  hive.exec.dynamic.partition.mode=nonstrict;
set  hive.exec.max.dynamic.partitions=1000;
set  hive.exec.max.dynamic.partitions.pernode=1000;
set  hive.exec.max.created.files=655350;

insert overwrite table sujx.channel_ltv_recharge_total
partition(year,month,day)
select
    a.userid,
    a.agent,
    a.reg_date,
    coalesce(b.login_days,0) as login_days,
    coalesce(c.total_recharge_cents,0) as total_recharge_cents,
    a.year as year,
    a.month as momth,
    a.day as day
from
    (select
        userid,
        agent,
        concat_ws('-',year,month,day) as reg_date,
        year,
        month,
        day
    from
        cdd.cdd_acc_register_20151127_txt
    where
        concat(year,month,day) between '20160601' and '20170630'
    )a
    left join
    (select
        userid,
        count(userid) as login_days
    from
        mid.mid_gamematchplat_day_userplatonline_r_v1
    where
        concat(year,month,day) between '20160601' and '20170630'
    group by
        userid
    )b
    on a.userid = b.userid
    left join
    (select
        user_id,
        sum(total_recharge_cents) as total_recharge_cents
    from
        mid.mid_gamematchplat_day_recharge_r
    where
        concat(year,month,day) between '20160601' and '20170630'
    group by
        user_id
    )c
    on a.userid = c.user_id;
