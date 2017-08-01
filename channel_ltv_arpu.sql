# channel_analysis
create table sujx.channel_ltv_arpu
(userid int,
agent int,
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

insert overwrite table sujx.channel_ltv_arpu
partition(year,month,day)
select
    a.userid,
    b.agent,
    coalesce(c.total_recharge_cents,0) as total_recharge_cents,
    a.year as year,
    a.month as month,
    a.day as day
from
    (select
        userid,
        year,
        month,
        day
    from
        mid.mid_gamematchplat_day_userplatonline_r_v1
    where
        concat(year,month,day) between '20160601' and '20170630'
    )a
    left join
    (select
        userid,
        agent
    from
        cdd.cdd_acc_register_20151127_txt
    )b
    on a.userid = b.userid
    left join
    (select
        user_id,
        total_recharge_cents,
        year,
        month,
        day
    from
        mid.mid_gamematchplat_day_recharge_r
    where
        concat(year,month,day) between '20160601' and '20170630'
    )c
    on a.userid = c.user_id and concat(a.year,a.month,a.day) =  concat(c.year,c.month,c.day);
