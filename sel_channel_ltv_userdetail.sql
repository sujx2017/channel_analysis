# channel_analysis
create table sujx.channel_ltv_userdetail
(userid int,
agent int,
reg_date string,
recharge_cent_3 int,
recharge_cent_7 int,
recharge_cent_15 int,
recharge_cent_30 int,
recharge_cent_45 int,
recharge_cent_60 int,
recharge_cent_90 int,
recharge_cent_120 int,
recharge_cent_180 int,
recharge_cent_270 int,
recharge_cent_365 int
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

insert overwrite table sujx.channel_ltv_userdetail
partition(year,month,day)
select
    a.userid,
    a.agent,
    a.reg_date,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 2 then b.total_recharge_cents end,0)) as recharge_cent_3,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 6 then b.total_recharge_cents end,0)) as recharge_cent_7,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 14 then b.total_recharge_cents end,0)) as recharge_cent_15,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 29 then b.total_recharge_cents end,0)) as recharge_cent_30,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 44 then b.total_recharge_cents end,0)) as recharge_cent_45,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 59 then b.total_recharge_cents end,0)) as recharge_cent_60,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 89 then b.total_recharge_cents end,0)) as recharge_cent_90,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 119 then b.total_recharge_cents end,0)) as recharge_cent_120,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 179 then b.total_recharge_cents end,0)) as recharge_cent_180,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 269 then b.total_recharge_cents end,0)) as recharge_cent_270,
    sum(coalesce(case when datediff(b.recharge_date,a.reg_date) between 0 and 364 then b.total_recharge_cents end,0)) as recharge_cent_365,
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
        concat(year,month,day) between '20160301' and '20170630'
    )a
    left join
    (select
        user_id,
        total_recharge_cents,
        concat_ws('-',year,month,day) as recharge_date
    from
        mid.mid_gamematchplat_day_recharge_r
    where
        concat(year,month,day) between '20160301' and '20170725'
    )b
    on a.userid = b.user_id
group by
    a.userid,
    a.agent,
    a.reg_date,
    a.year,
    a.month,
    a.day;
