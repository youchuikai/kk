 -- 活动数据
 select a.date,
        a.terminal_id,
        count(distinct a.gu_id) act_uv,
        sum(a.pv) act_pv
 from
 (select
    date,
    terminal_id,
    gu_id,
    pv
 from dw.fct_page
 where date>='2015-11-06' and date<='2015-11-11' and  page_id in(34,64,65,154)
 ) a 
 inner join
 (select gu_id,date,terminal_id from dw.fct_session b
  where date>='2015-11-06' and date<='2015-11-11' and consume_status='C3') b 
 on a.date=b.date and a.gu_id=b.gu_id and a.terminal_id=b.terminal_id
 group by a.date,a.terminal_id
 order by a.date,a.terminal_id;

##############################################################################
-- 全平台数据
 select a.date,
        a.terminal_id,
        count(distinct a.gu_id) act_uv,
        sum(a.pv) act_pv
 from
 (select
    date,
    terminal_id,
    gu_id,
    pv
 from dw.fct_page
 where date>='2015-11-06' and date<='2015-11-11'
 ) a 
 inner join
 (select gu_id,date,terminal_id from dw.fct_session b
  where date>='2015-11-06' and date<='2015-11-11' and consume_status='C3') b 
 on a.date=b.date and a.gu_id=b.gu_id and a.terminal_id=b.terminal_id
 group by a.date,a.terminal_id
 order by a.date,a.terminal_id;


 -- 在线人数
 select a.date,
        a.terminal_id,
        a.hour,
        count(distinct a.gu_id) act_uv
 from
 (select
    date,
    terminal_id,
    gu_id,
    pv,
    hour
 from dw.fct_page
 where date>='2015-11-06' and date<='2015-11-11' and  page_id in(34,64,65,154)
 ) a 
 inner join
 (select gu_id,date,terminal_id from dw.fct_session b
  where date>='2015-11-06' and date<='2015-11-11' and consume_status='C3') b 
 on a.date=b.date and a.gu_id=b.gu_id and a.terminal_id=b.terminal_id
 group by a.date,a.terminal_id,a.hour
 order by a.date,a.terminal_id,a.hour;

 **************************************
 fct_ordr_path

select a.date,a.terminal_id, from fct_ordr_path a where a.date>='2015-11-06' and a.date<='2015-11-11'
