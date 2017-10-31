-----------------------MONITORING EDW --------------------------------------------

select 
case when task in ('CS5_TE_S2C_FCT_CDR_PREPAID_SNAPD_SDP_DUMP_MA') then '1.1.A'
when task in ('STD_TE_I2S_POSTPAID_SUBSCRIBER_BASE_PULL') then '1.1.B'
when task in ('STD_C2E_FCT_PREPAID_SNAPD') then '1.1.B'
when task in ('STD_C2C_FCT_MSISDN_PREPAID_SUMD') then '3.1.A'
when task in ('STD_C2C_FCT_PREPAID_FIRST')  then '3.2.A'
when task in ('STD_C2C_FCT_PREPAID_LAST') then '3.2.A'
when task in ('STD_C2C_FCT_PREPAID_FIRST_ACTIV') then '1.1.A'
when task in ('STD_TE_S2S_INT_SUBSCRIBER') then '1.1.A'
when task in ('STD_TE_S2S_INT_SUBSCRIBER_MISC') then '1.1.A'
when task in ('STD_DIM_SUBSCRIBER') then '1.2.A'
when task in ('STD_BRT_SUBSCRIBER') then '1.3.A'
when task in ('STD_TE_S2C_FCT_CDR_INTERCONNECT') then '4.1.A'
when task in ('STD_C2C_FCT_MSISDN_USAGE_INTERCONNECT_FIRST') then '5.1.A'
when task in ('STD_C2C_FCT_MSISDN_USAGE_INTERCONNECT_LAST') then '5.1.A'
when task in ('STD_C2C_FCT_PVT_CDR_PREPAID_RATED_DED') then '4.3.A'
when task in ('STD_C2C_FCT_CDR_RATED_COMB') then '4.3.A:'
when task in ('STD_C2C_FCT_CDR_TRANS_COMB') then '4.3.A'
when task in ('STD_C2C_FCT_MSISDN_BASE_STATION_SUMD') then '4.4.A'
when task in ('STD_C2C_FCT_MSISDN_GEOGRAPHY_SUMD') then '4.5.A'
when task in ('STD_C2C_FCT_MSISDN_RATED_SUMD') then '4.4.A'
when task in ('STD_C2C_FCT_CDR_RATED_SUMD') then '4.4.A'
when task in ('STD_C2C_FCT_MSISDN_USAGE_PREPAID_FIRST') then '4.5.A'
when task in ('STD_C2C_FCT_MSISDN_USAGE_PREPAID_LAST') then '4.5.A'
when task in ('STD_C2C_FCT_MSISDN_USAGE_TAP_IN_FIRST') then ''
when task in ('STD_C2C_FCT_MSISDN_USAGE_TAP_IN_LAST') then ''
when task in ('STD_C2C_FCT_SUBS_EVENT_SUMD') then '3.3.A'
when task in ('STD_C2C_FCT_SUBS_EVENT_FIRST') then '3.3.A'
when task in ('STD_C2C_FCT_SUBS_EVENT_LAST') then '3.3.A'
when task in ('STD_C2C_FCT_SUBS_RGE_MM_LAST') then '3.3.A'
when task in ('STD_C2C_FCT_SUBS_RGE_LAST') then '2.1.G'
when task in ('STD_C2C_FCT_SUBS_RGE_MISC_LAST') then '3.3.A'
when task in ('STD_C2C_FCT_SUBS_SNAPD') then '6.1.A'
when task in ('DAY DURATION') then '6.1.A'
when task in ('STD_C2C_FCT_SUBS_RGE_CALC') then '6.2.A'
when task in ('STD_C2E_FCT_SUBSCRIBER_SNAPD') then '6.2.A'
when task in ('STD_C2E_FCT_SUBS_FULL_SUMD') then '6.2.A'
when task in ('STD_E2E_FRU_SUBSCRIBER_SNAPD') then '6.2.A'
when task in ('STD_E2E_FRU_SUBSCRIBER_HL_SNAPD') then '6.2.A'
when task in ('STD_E2E_FRU_SUBSCRIBER_SNAPD_HL') then '6.2.A'
when task in ('STD_C2E_FCT_SUBSCRIBER_SNAPM') then '6.2.A'
when task in ('STD_C2C_FCT_CDR_INTERCONNECT_SUMD') then '6.2.B'
when task in ('STD_C2E_FCT_INTERCONNECT_EXPENSE') then '6.2.B'
when task in ('STD_C2E_FCT_INTERCONNECT_REVENUE') then '6.2.B'
when task in ('STD_C2E_FCT_USAGE_DATA_OUT') then '6.2.C'
when task in ('STD_C2E_FCT_USAGE_POSTPAID_IN') then '6.2.C'
when task in ('STD_C2E_FCT_USAGE_POSTPAID_OUT') then '6.2.C'
when task in ('STD_C2E_FCT_USAGE_PREPAID_IN') then '6.2.C'
when task in ('STD_C2E_FCT_USAGE_PREPAID_OUT') then '6.2.C'
end section, 
src_dtk,my_dtk,run_status,task,duration,
start_date,end_date, rows_loaded,dw_run_id,dw_batch_id,dw_copied_batch_id,dw_task_id,rows_error,diy_parameter,
direct_exec_sql,task_business_area, range_cnt, avg_dur, max_dur, min_rps, avg_rps, max_rps
from (
select
src_dtk,my_dtk,run_status,task,duration,rows_loaded,dw_run_id,dw_batch_id,dw_copied_batch_id,start_date,end_date,dw_task_id,rows_error,diy_parameter,
direct_exec_sql,task_business_area,
range_cnt, avg_dur, max_dur, min_rps, avg_rps, max_rps
from (
select
c.dw_date_key src_dtk,coalesce(to_char(b.dw_date_key) ,a.diy_parameter,'_') my_dtk, a.run_status,task,
numtodsinterval(end_date-start_date,'day') duration,
rows_loaded,a.dw_run_id,a.dw_batch_id,c.dw_copied_batch_id,
to_char(start_date,'yyyymmdd hh24:mi:ss') start_date, 
to_char(end_date,'yyyymmdd hh24:mi:ss')end_date,
a.dw_task_id, rows_error,diy_parameter,direct_exec_sql
,t.task_business_area,
CONCURNCY,  priority,  node
,d.range_cnt, d.avg_duration avg_dur, d.max_duration max_dur, d.min_rows_persec min_rps, d.avg_rows_persec avg_rps, d.max_rows_persec max_rps
from bib_meta.dw_rt_runs a
left outer join (select  MAX_FILE_LOAD_CONCURRENCY CONCURNCY, flex1 priority, flex2 node, dw_task_id t_id,task_business_area from bib_meta.dw_tasks ) t on a.dw_task_id=t.t_id
left outer join bib_meta.dw_rt_batches b on a.dw_batch_id=b.dw_batch_id
left outer join BIB_META.dw_rt_batches c on b.dw_copied_batch_id=c.dw_batch_id
left outer join (SELECT dw_run_id, count(1) range_cnt, min(ROUND ((24*60*60*(A.end_date-A.start_date)) / 60, 2)) min_DURATION, ROUND(avg( ( (24*60*60*(A.end_date - A.start_date)) / 60)),2) avg_DURATION,
max(ROUND ((24*60*60*(A.end_date - A.start_date))/60, 2)) max_DURATION, min(ROUND (rows_loaded/ NULLIF (24*60*60*(A.end_date - A.start_date), 0),0))  min_rows_persec,
ROUND(avg( (rows_loaded/NULLIF (24*60*60*(A.end_date - A.start_date), 0))) ,0)avg_rows_persec,max(ROUND (rows_loaded/ NULLIF (24*60*60*(A.end_date - A.start_date), 0),0)) max_rows_persec
  FROM DW_RT_RANGE_RUNS A GROUP BY DW_RUN_ID) d on a.dw_run_id = d.dw_run_id
where 1=1  --13,917,739 
and run_status not in('CANCELLED')
--and (rows_loaded>0 and run_status in ('CLOSED','SUCCESS'))
and task in ('STD_TE_S2S_INT_SUBSCRIBER','STD_BRT_SUBSCRIBER','STD_DIM_SUBSCRIBER', 'STD_C2C_FCT_CDR_PREPAID_SNAPD',
'STD_C2C_FCT_PREPAID_FIRST','STD_C2C_FCT_PREPAID_LAST',
'STD_C2C_FCT_CDR_RATED_COMB', 'STD_C2C_FCT_SUBS_SNAPD','STD_C2E_FCT_SUBSCRIBER_SNAPM',
'STD_WAIT_SUBSCRIBER_SNAPM','STD_TD_SUBSCRIBER','STD_C2C_FCT_CDR_TRANS_COMB',
'STD_C2C_FCT_MSISDN_PREPAID_SUMD','STD_C2C_FCT_SUBS_RGE_CALC','STD_C2E_FCT_SUBSCRIBER_SNAPD',
'STD_C2E_FCT_SUBS_FULL_SUMD','STD_E2E_FRU_SUBSCRIBER_SNAPD','STD_E2E_FRU_SUBSCRIBER_HL_SNAPD','STD_E2E_FRU_SUBSCRIBER_SNAPD_HL',
'STD_WAIT_SUBS_SNAPD',
'CS5_TE_S2C_FCT_CDR_PREPAID_SNAPD_SDP_DUMP_MA','STD_TE_S2S_INT_SUBSCRIBER_MISC','STD_TE_S2C_FCT_CDR_INTERCONNECT_DIY_PAR','STD_C2C_FCT_MSISDN_USAGE_INTERCONNECT_FIRST',
'STD_C2C_FCT_PVT_CDR_PREPAID_RATED_DED','STD_C2C_FCT_MSISDN_BASE_STATION_SUMD','STD_C2C_FCT_MSISDN_GEOGRAPHY_SUMD','STD_C2C_FCT_MSISDN_RATED_SUMD','STD_C2C_FCT_CDR_RATED_SUMD',
'STD_C2C_FCT_MSISDN_USAGE_PREPAID_FIRST','STD_C2C_FCT_MSISDN_USAGE_PREPAID_LAST','STD_C2C_FCT_MSISDN_USAGE_TAP_IN_FIRST','STD_C2C_FCT_MSISDN_USAGE_TAP_IN_LAST',
'STD_C2C_FCT_SUBS_EVENT_SUMD','STD_C2C_FCT_SUBS_EVENT_FIRST','STD_C2C_FCT_SUBS_EVENT_LAST','STD_C2C_FCT_SUBS_RGE_LAST','STD_C2C_FCT_SUBS_RGE_MISC_LAST','STD_C2C_FCT_PREPAID_FIRST_ACTIV',
'STD_TE_I2S_POSTPAID_SUBSCRIBER_BASE_PULL','STD_C2E_FCT_PREPAID_SNAPD',
'STD_C2C_FCT_SUBS_RGE_MM_LAST','STD_E2E_FRU_SUBSCRIBER_SNAPD',
'STD_C2E_FCT_INTERCONNECT_EXPENSE','STD_C2E_FCT_INTERCONNECT_REVENUE', 'STD_C2C_FCT_CDR_INTERCONNECT_SUMD',
'STD_C2E_FCT_USAGE_DATA_OUT','STD_C2E_FCT_USAGE_POSTPAID_IN','STD_C2E_FCT_USAGE_POSTPAID_OUT',
'STD_C2E_FCT_USAGE_PREPAID_IN','STD_C2E_FCT_USAGE_PREPAID_OUT'
)
order by case when run_status='RUNNING' then 1 when run_status in ('FAILED','WAITING','PENDING') then 2 when run_status='RUNABLE' then 3 else 4 end,
2 desc ,
start_date desc) main
where main.my_dtk  
--= '20170727'
= (SELECT to_char(MAX_KEY) FROM DW_TASKS WHERE TASK = 'STD_DEP_EDW_PROCESS_DATE')
--to_char(TO_DATE('20161209','YYYYMMDD'),'yyyymmdd') --Insert the STD_DEP_EDW_PROCESS_DATE you would like to have a look at.
union all
select
null src_dtk,
(SELECT to_char(MAX_KEY) FROM DW_TASKS WHERE TASK = 'STD_DEP_EDW_PROCESS_DATE') my_dtk,
case when end_run.end_date is null then 'RUNNING' else 'CLOSED' end run_status,
start_run.task,
numtodsinterval(
case when end_run.end_date is null then sysdate else end_run.end_date end 
- start_run.start_date,'day') duration,null rows_loaded,null dw_run_id,null dw_batch_id,null dw_copied_batch_id,
to_char(start_run.start_date,'yyyymmdd hh24:mi:ss') start_date,
to_char(end_run.end_date,'yyyymmdd hh24:mi:ss') end_date,
null dw_task_id,null rows_error,null diy_parameter,
null direct_exec_sql,null task_business_area,
null range_cnt, null avg_dur, null max_dur, null min_rps, null avg_rps, null max_rps
from 
(
select 'DAY DURATION' task, start_date from (
select start_date, 
rank() over (order by start_date, rownum) rnk
from bib_meta.dw_rt_runs a
left outer join (select dw_task_id t_id,task_business_area from bib_meta.dw_tasks ) t on a.dw_task_id=t.t_id
left outer join bib_meta.dw_rt_batches b on a.dw_batch_id=b.dw_batch_id
left outer join BIB_META.dw_rt_batches c on b.dw_copied_batch_id=c.dw_batch_id
where 1=1
and run_status not in('CANCELLED')
and task in ('STD_C2C_FCT_MSISDN_PREPAID_SUMD','STD_C2C_FCT_PREPAID_FIRST','STD_C2C_FCT_PREPAID_LAST','STD_C2C_FCT_CDR_RATED_COMB' )
and coalesce(to_char(b.dw_date_key) ,a.diy_parameter,'_')=(SELECT to_char(MAX_KEY) FROM DW_TASKS WHERE TASK = 'STD_DEP_EDW_PROCESS_DATE')) where rnk=1) start_run,
( select end_date from bib_meta.dw_rt_runs a
left outer join (select dw_task_id t_id,task_business_area from bib_meta.dw_tasks ) t on a.dw_task_id=t.t_id
left outer join bib_meta.dw_rt_batches b on a.dw_batch_id=b.dw_batch_id
left outer join BIB_META.dw_rt_batches c on b.dw_copied_batch_id=c.dw_batch_id
where 1=1
and run_status not in('CANCELLED')
and task in ('STD_C2C_FCT_SUBS_SNAPD')  --4,019,851
and coalesce(to_char(b.dw_date_key) ,a.diy_parameter,'_')=(SELECT to_char(MAX_KEY) FROM DW_TASKS WHERE TASK = 'STD_DEP_EDW_PROCESS_DATE')
) end_run) order by 
case when task in ('CS5_TE_S2C_FCT_CDR_PREPAID_SNAPD_SDP_DUMP_MA') then 1
when task in ('STD_TE_I2S_POSTPAID_SUBSCRIBER_BASE_PULL') then 1
when task in ('STD_C2E_FCT_PREPAID_SNAPD') then 9.1
when task in ('STD_C2C_FCT_MSISDN_PREPAID_SUMD') then 2
when task in ('STD_C2C_FCT_PREPAID_FIRST') then 3
when task in ('STD_C2C_FCT_PREPAID_LAST') then 4
when task in ('STD_C2C_FCT_PREPAID_FIRST_ACTIV') then 5
when task in ('STD_TE_S2S_INT_SUBSCRIBER') then 6
when task in ('STD_TE_S2S_INT_SUBSCRIBER_MISC') then 7
when task in ('STD_DIM_SUBSCRIBER') then 8
when task in ('STD_BRT_SUBSCRIBER') then 9
when task in ('STD_TE_S2C_FCT_CDR_INTERCONNECT_DIY_PAR') then 10
when task in ('STD_C2C_FCT_MSISDN_USAGE_INTERCONNECT_FIRST') then 11
when task in ('STD_C2C_FCT_PVT_CDR_PREPAID_RATED_DED') then 12
when task in ('STD_C2C_FCT_CDR_RATED_COMB') then 13
when task in ('STD_C2C_FCT_MSISDN_BASE_STATION_SUMD') then 14
when task in ('STD_C2C_FCT_MSISDN_GEOGRAPHY_SUMD') then 15
when task in ('STD_C2C_FCT_MSISDN_RATED_SUMD') then 16
when task in ('STD_C2C_FCT_CDR_RATED_SUMD') then 17
when task in ('STD_C2C_FCT_MSISDN_USAGE_PREPAID_FIRST') then 18
when task in ('STD_C2C_FCT_MSISDN_USAGE_PREPAID_LAST') then 19
when task in ('STD_C2C_FCT_MSISDN_USAGE_TAP_IN_FIRST') then 20
when task in ('STD_C2C_FCT_MSISDN_USAGE_TAP_IN_LAST') then 21
when task in ('STD_C2C_FCT_SUBS_EVENT_SUMD') then 22
when task in ('STD_C2C_FCT_SUBS_EVENT_FIRST') then 23
when task in ('STD_C2C_FCT_SUBS_EVENT_LAST') then 24
when task in ('STD_C2C_FCT_SUBS_RGE_LAST') then 25
when task in ('STD_C2C_FCT_SUBS_RGE_MISC_LAST','STD_C2C_FCT_SUBS_RGE_MM_LAST') then 26
when task in ('STD_C2C_FCT_SUBS_SNAPD') then 28
when task in ('DAY DURATION') then 0
when task in ('STD_C2C_FCT_SUBS_RGE_CALC') then 30
when task in ('STD_C2E_FCT_SUBSCRIBER_SNAPD') then 31 
when task in ('STD_C2E_FCT_SUBS_FULL_SUMD') then 32
when task in ('STD_E2E_FRU_SUBSCRIBER_SNAPD') then 33
when task in ('STD_E2E_FRU_SUBSCRIBER_HL_SNAPD') then 34
when task in ('STD_E2E_FRU_SUBSCRIBER_SNAPD_HL') then 35
when task in ('STD_C2C_FCT_CDR_INTERCONNECT_SUMD') then 36
when task in ('STD_C2E_FCT_INTERCONNECT_EXPENSE') then 37
when task in ('STD_C2E_FCT_INTERCONNECT_REVENUE') then 38
when task in ('STD_C2E_FCT_USAGE_DATA_OUT') then 39
when task in ('STD_C2E_FCT_USAGE_POSTPAID_IN') then 40
when task in ('STD_C2E_FCT_USAGE_POSTPAID_OUT') then 41
when task in ('STD_C2E_FCT_USAGE_PREPAID_IN') then 42
when task in ('STD_C2E_FCT_USAGE_PREPAID_OUT') then 43
when task in ('STD_C2E_FCT_SUBSCRIBER_SNAPM') then 44
end;
/



----------rgs prev day ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select date_key, sum(openingbase_cnt), SUM(RGS_1_CNT), SUM(RGS_7_CNT),SUM(RGS30_CNT),SUM(RGS60_CNT) 
from  BIB.FRU_CEO_GEO_SUMD B
group by date_key 
order by date_key;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------PRE CHECKS ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--check if we received postpaid data AND TREND 

select * from  i_control@IA_USER.MTN.CO.ZA   where OBJECT_NAME     like '%ADDRE%' order by date_key desc;



SELECT 
DISTINCT NVL(B.DATE_KEY,MAX(A.DATE_KEY))DATE_KEY,
A.OBJECT_NAME,
B.PROCESSING_STATUS
FROM  i_control@IA_USER.MTN.CO.ZA  A,
 (SELECT 
  OBJECT_NAME, 
  DATE_KEY,
  PROCESSING_STATUS
  FROM  i_control@IA_USER.MTN.CO.ZA 
  WHERE DATE_KEY = (SELECT DISTINCT MAX_KEY FROM DW_TASKS WHERE TASK = 'STD_DEP_EDW_PROCESS_DATE')
 )B
WHERE A.OBJECT_NAME = B.OBJECT_NAME  (+)
--AND A.DATE_KEY=B.DATE_KEY
GROUP BY A.OBJECT_NAME,
B.PROCESSING_STATUS,
B.DATE_KEY
ORDER BY 1,2
;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------OVERALL FILE TRENDS  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT  
 MAX_DW_DATE_KEY DATE_KEY 
, COUNT(*) FILE_COUNT 
, SUM(ROW_COUNT) CDR_COUNT,
MIN(ROW_COUNT) MIN_COUNT, AVG(ROW_COUNT) AVG_COUNT, MAX(ROW_COUNT) MAX_COUNT
FROM ( 
      SELECT  /*+PARALLEL (A,20) */ TASK, MAX_DW_DATE_KEY, ROW_COUNT 
      FROM BIB_META.DW_RT_FILES A JOIN BIB_META.DW_TASKS B ON (A.DW_TASK_ID=B.DW_TASK_ID) 
      Where A.Processing_Status In ('LOADED','ARCHFAILED','ARCHIVED','ARCHIVING','ARCHIVEFAILED')
--      AND TASK like 'TF_S2S_GGSN_CDR%' 
--        and task like 'TF_S2S_IBF_XDR_ZA'
        AND TASK LIKE 'TF_I2S_I_CDR_PREPAID_DED_SNAPD%'

  AND MAX_DW_DATE_KEY BETWEEN to_char(SYSDATE - 30,'yyyymmdd') and to_char(SYSDATE ,'yyyymmdd')
)  
GROUP BY  MAX_DW_DATE_KEY 
ORDER BY DATE_KEY DESC, MAX_DW_DATE_KEY 
--ORDER BY DATE_KEY DESC
;







SELECT  
 MAX_DW_DATE_KEY DATE_KEY 
, COUNT(*) FILE_COUNT 
, SUM(ROW_COUNT) CDR_COUNT,
MIN(ROW_COUNT) MIN_COUNT, AVG(ROW_COUNT) AVG_COUNT, MAX(ROW_COUNT) MAX_COUNT
FROM ( 
      SELECT  /*+PARALLEL (A,20) */ TASK, MAX_DW_DATE_KEY, ROW_COUNT 
      FROM BIB_META.DW_RT_FILES A JOIN BIB_META.DW_TASKS B ON (A.DW_TASK_ID=B.DW_TASK_ID) 
      Where A.Processing_Status In ('LOADED','ARCHFAILED','ARCHIVED','ARCHIVING','ARCHIVEFAILED')
--      AND TASK like 'TF_S2S_GGSN_CDR%' 
--        and task like 'TF_S2S_IBF_XDR_ZA'
        AND TASK LIKE 'TF_S2S_XDR_SV_ZA%'

  AND MAX_DW_DATE_KEY BETWEEN to_char(SYSDATE - 20,'yyyymmdd') and to_char(SYSDATE ,'yyyymmdd')
)  
GROUP BY  MAX_DW_DATE_KEY 
ORDER BY DATE_KEY DESC, MAX_DW_DATE_KEY 
--ORDER BY DATE_KEY DESC
;
--------------------CHECK NODES---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select  *  from  STG_GEN.I_CDR_PREPAID_SNAPD PARTITION FOR (20170812)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------CHECK DUMPS 421 AND TREND ----------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT SUBSTR(SHORT_FILENAME,1,8) DATE_KEY,PROCESSING_STATUS, COUNT(*) DUMPS,sum(row_count) RECORD_COUNT, MAX_DW_DATE_KEY, max(REGISTER_DATETIME)
FROM DW_RT_FILES A WHERE a.dw_task_id = dw_get_task_id ('TF_I2S_I_CDR_PREPAID_SNAPD') 
AND SUBSTR(SHORT_FILENAME,1,8) = '20170316'
---between '20170121' and SUBSTR(TRUNC(to_date('20170306 00:00:00', 'yyyymmdd hh24:mi:ss')),1,8)
GROUP BY SUBSTR(SHORT_FILENAME,1,8) , PROCESSING_STATUS, MAX_DW_DATE_KEY
ORDER BY 1 DESC
;


select * from dw_rt_runs where task LIKE '%DUMP_MA%' AND DIY_PARAMETER = '20170525'
order by start_date desc;


----------------interconnect
select date_key, count(*) from bib_cdr.fct_cdr_interconnect  partition for (20170404) group by date_key --62 million Trend
--union
--select date_key, count(*) from bib_cdr.fct_cdr_interconnect  partition for (20170307) group by date_key --62 million Trend
--union
--select date_key, count(*) from bib_cdr.fct_cdr_interconnect  partition for (20170306) group by date_key --62 million Trend
--union 
--select date_key, count(*) from bib_cdr.fct_cdr_interconnect  partition for (20170305) group by date_key --62 million Trend
;



select 
username, 
schemaname
from gv$session
--where sid = 766
;

select * from gv$instance;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------MOVE PROCESS DATES---------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT task, max_key FROM DW_TASKS WHERE TASK LIKE '%STD_DEP_IT_PROCESS_DATE%';


--STD_DEP_EDW_PROCESS_DATE
--STD_DEP_FIN_PROCESS_DATE
--STD_DEP_HR_PROCESS_DATE
--STD_DEP_IT_PROCESS_DATE
--STD_DEP_LCM_PROCESS_DATE
--STD_DEP_TECH_PROCESS_DATE



UPDATE DW_TASKS
 SET MAX_KEY = '20170817'
 WHERE TASK LIKE '%STD_DEP_EDW_PROCESS_DATE%';
 COMMIT;
 /
 


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------activate profiles------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--D-1

begin
    dw_bib_sched.SET_PROFILE('NORMAL'); 
    dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
end;
--/


update dw_rt_runs set run_status = 'CANCELLED' WHERE TASK = 'STD_TE_S2S_INT_SUBSCRIBER' AND RUN_STATUS = 'RUNABLE' AND DW_BATCH_ID <> 4240525;
/

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------SETUP TASKS --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



Begin
--Dw_Exec_Task_Setup(V_Itask=>'STD_TE_I2S_POSTPAID_SUBSCRIBER_BASE_PULL',Pi_Days_Back=>3);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2E_FCT_SUBSCRIBER_SNAPD',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_WAIT_SUBS_CLASSIFICATION',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_CDR_TRANS_COMB',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_CDR_RATED_COMB',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2E_FCT_PREPAID_SNAPD',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_MSISDN_PREPAID_SUMD',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_PREPAID_FIRST',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_PREPAID_LAST',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_TE_S2S_INT_SUBSCRIBER',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_TE_S2C_FCT_CDR_INTERCONNECT',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_MSISDN_BASE_STATION_SUMD',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_MSISDN_GEOGRAPHY_SUMD',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_MSISDN_RATED_SUMD',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_CDR_RATED_SUMD',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_MSISDN_USAGE_PREPAID_FIRST',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_C2C_FCT_MSISDN_USAGE_PREPAID_LAST',Pi_Days_Back=>1);
--Dw_Exec_Task_Setup(V_Itask=>'CS5_TE_S2C_FCT_CDR_PREPAID_SNAPD_SDP_DUMP_MA',Pi_Days_Back=>1);
Dw_Exec_Task_Setup(V_Itask=>'STD_WAIT_SUBS_SNAPD',Pi_Days_Back=>1);
End;
/



UPDATE DW_RT_RUNS SET RUN_STATUS = 'CLOSED' WHERE TASK IN ('STD_WAIT_SUBS_SNAPD') AND DW_BATCH_ID = 5675152;
/




---------------TO WORK ON FOR HOLDING INTS -------------------------------------------------------------------------------------------------------------------------------------------------------------------

--generate setups 
--'dw_exec_task_setup(v_itask=>'|| '''' || task ||''''|| ',pi_days_back=>41);'



--BIB sched LIST VIEW
select * from vw_dw_bib_sched_list
where task = 'STD_TE_S2C_FCT_CDR_TAP_IN_MACH'
;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM DW_RT_RANGE_RUNS WHERE DW_RUN_ID = 9924345
and run_status  <>    'SUCCESS'
--order by DW_RANGE_VALUE_ID  desc
;

SELECT * FROM DW_RT_RUNS WHERE DW_RUN_ID IN
(
10437107
);


-------------------------GENERIC------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

UPDATE  
DW_RT_RUNS
--DW_RT_RANGE_RUNS 
  SET 
--RUN_STATUS = 'RUNABLE'
--RUN_STATUS = 'HOLD_MK_FIX'
RUN_STATUS = 'SUCCESS'
--RUN_STATUS = 'CLOSED'
--RUN_STATUS = 'JST_HOLD'
--end_date = sysdate

--,
--start_DATE = sysdate - 50
WHERE 
1=1
--AND  TASK LIKE 'CS5_TE_S2C_FCT_CDR_PREPAID_SNAPD_SDP_DUMP_MA%'
--and  TASK in 
--(
--)
AND  DW_RUN_ID  IN 
(
10527746,
10524457,
10520323,
10497777,
10497912,
10498411,
10514794
) 
-- AND DW_BATCH_ID  = 6129957
--AND DW_RANGE_VALUE_ID = 10143
--AND DIY_PARAMETER  NOT in ('20170427')
--AND RUN_STATUS IN ('HOLD_MK_FIX')
--AND RUN_STATUS  IN ('FAILED')
--AND RUN_STATUS  IN ('PENDING')
--  AND RUN_STATUS  IN ('RUNABLE')
;
/

commit;



update dw_rt_files set processing_status = 'REGISTERED' where dw_batch_id = 4314802
and processing_status = 'FAILED';
/
-----------failed int_sub add a payment option-----------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT * FROM STG_GEN.I_POSTPAID_SUBSCRIBER_BASE
WHERE BATCH_ID = 3388471 --failing batch
and payment_option_cd is null;


update STG_GEN.I_POSTPAID_SUBSCRIBER_BASE set payment_option_cd = 'U' 
WHERE BATCH_ID = 3388471 --failing batch
and payment_option_cd is null;


update dw_rt_range_runs set run_status = 'RUNABLE'
where dw_run_id = 4820467 
and DW_RANGE_VALUE_ID in
(
select DW_RANGE_VALUE_ID 
from bib_meta.dw_rt_range_runs r 
where r.dw_run_id = 4820467 
and run_status = 'RUNNING'
and dw_range_value_id not in (select dw_range_value_id from bib_ctl.dw_bib_sched_list where task = 'STD_C2C_FCT_CDR_RATED_COMB')
)
;

--------------------------LOG APP--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--vw_rt_range_runs 

SELECT  /*+ parallel(20)*/ *
FROM DW_RT_LOG_APP  
where 1=1
and dw_task_id in dw_get_task_id('CS6_TE_S2C_FCT_CDR_PREPAID_RATED_MA')
--and process_name = 'DW_BIB_DELETE_DUPS_SUBPART_CX'
and  ERROR_SYSDATE  >  sysdate - 0.1
TO_DATE ('20170807 12:35:31' , 'YYYYMMDD HH24:MI:SS')
--and error_code = 'E'
--and dw_run_id =  10876755 
--and dw_batch_id = 9178598
--AND MESSAGE LIKE '%PREPAID_SNAPD-INTERNAL%'
--AND PROCESS_NAME LIKE '%TABLESPACE%'
ORDER BY error_sysdate DESC
;
 
 select * from DW_RT_DO_BATCH_LOG where job_id = 'A10085821_20171024173712';
 
select
*
--error_sysdate, error_code, message
from dw_rt_log_app
where  dw_task_id = dw_get_task_id ('TF_S2S_CS6_SDP_CDR')  
and error_sysdate > sysdate-1
and message like 'Execute_Do_Batch%'
order by dw_log_id desc;
 
 
 select * from DW_RT_DO_BATCH_LOG where job_id = 'A10122750_20171024165420';
 
 
 select * from dw_rt_log_app
where dw_task_id = dw_get_task_id ('TF_S2S_MSC_SMS_ZA')  
--  AND  PROCESS_NAME  like '%AUDIT%'
  AND  PROCESS_NAME  = 'DW_FILE_TASK_REGISTER'
--  AND PROCESS_NAME = 'DW_FILE_TASK_ARCHIVE'
--  AND PROCESS_NAME <> 'DW_REMOTE_JOB'
--  AND PROCESS_NAME =  'DW_FILE_TASK_AUDIT'
and error_sysdate >=  sysdate - 10
and dw_run_id = 10854884
--and error_code <> 'I'
order by 1 desc
;






Select *
from dw_rt_log_app
where error_sysdate>sysdate - 10
and error_code = 'E'
and (dw_run_id, dw_sid) in (
 select dw_run_id, dw_sid
from dw_rt_runs where dw_run_id = 5188629)
order by dw_log_id
;





SELECT * FROM DW_TEMPLATES WHERE TEMPLATE_NAME = 'BIB_EXEC_TASK_TEMPLATE';
select * from dw_rt_run_errors;



select def_tablespace_name, index_name, table_name from dba_part_indexes where table_name = 'FCT_CDR_PREPAID_TRANS';


ALTER TABLE STG_CDR.MMC_CDR MODIFY(ORIGINATORIMSI VARCHAR2(4000 BYTE));

/

-----------------------------multi ------------------------------------------------------------
--FRU_SUBSCRIBER_SNAPD
--FCT_PREPAID_USAGE_SUMD





SELECT start_date,  end_date, task , 
--numtodsinterval(end_date-start_date,'DAY') duration ,   
ROUND((end_date - start_date) * 24 * 60 , 2)  Duration1, 
numtodsinterval(end_date-start_date,'day') duration, dw_batch_id, dw_run_id ,  run_status, diy_parameter, 
DIRECT_EXEC_SQL, rows_loaded
FROM DW_RT_RUNS
WHERE  1=1
--and start_date between  to_date('20171005 00:00:00' ,  'YYYYMMDD HH24:MI:SS') and  to_date('20171005 23:59:59' ,  'YYYYMMDD HH24:MI:SS')
--and TASK like '%CS6_TE_S2C_FCT_CDR_PREPAID_RATED_MA%' 
--and task in  (select task  from dw_TASK_DATA_DEPEND WHERE DEPENDENT_TASK like '%STD_TE_S2E_FCT_IT_TICKET')
--and RUN_STATUS not in  ('CLOSED' , 'SUCCESS' )
and run_status   IN  ('FAILED' )
--and run_status   IN  ('HOLD_MK_FIX' )
--AND   DW_BATCH_ID  IN (10788318)
--AND   DW_RUN_ID  IN (10788318)
--AND DIY_PARAMETER = '20171024'
--and START_DATE >  sysdate - 7
ORDER BY START_DATE  DESC
--order by diy_parameter DESC
;


CS5_TE_S2C_FCT_CDR_ADJUSTMENT_IBF_MA 385 
CS6_TE_S2C_FCT_CDR_PREPAID_RATED_MA 485 


begin 
dw_bib_utl. PERIOD_BATCH_ANALYSIS(v_task_like =>'%CS6_TE_S2C_FCT_CDR_PREPAID_RATED_MA%',v_from_key =>20170731, v_to_key=>20170731);
     dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
    end;
/


BEGIN 
   FOR i IN 20170720 .. 20170720
    LOOP
     DBMS_OUTPUT.PUT_LINE('BIB_CTL.DW_BIB_UTL.DIY_RESET(''STD_C2C_FCT_CDR_TRANS_COMB'','''||i||''');');
    END LOOP;
END;
/




UPDATE DW_RT_RUNS SET RUN_STATUS = 'HOLD_MK'
WHERE TASK IN ( 'STD_C2C_FCT_CDR_TRANS_COMB')
AND DW_RUN_ID IN  (SELECT MAX(START_DATE) , DW_RUN_ID FROM DW_RT_RUNS WHERE TASK = 'STD_C2C_FCT_CDR_TRANS_COMB' AND DIY_PARAMETER >= '20170720' AND DIY_PARAMETER <= '20170731'
GROUP BY START_DATE )
--AND RUN_STATUS = 'RUNABLE'
;

COMMIT;


SELECT MAX(START_DATE) , TASK,  DW_RUN_ID FROM DW_RT_RUNS 
WHERE TASK = 'STD_C2C_FCT_CDR_TRANS_COMB' AND DIY_PARAMETER >= '20170720' AND DIY_PARAMETER <= '20170731'
GROUP BY TASK , DW_RUN_ID
;




select * from stg_cdr.IBF_XDR partition for (20170920)
where 1=1
;



delete from stg_cdr.IBF_XDR partition for (20170920)
where 1=1
and unique_record_id in
(
269815276594630414920170920095432311
);
commit;




------------------------------------failed runs 

SELECT start_date,  end_date, a.task , a.dw_batch_id, dw_run_id ,  run_status, diy_parameter,  task_business_area,
DIRECT_EXEC_SQL, rows_loaded
FROM DW_RT_RUNS a, DW_TASKS B
WHERE A.DW_TASK_ID  = B.DW_TASK_ID
--and b.task_business_area not in ( 'Finance')
--and b.task_business_area not like '%Distribution'
and A.run_status  IN  ('FAILED' )
ORDER BY start_date  DESC
;


--------------------backlog monitoring------------------------------------------

SELECT diy_parameter, round(sum(end_date - start_date),2) Duration, 
sum(case  when run_status = 'RUNABLE'  then 1 else 0 end) as RUNABLE,
sum( case when run_status in ('CLOSED', 'SUCCESS') then 1 else 0 end) as CLOSED,
sum( case when run_status in ('RUNNING') then 1 else 0 end) as RUNING,  
sum(rows_loaded) rows_loaded 
FROM DW_RT_RUNS
WHERE TASK like '%CS6_TE_S2C_FCT_CDR_PREPAID_RATED_MA%'
--and start_date > trunc(sysdate - 5)
group by  diy_parameter
order by diy_parameter desc
/




----------------------------------------------------------DROPPING-----------------------------------------------------------

select task, rj.job_name, 'exec dbms_scheduler.drop_job('''||rj.owner||'.'||rj.job_name||''',force=>true);' drop_job,  rj.*
from dba_scheduler_running_jobs rj
join bib_ctl.dw_bib_sched_list lst
on rj.running_instance = lst.dw_inst_id and rj.session_id = lst.dw_sid
where lst.task LIKE 'STD_TE_S2S_REM_HPD_HELP_DESK'
;




-------------------------------SCHED LIST ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


select *
from bib_meta.dw_rt_range_runs r 
where r.dw_run_id = 4820467 
and run_status = 'RUNNING'
and dw_range_value_id not in (select * from bib_ctl.dw_bib_sched_list where task = 'TD_S2S_SCOM')
order by start_date desc;

select * from bib_ctl.dw_bib_sched_list where task = 'TR_S2S_SCOM';

------------------------------------------------FAVOURITE METAS -----------------------------------------------------------------------------------------------------------------------------------------------------

--and not regexp_like (A_ACCOUNT_BALANCE_AMT,'^[[:digit:]\.[:digit:]]+$')
--regexp_like(duration,'[^a-zA-Z0-9]') NOT NUMBERS
--;

------------------------------------------------------------------------------DUPLICATES ------------------------------------------------------------------------------------------------------------------------------

delete from STG_GEN.FUNDM_USER_ACCOUNT
 where rowid in
 (
 select rowid
from
 (
 select
 rowid,
 row_number() over ( partition by OID order by batch_id desc, last_update desc ) row_number
 from STG_GEN.FUNDM_USER_ACCOUNT b
 ) a
 where row_number > 1
 );
 
 
 349012
 355708
 
 

exec dw_bib_utl.task_force_super_batch('STD_TE_S2C_FCT_CDR_UNRATED_GGSN_INT');


update dw_rt_runs set rows_loaded=null where rows_loaded is not null and run_status='RUNABLE' and task like '%STD_TE_S2C_FCT_HLR_SNAPD_HLR%';
/


update dw_rt_files set processing_status = 'REGISTERED' 
where 1=1
and dw_task_id = 22118
--and dw_batch_id = 5148644 
and processing_status = 'FAILED';
/

---------------------------------------------- clob log --------------------------------------------------------------------------------------------------------------------------------------------------------------------

select * 
from bib_ctl.DW_RT_CLOB_LOG
where LOG_DATE >sysdate - 1
and dw_run_id = 4544709
order by LOG_DATE desc;
/

---------------------------------------------- PUMP  --------------------------------------------------------------------------------------------------------------------------------------------------------------------

select 
task, 'update dw_rt_runs set run_status = ''RUNABLE'', rows_loaded = null where dw_batch_id = '||dw_batch_id||' and task = '''||task||''';'
from dw_Rt_runs
where dw_batch_id in (
select batch_id 
from dl_P_20170322_batches_tmp)
and task <> 'TG_CDR_PREPAID_RATED';


------------------------------FILES STUFF --------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT  /* + PARALLEL 30 */   * FROM dw_rt_batch_date_histograms
WHERE DW_BATCH_ID IN 
(
SELECT DW_BATCH_ID FROM DW_RT_FILES 
WHERE dw_task_id in dw_get_task_id('TF_S2S_CS6_SDP_CDR')
and register_datetime  >   sysdate -  1
)
order by dw_date_key desc
;



select  /* + PARALLEL 30 */  
*
--'rm  -f '||  SHORT_FILENAME
from dw_rt_files where 1=1
--and  dw_batch_id    =  10062549
--and dw_file_key = 471981930
and dw_task_id in dw_get_task_id('TF_S2S_IBF_XDR_ZA')
--  AND PROCESSING_STATUS LIKE '%LOADED%'
--AND TRUNC(REGISTER_DATETIME) = TO_DATE ('20170303' ,  'YYYYMMDD') ;
and short_filename like '%SDPOUTPUTCDR_4001_gesdp9_SUB_5243_20170921-155209.ASN_20170921155408%' 
and register_datetime  >   sysdate -  100
--and bad_row_count > 0
--order by START_DATETIME desc
;



select
'DECLARE
v_pi_run_id  integer := ' || DW_LOAD_RUN_ID || ';' || chr(10) ||
'BEGIN
DW_FILE_TASK_ARCHIVE(''' || task  || ''',v_pi_run_id);
dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
END;
/
' 
from (
select /*+ parallel(10)*/   
distinct DW_LOAD_RUN_ID,  (select task from dw_tasks b where a.DW_TASK_ID = b.DW_TASK_ID and task_type  like 'FILE' ) task
from dw_rt_files  a 
WHERE  DW_TASK_ID in (select dw_task_id from dw_tasks where task_type  like 'FILE' and task like 'TF_S2S_INTERCONNECT_CDR_ZA'
and short_filename like '%20171002%'
and PROCESSING_STATUS   like 'LOADED'
and register_datetime  >   sysdate -  30
)

)
;
/



update dw_rt_files set PROCESSING_STATUS = 'ARCHIVED'
WHERE dw_task_id in dw_get_task_id('TF_S2S_INTERCONNECT_CDR_ZA')
AND PROCESSING_STATUS = 'LOADED'
and short_filename like '%20171002%' 
;

COMMIT;


select * from DW_BIB_SCHED_LIST
where dw_run_id=10852933 ; 

commit;





delete from dw_rt_files where 1=1
and dw_task_id in dw_get_task_id('TF_S2S_IBF_XDR_ZA')
--AND PROCESSING_STATUS = 'REG_FAILED'
AND DW_FILE_KEY IN
(
459520968,
459520969,
459520979,
459520974,
459520967
)
--and short_filename like '%20170904%' 
--and register_datetime  >   sysdate -  18
;

commit;


'TF_I2S_I_CDR_PREPAID_ACCUM_SNAPD',
'TF_I2S_I_CDR_PREPAID_DED_SNAPD',
'TF_I2S_I_CDR_PREPAID_SNAPD'



select ROWS_LOADED, ROWS_ERROR , RUN_STATUS from dw_rt_runs where dw_batch_id in 
(
8574571,
8574557

);


--prepaid_snapd

select * from dw_rt_batches where dw_batch_id in
(
10097398
);



select * from bad_data where batch_id in
(
9564884
);

8576624

select * from STG_GEN.I_CDR_PREPAID_SNAPD
--partition for (20170823)
where batch_id in 
(
9230854
);



SELECT 
DISTINCT NVL(B.DATE_KEY,MAX(A.DATE_KEY))DATE_KEY,
A.OBJECT_NAME,
B.PROCESSING_STATUS
FROM I_CONTROL@BIBSRC_LINK A,
 (SELECT 
  OBJECT_NAME, 
  DATE_KEY,
  PROCESSING_STATUS
  FROM I_CONTROL@BIBSRC_LINK
  WHERE DATE_KEY = (SELECT DISTINCT MAX_KEY FROM DW_TASKS WHERE TASK = 'STD_DEP_EDW_PROCESS_DATE')
 )B
WHERE A.OBJECT_NAME = B.OBJECT_NAME  (+)
--AND A.DATE_KEY=B.DATE_KEY
GROUP BY A.OBJECT_NAME,
B.PROCESSING_STATUS,
B.DATE_KEY
ORDER BY 1,2
;



select * from dw_rt_batches where dw_batch_id  in
(
6131173
);


SELECT DW_RUN_ID || ',' FROM DW_RT_RUNS WHERE DW_BATCH_ID IN
(
select DW_BATCH_ID   from dw_rt_batches where 1=1
and dw_date_key = '20170405'
and table_name = 'STG_GEN.I_CDR_PREPAID_SNAPD'
);
/


select *    from dw_rt_batches where 1=1
--and dw_date_key = '20170406'
--and table_name = 'STG_GEN.I_CDR_PREPAID_SNAPD'
and dw_batch_id = 9177432
order by create_datetime desc;
/



select * from  DW_RT_DIY_LIST
--where dw_TASK_id = 12544
;



SELECT DISTINCT MAX_KEY FROM DW_TASKS WHERE TASK = 'STD_DEP_EDW_PROCESS_DATE';
-------------------------diy list-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


select  * 
from dw_Rt_diy_list
where dw_task_id   in dw_get_task_id('STD_C2C_FCT_SUBS_EVENT_SUMD')
--and PARAMETER like '201701%'
and PARAMETER  =    '20170831'
--and PROCESSING_STATUS='RUNABLE';--and '20160301';
--and PROCESSING_STATUS <> 'LOCKED';--and '20160301';
;

update dw_Rt_diy_list set PROCESSING_STATUS='REGISTERED' 
where dw_task_id = 11188 
--and PARAMETER like '20170101' 
and PROCESSING_STATUS='LOCKED' 
--and DW_RUN_ID in (3512062,3512056,3512060)
and PARAMETER in ('20170206','20170208','20170211','20170213','20170215','20170216','20170218')
;




select A.TASK,C.TARGET_TABLE_NAME, 
   TASK_BUSINESS_AREA, 


  A.RUN_STATUS, 

  sum(A.ROWS_LOADED)as ROWS_LOADED, 
 (a.DIY_PARAMETER)  
 
from DW_RT_RUNS A, DW_TASKS B, BIB_META.DW_TASK_EXEC_STEPS C 
where A.RUN_STATUS='CLOSED'  
and A.TASK=B.TASK 
and A.DW_TASK_ID = c.dw_task_id 
and B.IS_RUNNABLE='Yes' 
and C.TARGET_TABLE_NAME is not null 


and B.TASK ='STD_C2E_FCT_USAGE_DATA_OUT' 
and A.DIY_PARAMETER BETWEEN '20170401' and '20170431' 


group by A.TASK,C.TARGET_TABLE_NAME,TASK_BUSINESS_AREA, B.TASK_TECHNICAL_AREA,B.TASK_TARGET_TYPE,A.RUN_STATUS,B.IS_RUNNABLE,target_table_name,a.DIY_PARAMETER 
order by A.TASK ,MAX(A.DIY_PARAMETER) asc;

SELECT * FROM (
SELECT /*+parallel (A,10) FULL(A)*/ 
       A.SHORT_FILENAME, FILENAME, 
       PROCESSING_STATUS, 
       TO_CHAR(TRUNC(REGISTER_DATETIME),'YYYYMMDD') REG_DATE, 
       COUNT(*) CNT
FROM BIB_META.DW_RT_FILES A
JOIN BIB_META.DW_TASKS B
    ON A.DW_TASK_ID = B.DW_TASK_ID
where 1=1
--AND b.parameter_type = 'LOAD'

--      AND PROCESSING_STATUS like '%DUP%' 
        AND REGISTER_DATETIME  > SYSDATE - 0.5

  AND TASK in 
  (
  'TF_S2S_CS6_CCN_CDR'
--  'STD_TF_S2S_HRIS_ASSIGNMENT'
--  'STD_TF_S2S_HRIS_BENEFITS'
--  'STD_TF_S2S_HRIS_PERSONAL'
  )
GROUP BY 
       SHORT_FILENAME,
       FILENAME,
        PROCESSING_STATUS, 
        TO_CHAR(TRUNC(REGISTER_DATETIME),'YYYYMMDD')
ORDER BY 4 DESC
) WHERE PROCESSING_STATUS NOT IN ('LOADED','ARCHFAILED','ARCHIVED','ARCHIVING' , 'LOADING');

--------------who did what ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM  bib_META.audit_ddl
WHERE SYSEVENT  = 'UPDATE';
;

select * from dw_table_metadata ;
where  tablespace_name  = 'TS_STG_CDR';


select * from DW_TABLESPACE_RETENTION 
--where  tablespace_name  = 'TS_STG_CDR
;

select * 
from stg_cdr.audit_ddl
where name = 'ER_SASN_CDR';

------------------------GET DDL ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select dbms_metadata.get_ddl('TABLE','I_INVENTORY_SNAPM','STG_GEN') from dual;
/

BIB_CTL.VS_CC_CALL_EVENT CEVENT


BIB_CTL.VI_FIN_GL_ACCT_TRANSACTION
SELECT *   FROM I_BILLING_ACCOUNT_SNAPM;

BIB_CTL.VI_BILLING_PAYMNT_ALLOCATION ALLOC
select  upper('dl_tmp_processdt_snapshot') from dual;


select * from DW_RT_DO_BATCH_LOG  where dw_batch_id = 4873895;
--return code 15 concatenation error.


SELECT A.*, 'mv '||REPLACE(filename,'incoming','work')||'   '||filename as mv FROM DW_RT_FILES A WHERE DW_BATCH_ID in
(
4972918
) ;
-rw-r--r-- 1

------------------------COLUMN MAPPING ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select * from dw_column_mappings where 1=1
and task LIKE '%TF_S2S_IBF_XDR_ZA%'
ORDER BY COLUMN_ORDER DESC

;
/

"new_SDPOUTPUTCDR_4001_jhsdp3_SUB_4815_20170411-132025.ASN
"
select column_order,task, column_name, sqlloader_format, oracle_format from DW_TASK_FILE_COLUMNS
--  where task LIKE '%STD_TE_S2C_FCT_CDR_POSTPAID_RATED_PATTERN%'
order by task , column_order;
/

select * from dw_task_attributes;


----------------------- -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------- -------------------DEPENDING----------------------------------------------------------------------------------------------------------------------------------------------------------------------------



select *   from dw_TASK_DATA_DEPEND WHERE dependent_TASK like '%TG_CDR_PREPAID_RATED'
--and super_batch_ind = 'Y'
;




begin 
dw_bib_utl. PERIOD_BATCH_ANALYSIS(v_task_like =>'%CS6_TE_S2C_FCT_CDR_PREPAID_RATED_MA%',v_from_key =>20170701, v_to_key=>20170702);
     dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
    end;
/


UPDATE dw_task_data_depend
set SUPER_BATCH_IND = 'Y'
--set DEPENDENCY_TYPE = 'DIY_BATCH_DATE_WAIT' 
where dependent_task = 'STD_C2C_FCT_CDR_RATED_COMB'
AND task in ( 'TG_CDR_PREPAID_RATED', 'TG_CDR_POSTPAID_RATED' , 'TG_CDR_INTERCONNECT')
AND SUPER_BATCH_IND = 'N'
;

--STD_C2C_FCT_MSISDN_RATED_SUMD

UPDATE dw_task_data_depend
--set SUPER_BATCH_IND = 'N'
set DEPENDENCY_TYPE = 'DIY_BATCH_DATE_WAIT_MK' 
where dependent_task = 'STD_C2C_FCT_MSISDN_GEOGRAPHY_SUMD'
AND task = 'STD_C2C_FCT_MSISDN_BASE_STATION_SUMD'
AND SUPER_BATCH_IND = 'Y'
--AND DEPENDENCY_TYPE = 'DIY_DIY_DATE'
;


UPDATE dw_task_data_depend
set SUPER_BATCH_IND = 'Y'
--set DEPENDENCY_TYPE = 'DIY_DATE' 
where dependent_task = 'STD_C2C_FCT_SUBS_RGE_LAST'
AND task = 'STD_C2C_FCT_MSISDN_USAGE_PREPAID_LAST'
AND SUPER_BATCH_IND = 'N'
;
--------------INSERT DEPENDANCY------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

begin
dw_bib_utl.insert_dependency(  
  pi_parent_task_like =>'STD_E2E_FRU_PVT_POSTPAID_SUMD',
  pi_dependent_task_like =>'STD_C2C_FCT_SUBS_RGE_MISC_LAST',
  pi_SUPER_BATCH_IND =>'N',
  pi_DEPENDENCY_TYPE=> 'DIY_DIY_DATE');
end;
/

tar zxvf 89205.tar.gz


-------------------------------------MAX_FILE ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT 
file_wildcard,
flex1,
flex2,
''''|| TASK || ''',' , FLEX1, LAG_TIME, flex2, max_file_load_concurrency ,IS_RUNNABLE, a.*  
FROM DW_TASKS a 
WHERE 
1=1
--AND FLEX1 LIKE '7'
--AND FLEX2 = '3'
--AND IS_RUNNABLE = 'No'
--and parameter_type = 'BATCH'
--AND TASK IN (SELECT TASK FROM DW_TASK_DATA_DEPEND WHERE DEPENDENT_TASK  = 'STD_TE_S2E_FCT_IT_TICKET')
--AND DW_TEMPLATE_ID = 11093
--AND MAX_FILE_ARCHIVE_CONCURRENCY = 10
--AND TASK_BUSINESS_AREA LIKE '%Sales%'
--and TASK_TECHNICAL_AREA like 'Integration to Stage Pull'
AND TASK  like '%TF_S2S_XDR_SV_ZA%'
--and IS_RUNNABLE <> 'No'
--and file_wildcard is not null;
--and task in

;
/



------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

UPDATE
DW_TASKS
--dw_environment_variables 
SET
--FLEX1 = '16'
--FLEX2 = 'RR:2,3,4,5,6,7,8'
--LAG_TIME = NULL
-- VARIABLE_VALUE =  10
--IS_RUNNABLE = 'Yes' 
--FILE_WILDCARD = '%SDPOUTPUTCDR%'
max_file_load_concurrency =   10
--MAX_FILE_AUDIT_CONCURRENCY =10
--ALLOW_BATCHING = 'N'

WHERE
1=1
--and VARIABLE_NAME = 'LOCATION:BIB_CTL:DW_EXEC_TASK_RUN:7'
--AND FLEX1 = '17'
AND TASK in 
(

'TF_S2S_XDR_SV_ZA'

)

;

COMMIT;


select * from stg_cdr.ibf_xdr partition for (20171001)
where ADJUSTMENTOFFERS is not null
;

select task,is_runnable, LAST_MOD_STATIC_COLS_TS,LAST_MOD_TS,AUD_ACTION,aud_timestamp, aud_user,aud_osuser,aud_host,aud_module
from BIB_META.DW_TASKS_AUD
where task = 'STD_C2C_MSISDN_RECYCLE'
and aud_timestamp > trunc(sysdate -1)
order by last_mod_ts desc ;
 


--------------------------------------ENVIRONMENTAL VARIABLES----------------------------------------------------------------------------------------------------------------------------------------

select /*+ parallel(2)*/ * from dw_environment_variables
where variable_name like '%MIN_PROCESS_TS%'
order by 2;


--------STOP SCHEDULER JOBS-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


exec dw_umd_utl.sr_upd('DW_BIB_SCHED_LIST_STOP','Y','Y');
exec dw_umd_utl.sr_upd('DW_BIB_SCHED_LIST_STOP','','');


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
UPDATE dw_environment_variables
SET VARIABLE_VALUE = 'N'
WHERE VARIABLE_NAME = 'GLOBAL_ETL_SWITCH';



-----------------CLEAR CACHE--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

begin
   dw_exec.clear_cache(dw_get_task_id('TF_S2S_CS6_SDP_CDR'));
end;
/


---------------------CHECK SPACE --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT distinct  TOTAL_MB/1024 AS TOTAL_IN_GB, 
ROUND(TOTAL_MB/1024/1024,2) AS TOTAL_IN_TB, 
ROUND(USABLE_FILE_MB/1024,0) AS AVAILABLE_GB, 
ROUND(USABLE_FILE_MB/1024/1024,3) AS AVAILABLE_TB,
ROUND((USABLE_FILE_MB/1024)/(TOTAL_MB/1024)*100,0)||'%' PERC_AVAILABLE, NAME 
FROM GV$ASM_DISKGROUP
ORDER by 6 ASC
;


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT  tablespace_name, tablespace_size /1000000/1024 tablespace_size, allocated_space /1000000/1024  allocated_space, 
 free_space /1000000/1024  free_space FROM dba_temp_free_space;




-------------------------------------------------------------------------------------------------


--------------CLEANING UP META TABLES ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

DELETE FROM bib_meta.dw_rt_file_date_histograms   
WHERE DW_FILE_KEY IN 
(SELECT DW_FILE_KEY FROM bib_meta.dw_rt_files WHERE dw_batch_id IN (8280846));
DELETE FROM bib_meta.dw_rt_batch_date_histograms 
WHERE dw_batch_id IN (8280846);
DELETE FROM bib_meta.dw_rt_files 
WHERE dw_batch_id IN (8280846);
DELETE FROM bib_meta.dw_rt_run_errors 
WHERE dw_run_id IN (8894265);
DELETE FROM bib_meta.dw_rt_batches 
WHERE dw_batch_id IN (8280846);
DELETE FROM bib_meta.dw_rt_runs 
WHERE dw_batch_id IN (8280846);
commit;
/

delete from dw_rt_files where DW_FILE_KEY = 455512189;
commit;

--------------E.G INTERSECT  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



SELECT COLUMN_NAME || ','  FROM ALL_TAB_COLUMNS WHERE OWNER = 'STG_GEN' AND TABLE_NAME = 'SUBS_POSTPAID_SOURCE' 
INTERSECT SELECT COLUMN_NAME || ','  FROM ALL_TAB_COLUMNS@EXADATA WHERE OWNER = 'STG_GEN' AND TABLE_NAME = 'SUBS_POSTPAID_SOURCE' ;


select 
task, 'update dw_rt_runs set run_status = ''RUNABLE'', rows_loaded = null where dw_batch_id = '||dw_batch_id||' and task = '''||task||''';'
from dw_Rt_runs
where dw_batch_id in (
select batch_id 
from dl_P_20170322_batches_tmp)
and task <> 'TG_CDR_PREPAID_RATED';



update dw_rt_runs set start_date = sysdate - 30 where dw_run_id in
(
select 
dw_run_id 
--task, run_status,  dw_run_id, 'update dw_rt_runs set run_status = ''RUNABLE'', rows_loaded = null where dw_batch_id = '||dw_batch_id||' and task = '''||task||''';'
from dw_Rt_runs
where dw_batch_id in (
select batch_id 
from dl_P_20170322_batches_tmp)
and task <> 'TG_CDR_PREPAID_RATED'
)
and run_status = 'RUNABLE'
;

--------------d-1 durations --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select a. *, case  when dur  >= 1 then 'NOT MET' else 'MET' end status ,
case  when dur  >= 1 then 1  else 0  end  MRK
from 
(
select a.date_key,  a.startdate, b.enddate, 
numtodsinterval(enddate-startdate,'day') duration ,
(enddate-startdate) dur
from

(select diy_parameter  date_key, max(start_date) startdate from dw_rt_runs where task = 'STD_TE_I2S_POSTPAID_SUBSCRIBER_BASE_PULL'
and diy_parameter  >  '20170715'
group  by diy_parameter  )a ,
(select diy_parameter  date_key , min(start_date)  enddate from dw_rt_runs where task = 'STD_C2E_FCT_SUBS_FULL_SUMD'
and diy_parameter > '20170715'
group  by diy_parameter  )b
where a.date_key = b.date_key
order by a.date_key asc
) a order by date_key
/



update dw_rt_diy_list
set processing_status = 'LOCKED'
where dw_task_id in
 (
11175,
11176
 )
and parameter  <   '20170412'
--and processing_status like 'LOCKED'
;
/

commit;




update dw_rt_batches set load_status = 'CANCELLED'
WHERE DW_BATCH_ID = 4314802;






select count(*)   from STG_CDR.MSC_VOICE_TR WHERE BATCH_ID = 4398557
--AND DW_FILE_ID = 269672430
;


SELECT COUNT(*) FROM STG_CDR.MSC_SMS WHERE BATCH_ID = 
4308568;

UPDATE DW_RT_FILES SET PROCESSING_STATUS = 'REGISTERED'
WHERE DW_BATCH_ID IN 
(
SELECT DW_BATCH_ID FROM DW_RT_RUNS WHERE TASK = 'TF_S2S_MSC_VOICE_ZA'
AND RUN_STATUS = 'FAILED'
AND START_DATE > SYSDATE - 0.1
);



select *  from STG_CDR.SMSC_CDR SUBPARTITION FOR (201604) 
 where date_key=-1 and dw_subpart=0 ;
 
 
 select distinct dw_file_id from STG_CDR.CTA_4568204
 where n_batch_id = 4568204 
 and  date_key  = -1;
 
SELECT MSISDN_NSK, COUNT (*)
    FROM BIB_CDR.FCT_PREPAID_FIRST U
  HAVING COUNT (*) > 1
GROUP BY MSISDN_NSK;

--truncate table BIB_CDR.FCT_PREPAID_LAST ;]


select short_filename from dw_rt_files where dw_batch_id in
                                (
                                seleCt dw_copied_batch_id from dw_rt_batches where dw_batch_id in

                                (
                                select dw_batch_id from dw_rt_runs where task = 'STD_TE_S2C_FCT_CDR_INTERCONNECT' and run_statuS = 'FAILED'
                                )
)
;


SELECT * FROM STG_CDR.INTERCONNECT_CDR WHERE  BATCH_ID = 4307053;

update dw_rt_runs set run_status = 'CANCELLED' where task = 'STD_TE_S2S_INT_SUBSCRIBER' and dw_batch_id <>  4612529
and run_status = 'RUNNING';


select  max(file_date)  from STG_GEN.HRIS_ASSIGNMENT;



update DW_TASKS 
set FILE_REGISTER_DIRECTORY = replace (FILE_REGISTER_DIRECTORY, '/CDR' , ''),
FILE_PROCESS_DIRECTORY =   replace (FILE_PROCESS_DIRECTORY, '/CDR' , ''),
 FILE_SUCCESS_DIRECTORY =   replace (FILE_SUCCESS_DIRECTORY, '/CDR' , ''),
 FILE_ARCHIVE_DIRECTORY =   replace (FILE_ARCHIVE_DIRECTORY, '/CDR' , ''),
 FILE_ERROR_DIRECTORY =   replace (FILE_ERROR_DIRECTORY, '/CDR' , '')
where task = 'STD_TF_S2S_HRIS_ADDRESS';


select * from stg_cdr.er_hlr_norm;


select * from dba_directories
where directory_name = 'DW_LIST_DIR_SAPROD';


select * from dw_rt_runs where dw_batch_id = 4560447;


SELECT * from I_EPRO_IVRSERVICEAUDIT_FL@BIB_INT.MTN.CO.ZA ;






select *
from
UMD.UMD_WALLET
--where payment_option_cd is null
;


select * from dw_rt_files where dw_batch_id in(

select  dw_batch_id   from dw_rt_batches where dw_task_id =18106
--and table_name = 'STG_GEN.I_CDR_PREPAID_SNAPD'
and dw_date_key = '20170404'
);




select * from STG_CDR.INTERCONNECT_CDR where BATCH_ID=4306942 
and DW_FILE_ID= 265720518 and MSISDN_NSK=27734485741 and DW_FILE_ROW_NUMBER=465317;

create table mikes_hlr as
SELECT * from STG_CDR.CTA_FIL5137597;


select DISTINCT SCHAR_CD  from STG_CDR.HLR_NORM PARTITION FOR (20170406);
--WHERE SCHAR_CD 


select * from dw_rt_runs where task like '%CLF%' AND RUN_STATUS = 'FAILED';




update dw_tasks set max_file_load_concurrency = 10 where task in (
'TF_S2S_CS6_AIR_CDR'
,'TF_S2S_CS6_SDP_CDR'
,'TF_S2S_GGSN_CDR'
,'TF_S2S_HLR_NORM_ZA'
,'TF_S2S_IBF_XDR_ZA'
,'TF_S2S_INTERCONNECT_CDR_ZA'
,'TF_S2S_MACH_TAPIN'
,'TF_S2S_MACH_TAPOUT'
,'TF_S2S_MMC_CDR'
,'TF_S2S_MSC_CDR'
,'TF_S2S_MSC_SMS_ZA'
,'TF_S2S_MSC_VOICE_TR_ZA'
,'TF_S2S_MSC_VOICE_ZA'
,'TF_S2S_SASN_CDR'
,'TF_S2S_SGSN_CDR_ZA'
,'TF_S2S_SMSC_CDR_ZA'
,'TF_S2S_XDR_SV_ZA',
'TF_S2S_CS6_CCN_CDR');


select  *   from STG_GEN.I_CDR_PREPAID_SNAPD PARTITION FOR (20170814)
WHERE 1=1
--AND BATCH_ID = 4060903
AND MSISDN_NSK  = 27638305463
--GROUP BY  BATCH_ID
;


exec dbms_scheduler.drop_job('BIB_CTL.BIB_SCHED_7_5',force=>true);
exec dbms_scheduler.drop_job('BIB_CTL.BIB_SCHED_3_9',force=>true);


--------------------------dropping-----------------------------------
SELECT  job_name ,
CASE
WHEN STATE='RUNNING'
THEN (CURRENT_TIMESTAMP-LAST_START_DATE)
ELSE NULL
END MINS_RUNNING,
'DW_BIB_UTL.DROP_CTL_JOB('''||JOB_NAME||''');',
TO_CHAR(A.LAST_START_DATE,'YYYYMMDD HH24:MI:SS') LAST_START_DT,
A.LAST_RUN_DURATION,
 STATE
FROM DBA_SCHEDULER_JOBS A
WHERE OWNER         ='BIB_CTL'
--AND STATE           ='RUNNING'
AND JOB_NAME LIKE '%BXX%'
--AND LAST_START_DATE < CURRENT_TIMESTAMP-0.1
ORDER BY 1 DESC NULLS LAST

;





select * from dw_environment_variables
where  variable_value = 'bib_read/bib_read'
where variable_name = 'SHELL_DUMP_LOGIN';



select * from dl_P_20170322_batches_tmp;

create table PREPAID_RATED18_TEMP AS 
(
select DW_BATCH_ID  from dw_rt_batches where table_name = 'BIB_CDR.FCT_CDR_PREPAID_RATED' and dw_date_key = '20170418'
);

UPDATE DW_RT_RUNS SET 
--START_DATE  = TO_DATE('20170509 20:15:30', 'YYYYMMDD HH24:MI:SS' ),
END_DATE  = TO_DATE('20170509 20:28:56', 'YYYYMMDD HH24:MI:SS' )
WHERE DW_RUN_ID = 6130635;





select * from USER_ROLE_PRIVS where USERNAME='SAMPLE';
select * from USER_TAB_PRIVS where Grantee = 'SAMPLE';
select * from USER_SYS_PRIVS where USERNAME = 'SAMPLE';






select 
A.INDEX_OWNER,
A.INDEX_NAME,
B.TABLE_NAME,
--A.PARTITION_NAME,
--A.TABLESPACE_NAME,
A.STATUS,
COUNT(*) COUNTS,
'INVALID INDEXED PARTITION' TYPE
FROM DBA_IND_PARTITIONS A 
JOIN ALL_INDEXES B 
ON A.INDEX_OWNER=B.OWNER 
AND A.INDEX_NAME =B.INDEX_NAME
WHERE 1=1
AND A.INDEX_OWNER IN ('STG_GEN','BIB_CDR','BIB','BIB_AUX','BIB_META','BIB_CTL','STG_CDR')
AND A.STATUS = 'UNUSABLE'
GROUP BY
A.INDEX_OWNER,
A.INDEX_NAME,
'INVALID INDEXED PARTITION',
--A.PARTITION_NAME,
--A.TABLESPACE_NAME,
A.STATUS,
B.TABLE_NAME
UNION ALL
--INVALID INDEXED SUBPARTITIONS:i
select 
A.INDEX_OWNER,
A.INDEX_NAME,
B.TABLE_NAME,
--A.PARTITION_NAME,
--A.TABLESPACE_NAME,
A.STATUS,
COUNT(*) COUNTS,
'INVALID INDEXED SUBPARTITION' TYPE
FROM DBA_IND_SUBPARTITIONS A 
JOIN ALL_INDEXES B 
ON A.INDEX_OWNER=B.OWNER AND A.INDEX_NAME =B.INDEX_NAME
WHERE 1=1
AND A.INDEX_OWNER IN ('STG_GEN','BIB_CDR','BIB','BIB_AUX','BIB_META','BIB_CTL', 'STG_CDR')
AND A.STATUS = 'UNUSABLE'
GROUP BY 
A.INDEX_OWNER,
A.INDEX_NAME,
'INVALID INDEXED SUBPARTITION',
--A.PARTITION_NAME,
--A.TABLESPACE_NAME,
A.STATUS,
B.TABLE_NAME
UNION ALL
--INVALID INDEXES:
select 
owner, 
index_name, 
A.TABLE_NAME,
--NULL,
--TABLESPACE_NAME,
status,
COUNT(*) COUNTS,
status||' INDEX' TYPE
FROM ALL_INDEXES A
where OWNER IN ('STG_GEN','BIB_CDR','BIB','BIB_AUX','BIB_META','BIB_CTL', 'STG_CDR')
and status in ('INVALID','UNUSABLE')
GROUP BY
owner, 
index_name, 
A.TABLE_NAME,
status||' INDEX',
--NULL,
--TABLESPACE_NAME,
status
ORDER BY 5 DESC
;




SELECT datetime, stream, filename, record_count, bytes, create_dt last_refreshed FROM STG_CDR.EMM_BIB_AUDIT_MISSING_FILES;




select * from ALL_DB_LINKS 
--where DB_LINK='INT_POSTPAID_LINK'
;


select * from 
  ARADMIN.SRM_Request@SRC_ZA_SREMEDY
 ST ;
 
 
 

select * from UMD.UMD_SOURCE_SPECIFIC_RULES 
     where source_system_rule_cd='STD_TE_S2S_REM_SRM_REQUEST';
     
     
     
     select * from STG_CDR.CTA_SV_6560710
     where ne_charge_start_date > sysdate + 1;
     
     
     
     select * from ALL_SYNONYMS where SYNONYM_NAME='I_BILLING_TRANSACTION_RECEIPT';
     
     
     
     select * from umd.umd_source_specific_rules
     where source_system_rule_cd  like 'STD_TE_S2S_REM_SRM_REQUEST' ;
     
     
     
     select * from STG_CDR.CTA_SV_6599491;



select  NE_CHARGE_START_DATE  from STG_CDR.XDR_SV  
WHERE to_date(NE_CHARGE_START_DATE,'YYYYMMDD hh24:mi:ss')  > SYSDATE - 1 
--<= SYSDATE
;

SELECT SYSDATE FROM DUAL;


 select * from STG_CDR.CTA_SV_6600661;
 
 
 select  *  from bib.dim_usage_type 
-- where USAGE_TYPE_CD ='SMS'
 ;
 
 
 select BIB_CTL.PKG_USAGE_TYPE.lku(A.USAGE_TYPE_CD,'STD_TE_S2C_FCT_CDR_POSTPAID_RATED_INT')   from dual ;
 
 
 
 
 select *  
 FROM STG_CDR.XDR_SV 
WHERE BATCH_ID = 6196231
AND CH_PARTITION_NR_1 NOT IN (1, 15)                            --Ignore WASP records
                              -- This will ignore all roaming inbound and wasp events
AND NE_NORMALISED_EVENT_TYPE_ID = 1000064  --Only take usage records (i.e. ignore tap)
                                                        --and ne_event_type_code = 108
AND NVL (ne_general_9, 0) != '2'                                          --ignore MTC
AND ne_event_source = '2'                                   --Only classic subscribers
AND DW_FILE_ROW_NUMBER > 1 
;


-- Ignore headers already loaded 




select * from xdr_sv.MSC_SV_COMPARE
order by sdate desc;]


select dbms_metadata.get_ddl('TABLE','I_SD_TRANSACTION_TYPE','STG_GEN') from dual;




--  CREATE OR REPLACE FORCE EDITIONABLE VIEW "XDR_SV"."MSC_SV_COMPARE" ("SDATE", "MSC_CNT", "MSC_DUR", "SV_DUR", "SV_CNT", "CNT_DISC", "%cnt", "DUR_DISC", "%dur") AS 
  SELECT a."SDATE",
         a."MSC_CNT",
         a."MSC_DUR",
         sv_dur,
         sv_cnt,
         msc_cnt - sv_cnt cnt_disc,
         ROUND ( (msc_cnt - sv_cnt) * 100 / msc_cnt, 2) "%cnt",
         msc_dur - sv_dur dur_disc,
         ROUND ( (msc_dur - sv_dur) * 100 / msc_dur, 2) "%dur"
    FROM (  SELECT sdate, SUM (totalcalls) msc_cnt, SUM (totalduration) msc_dur
              FROM MSC_VOICE_OICK_SUM_DD
             WHERE sdate >= TRUNC (SYSDATE - 45, 'mm')
          GROUP BY sdate) a,
         (  SELECT sdate, SUM (dur) sv_dur, SUM (cnt) sv_cnt
              FROM SV_XDR_CLASSIC_REV_DD
             WHERE     sdate >= TRUNC (SYSDATE - 45, 'mm')
                   AND event_type IN ('VOICE', 'VIDEO')
          GROUP BY sdate) b
   WHERE a.sdate = b.sdate
ORDER BY a.sdate;


PREPAID_SNAPD
select  batch_id   FROM 
 STG_CDR.XDR_SV  partition for (20170529) b
WHERE 1=1
--BATCH_ID = 6199115
AND CH_PARTITION_NR_1 NOT IN (1, 15)                       
AND NE_NORMALISED_EVENT_TYPE_ID = 1000064  
AND NVL (ne_general_9, 0) != '2'  
AND ch_general_2 = 'MVNO'
;



select * from bad_data where batch_id = 9560382;

9560382
9560372

select * from stg_cdr.xdr_sv where date_key  = -1;

ENCLOSED BY '' LRTRIM



SELECT * FROM DW_COLUMN_MAPPINGS
-- WHERE     task LIKE 'TF_S2S_XDR_SV_ZA'
 WHERE mapping_function like '%SUBSTR%'
--       AND TARGET_COLUMN_NAME = 'ADDITIONAL_DESCRIPTION'
       ;

 SELECT *  /*+ INDEX(TGT) */ from STG_CDR.XDR_SV PARTITION FOR (20151018) TGT 
 where date_key= -1 
-- and dw_subpart=0 
--and batch_id=8107122
;

------------------SINGLE VIEW FIX ------------------------------------------------

delete  FROM STG_CDR.XDR_SV  
WHERE BATCH_ID = 8091853
AND CH_PARTITION_NR_1 NOT IN (1, 15)                 --Ignore WASP records AND all roaming inbound events
AND NE_NORMALISED_EVENT_TYPE_ID = 1000064  --Only take usage records (i.e. ignore tap)                                            
AND NVL (ne_general_9, 0) != '2'                                --ignore MTC
AND ne_event_source = '2'                                         --Only classic subscribers
AND DW_FILE_ROW_NUMBER > 1  
and ne_a_party_network_id = 'Unavailable' -- Ignore headers already loaded 
;
commit;



select SERVED_IMSI_NR, SERVED_MSISDN_NR, a.* from 
(
SELECT 'SV' SOURCE_SYSTEM_CD,
       'SV' BASE_STATION_TYPE,
       CASE WHEN ne_event_type_code = 105 THEN NULL ELSE ne_b_party_id END
          OTHER_NR,
       b.NE_CHARGE_START_DATE CDR_TS,
       NULL OUT_TRUNK_ROUTE_CD,
       NULL IN_TRUNK_ROUTE_CD,
       b.ne_general_11 SERVED_IMEI_NR,
       CASE WHEN ne_event_type_code = 105 THEN ne_b_party_id ELSE NULL END
          APN_CD,CASE WHEN ( NE_GENERAL_10 LIKE 'S%' AND NE_A_PARTY_ID != NE_C_PARTY_ID) THEN NE_C_PARTY_ID
       ELSE NE_A_PARTY_ID END  SERVED_MSISDN_NR /* For CALL FORWARDING NE_C_PARTY_ID is the SERVED_MSISDN_NR OLD rule is NE_A_PARTY_ID SERVED_MSISDN_NR */
 ,
       NVL (b.ne_a_party_network_id, 0) SERVED_IMSI_NR,
       CASE
          WHEN ne_event_type_code IN (70,
                                      105,
                                      107,
                                      108,
                                      109,
                                      3001)
          THEN
             parser_pkg.getfield (ne_general_8, 1, '|')
          ELSE
             NULL
       END
          BASE_STATION_ID,
       DECODE (ne_event_type_code,
               100, 'CON',
               102, 'SMS',
               103, 'MMS',
               104, 'VAS',
               105, 'DAT',
               107, 'VOI',
               108, 'DAT',
               3001, 'VTE',
               3002, 'VOI',
               'VAS')
          USAGE_TYPE_CD,
       ne_event_type_code || '-' || ne_event_sub_type_code USAGE_SUBTYPE_CD,
       b.ch_general_2 PACKAGE_CD,
       NULL TARIFF_PLAN_CD,
       CASE WHEN CH_CURRENCY_ID = 1 THEN 'ZAR' ELSE NULL END CURRENCY_CD,
       NULL RATING_RULE_CD,
       NULL DISCOUNT_CD,
       SUBSTR (ne_general_11, 1, 8) TAC,
       DECODE (ne_event_source,  '1', 'P',  '2', 'N',  'H') PAYMENT_OPTION_CD,
       NULL CALL_REFERENCE_NR,
       NULL SEQ_NUMBER_CD,
       CASE WHEN ne_general_9 = 1 THEN 'O' ELSE NULL END DIRECTION_CD,
       b.ne_general_10 BEARERSERVICE_CD,
       b.ne_general_10 TELESERVICE_CD,
       parser_pkg.getfield (ch_general_9, 2, '|') BUNDLE_CD,
       parser_pkg.getfield (ch_general_10, 2, '|') BILLING_CYCLE_FULL_CD,
       NULL END_BASE_STATION_ID,
       NULL OTHER_BASE_STATION_ID,
       NULL OTHER_END_BASE_STATION_ID,
       b.BATCH_ID BATCH_ID,
       b.DW_FILE_ID DW_FILE_ID,
       b.DW_FILE_ROW_NUMBER DW_FILE_ROW_NUMBER,
       NVL (NE_VOLUME, 0) BYTES_SENT_QTY,
       0 BYTES_RECEIVED_QTY,
       b.CH_CHARGE CALL_COST_AMT,
       b.NE_DURATION CALL_DURATION_QTY,
       dw_bib_etl_utl.dw_clean_to_number (NVL (SUBSTR (CH_GENERAL_8,
                                                       1,
                                                         INSTR (CH_GENERAL_8,
                                                                ':',
                                                                1,
                                                                1)
                                                       - 1),
                                               0))
          DISCOUNT_AMT,
       CASE
          WHEN ch_general_1 = '1' AND NE_EVENT_SUB_TYPE_CODE IN (2019, 3005)
          THEN
             0
          ELSE
             ne_duration
       END
          CHARGED_DURATION_QTY,
       NULL FAF_IND,
       CASE
          WHEN (ne_event_type_code = 105 AND ne_b_party_id = ne_c_party_id)
          THEN
             1
          ELSE
             0
       END
          REVERSE_BILL_CNT,
       CASE WHEN b.CH_CHARGE = 0 AND ch_general_9 IS NULL THEN 1 ELSE 0 END
          ZERO_RATED_CNT,
       NULL NETWORK_CD,
       NULL UNITIZED_DURATION_QTY,
       b.CH_CHARGE * 0.14 TAX_AMT,
       b.CH_CHARGE ,
       NULL EXCISE_TAX_AMT,
       CASE WHEN ch_general_9 IS NULL THEN 0 ELSE 1 END IN_BUNDLE_CNT,
       NULL PROMO_CNT,
       b.CH_CHARGE * 1.14 CHARGE_AMT,
       NULL TRAVEL_TIME_QTY,
       dw_bib_etl_utl.dw_clean_to_number (ch_general_5) DURATION_UNITS_QTY,
       NULL FULL_DISCOUNT_CNT,
       NULL FULL_DISCOUNT_DURATION_QTY
  FROM STG_CDR.XDR_SV B 
WHERE BATCH_ID = 8091853
 
-- 8091853
--8112629
 
 
 
AND CH_PARTITION_NR_1 NOT IN (1, 15)                 --Ignore WASP records AND all roaming inbound events
AND NE_NORMALISED_EVENT_TYPE_ID = 1000064  --Only take usage records (i.e. ignore tap)                                            
AND NVL (ne_general_9, 0) != '2'                                --ignore MTC
AND ne_event_source = '2'                                         --Only classic subscribers
AND DW_FILE_ROW_NUMBER > 1                              -- Ignore headers already loaded 

 
)  a where  not  regexp_like (SERVED_IMSI_NR,'^[[:digit:]\.[:digit:]]+$')  
;
BIB_CTL.DW_ZA_UTL.STANDARD_MSISDN_NSK(A.SERVED_MSISDN_NR,A.SERVED_IMSI_NR) MSISDN_NSK
function STANDARD_MSISDN_NSK (in_msisdn varchar2, in_imsi number) return integer  parallel_enable ;
--run_id 8694192


select BIB_CTL.DW_ZA_UTL.STANDARD_MSISDN_NSK(27100650000 ,'Unavailable') MSISDN_NSK   from dual;


UPDATE DW_RT_RUNS SET RUN_STATUS = 'HOLD_MK' WHERE TASK = 'STD_C2C_MSISDN_RECYCLE'
AND TRUNC(START_DATE ) =  TO_DATE('20170808 00:00:00' , 'YYYYMMDD HH24:MI:SS')
AND RUN_STATUS  = 'RUNABLE';



SELECT TRUNC(START_DATE )  FROM DW_RT_RUNS WHERE TASK = 'STD_C2C_MSISDN_RECYCLE'
AND TRUNC(START_DATE ) =  TO_DATE('20170808 00:00:00' , 'YYYYMMDD HH24:MI:SS')
AND RUN_STATUS  = 'RUNABLE'
;




begin
dw_bib_utl.insert_dependency(  
  pi_parent_task_like =>'STD_BRT_SUBSCRIBER',
  pi_dependent_task_like =>'STD_TE_C2C_MSISDN_RECYCLE',
  pi_SUPER_BATCH_IND =>'N',
  pi_DEPENDENCY_TYPE=> 'ANY_BATCH_WAIT');
end;
 /
 
 
 BEGIN
DW_BIB_UTL.DROP_CTL_JOB('MIKE_BRT_FIXGAPS12');
--DW_BIB_UTL.DROP_CTL_JOB('MIKE_BRT_FIXGAPS11');
END;
/


SELECT ''''|| DEPENDENT_TASK || ''',' FROM DW_TASK_DATA_DEPEND WHERE DEPENDENT_TASK  = 'EXEC_EMM_BIB_AUDIT_DETAIL';






select
--substr(DA_DTL,2) DA_DTL,
a.* from STG_CDR.CS6_SDP_RAW_CDR a
--where MSISDN_NSK = 27787889160
where  BATCH_ID = 10108710
and dw_file_id = 483675061
;





select length(DA_DTL) - length(replace(DA_DTL, '#', null))  ||  substr(DA_DTL,INSTR(da_dtl,'#',1,1)) DA_DTL , a.* from STG_CDR.CS6_CCN_RAW_CDR a
where MSISDN_NSK = 27787889160
and BATCH_ID = 8019160;



select length(DA_DTL) - length(replace(DA_DTL, '#', null))||substr(DA_DTL,INSTR(da_dtl,'#',1,1)) DA_DTL , a.* from STG_CDR.CS6_CCN_RAW_CDR a
where MSISDN_NSK = 27787889160
and BATCH_ID = 8019160;




SELECT * FROM BIB_CTL.VZA_PAYMENT_OPTION;




select dbms_metadata.get_ddl('VIEW','VS_PAYMENT_OPTION','BIB_CTL') from dual;
/




SELECT  BIB_CTL.PKM_CDR_REPORTING_PERIOD.lu(BIB_CTL.DW_UTL_REMOTE.DW_DATE_KEY('20170822 12:00:22') ,'P') CDR_REPORTING_PERIOD_NR FROM DUAL;




delete from 
STG_GEN.I_CDR_PREPAID_SNAPD PARTITION FOR (20170822)
where 1=1
and  PAYMENT_OPTION_CD = 'I'
and batch_id = 8574479
;

/



 select * FROM STG_CDR.CS6_AIR_RAW_CDR PARTITION FOR (20170801)
WHERE 1=1
--AND BATCH_ID = 8763088
AND TXN_CD IS  NULL
;

--update dw_rt_diy_list
--set processing_status = 'LOCKED'
SELECT * FROM dw_rt_diy_list
where 1=1
--parameter > '20170727'
and Dw_task_id in
(
11212

);







update dw_rt_diy_list
--set processing_status = 'REGISTERED'
set processing_status = 'LOCKED'
where parameter  <  '20170919'
and Dw_task_id in
(
11160,
11175,
11176
)
;

COMMIT;



select * from dw_tasks where dw_task_id in

(

11160,
11175,
11176
)
;



SELECT * FROM STG_CDR.IBF_XDR
WHERE UNIQUE_RECORD_ID IN
(
269815276594630414920170920095432311,
269815276594630415020170920095451311,
269815276594622127220170919114950310,
326347019669729794120170831082128310,
326347019669729794220170831082226311,
326347019669729794320170831082240312,
326347019669729794420170831082357310
)
;




ALTER TABLE STG_CDR.IBF_XDR ADD EFFECTIVERATE             NUMBER (12);
--COMMIT;



select * from  STG_CDR.INTERCONNECT_CDR
where batch_id in (
9833016
--9833009,
--9832887,
--9832880,
--9832879,
--9832875
);



select  /* + PARALLEL 30 */ *   from dw_rt_files where 1=1
and  dw_batch_id   in
(
9833016,
9833009,
9832887,
9832880,
9832879,
9832875
)
and dw_task_id in dw_get_task_id('TF_S2S_INTERCONNECT_CDR_ZA')
;






SELECT COUNT(*) TOT_SUBSCRIBERS,
PART_RGS, BASE_STATION_TECHNOLOGY_TXT
FROM
(

SELECT
CASE WHEN RGS30_CNT = 1 THEN 'Y' ELSE 'N' END PART_RGS,
BASE_STATION_TECHNOLOGY_TXT

FROM
(


--------------------------------Most Cell breakdown ---------------------------------------------------------------------------------------------------------------------


SELECT * FROM 
BIB.FRU_SUBSCRIBER_SNAPD A
--PARTITION FOR (20171001) A 
LEFT JOIN BIB.DIM_BASE_STATION B
ON  A.BASE_STATION_KEY  = B.BASE_STATION_KEY
WHERE DATE_KEY >=  '20170901' AND DATE_KEY <= '20170930'
)

)
GROUP BY PART_RGS, BASE_STATION_TECHNOLOGY_TXT;





SELECT
MSISDN_NSK,
CASE WHEN RGS30_CNT = 1 THEN 'Y' ELSE 'N' END PART_RGS,
BASE_STATION_TECHNOLOGY_TXT

FROM
(

SELECT * FROM 
BIB.FCT_SUBSCRIBER_SNAPD 
PARTITION FOR (20170930) A 
LEFT JOIN BIB.DIM_BASE_STATION B
ON  A.BASE_STATION_KEY  = B.BASE_STATION_KEY
--WHERE DATE_KEY >=  '20170901' AND DATE_KEY <= '20170930'
)

--)
--GROUP BY PART_RGS, BASE_STATION_TECHNOLOGY_TXT


---------------------------------SUBSCRIBERS PER CELL -----------------------------


CREATE OR REPLACE VIEW BIB.VW_OWS_SUBS
AS
     SELECT /*+ parallel 5 */
           date_key, base_station_id cgi, SUM (closingbase_cnt) rgs_90_cnt
       FROM bib.FRU_SUBSCRIBER_HL_SNAPD a, bib.dim_base_station b
      WHERE     a.base_station_key = b.base_station_key(+)
            AND date_key =
                   (SELECT TO_CHAR (TO_DATE (MAX (diy_parameter), 'YYYYMMDD'),
                                    'YYYYMMDD')
                      FROM dw_rt_runs
                     WHERE     task LIKE 'STD_E2E_FRU_SUBSCRIBER_HL_SNAPD'
                           AND run_status IN ('CLOSED', 'SUCCESS')
                           AND diy_parameter < TO_CHAR (SYSDATE, 'YYYYMMDD') /* Ingore current month */
                           AND rows_loaded > 0    /* Records must be loaded */
                                              )
   GROUP BY date_key, base_station_id
 ;
 
 
 
 
 CREATE OR REPLACE VIEW BIB.VW_OWS_SUBS
AS
SELECT /*+ parallel 5 */
           date_key, base_station_id cgi,BASE_STATION_CD cell_id,BASE_STATION_LAC_CD site_id,  SUM (closingbase_cnt) rgs_90_cnt
       FROM bib.FRU_SUBSCRIBER_HL_SNAPD a, bib.dim_base_station b
      WHERE     a.base_station_key = b.base_station_key(+)
            AND date_key =
                   (SELECT TO_CHAR (TO_DATE (MAX (diy_parameter), 'YYYYMMDD'),'YYYYMMDD')
                      FROM dw_rt_runs
                     WHERE     task LIKE 'STD_E2E_FRU_SUBSCRIBER_HL_SNAPD'
                           AND run_status IN ('CLOSED', 'SUCCESS')
                           AND diy_parameter < TO_CHAR (SYSDATE, 'YYYYMMDD') /* Ingore current month */
                           AND rows_loaded > 0                              /* Records must be loaded */
                    ) 
   GROUP BY date_key, base_station_id, base_station_cd,base_station_lac_cd
   /
   
   


select
'DECLARE
v_pi_run_id  integer := ' || DW_LOAD_RUN_ID || ';' || chr(10) ||
'BEGIN
DW_FILE_TASK_ARCHIVE(''' || task  || ''',v_pi_run_id);
dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
END;
/
' 
from (
select /*+ parallel(10)*/   
distinct DW_LOAD_RUN_ID,  (select task from dw_tasks b where a.DW_TASK_ID = b.DW_TASK_ID and task_type  like 'FILE' ) task
from dw_rt_files  a 
WHERE  DW_TASK_ID in (select dw_task_id from dw_tasks where task_type  like 'FILE' and task like 'TF_S2S_INTERCONNECT_CDR_ZA'
and short_filename like '%20171002%'
and register_datetime  >   sysdate -  30
)
and PROCESSING_STATUS   like 'LOADED'
)
;
/
   
   
   
   select * from bib.dim_base_station
--   WHERE RECORD_FORCE_SOURCE IS NOT NULL
--   and RECORD_FORCE_SOURCE not like '%RATED%'
;
   
   SELECT * FROM STG_CDR.MAPS_INV_4G PARTITION FOR (20170502)
   WHERE CELL_ID LIKE 'L01005%';
   
   
   SELECT  BIB_META.dw_utl.clob_log_get_clob(84990054) FROM DUAL;