
-------------------------------------------ERROR  RAW :-------------------------------------------------------------------
--select sum(failed_runs) from
--(

SELECT 
         to_number(to_char(TRUNC (a.date_stamp),'YYYYMMDD')) DATE_STAMP,
         b.task,
         COUNT (DISTINCT b.dw_run_id) FAILED_RUNS,
         COUNT (DISTINCT A.ERROR_CODE) unique_errors,
         MAX (ERROR_DESCRIPTION) max_error_descriptions,
         MIN (ERROR_DESCRIPTION) min_error_descriptions
    FROM bib_meta.dw_rt_run_errors a, bib_meta.dw_rt_runs b
   WHERE a.dw_run_id = b.dw_run_id
   and TRUNC (a.date_stamp) > to_date('20170923' , 'yyyymmdd') and TRUNC (a.date_stamp) < to_date('20171023' , 'yyyymmdd')
 --   AND (B.TASK LIKE  'TF_%' OR B.TASK LIKE  'SDT_TF_%')
GROUP BY TRUNC (a.date_stamp),
         b.task        
ORDER BY TRUNC (a.date_stamp) DESC, 3 DESC
--)
;


-------------------------------------------ERROR ANALYSIS :-------------------------------------------------------------------
SELECT 
DATE_STAMP,
task_business_area,

case  
when max_error_descriptions like '%unique constraint%' Then 'Unique Constraint'
when max_error_descriptions like '%NULL%' then 'Source Issue'
when max_error_descriptions like '%DEADLOCK RETRY%' then 'ETL Bug'
when max_error_descriptions like '%inserted partition%' then 'Partition Provision issue'
when max_error_descriptions like '%object no longer exists%' then 'ETL Bug'
when max_error_descriptions like '%partitioning key maps%'  then 'Partition Provision issue'
when max_error_descriptions like '%deadlock detected%'  then 'DB Locks Issue'
when max_error_descriptions like '%table or view does%'  then 'DB Locks Issue'
when max_error_descriptions like '%table or view does%'  then 'DB Locks Issue'
when max_error_descriptions like '%cannot access a remote%'  then 'Connection issues'
when max_error_descriptions like '%UNUSABLE UNIQUE INDEX%'  then 'Invalid indexes'
when max_error_descriptions like '%Specified subpartition does%'  then 'DB Locks Issue'
when max_error_descriptions like '%numeric or value error%'  then 'Bad Data'
when max_error_descriptions like '%unable to get a stable set%'  then 'Unique Constraint'
when max_error_descriptions like '%duplicate keys%'  then 'Unique Constraint'
when max_error_descriptions like '%unable to extend temp%'  then 'Space issues'
when max_error_descriptions like '%NOT IN LKU_WALLET%'  then 'Bad Data'
when max_error_descriptions like '%No OPEN PERIODS FOUND%'  then 'Bad Data'
when max_error_descriptions like '%timeout%'  then 'DB Locks Issue'
when max_error_descriptions like '%End Date Cant%' then 'BRT Dimension issues' 
when max_error_descriptions like'%is read-only%' then 'TS out of space' 
when max_error_descriptions like '%DATAFILE%' then 'Datafile Issue' 
when max_error_descriptions like '%Natural Key Lookup Failed%' then 'Source Issue'
when max_error_descriptions like '%Lock returned value%' then 'DB Locks Issue'
when max_error_descriptions like '%executing ODCIEXTTABLEFETCH%' then 'Source Issue'
when max_error_descriptions like '%value too large for column%' then 'Source Issue'
when max_error_descriptions like '%inconsistent datatypes%' then 'Source Issue'
when max_error_descriptions like '%literal does not match format%' then 'Source Issue'
when max_error_descriptions like '%insufficient privileges%' then 'Source Issue'
else 'UNKN'
end ERROR_CATEGORY
--,max_error_descriptions
,FAILED_RUNS,
unique_errors

from 
(
select a.DATE_STAMP, a.FAILED_RUNS, a.max_error_descriptions, b.task_business_area, a.unique_errors from 
  (
SELECT 
         to_number(to_char(TRUNC (a.date_stamp),'YYYYMMDD')) DATE_STAMP,
         b.task,
         COUNT ( b.dw_run_id) FAILED_RUNS,
         COUNT (DISTINCT A.ERROR_CODE) unique_errors,
         MAX (ERROR_DESCRIPTION) max_error_descriptions
        FROM bib_meta.dw_rt_run_errors a, bib_meta.dw_rt_runs b
        WHERE a.dw_run_id = b.dw_run_id
           and TRUNC (a.date_stamp) > to_date('20170923' , 'yyyymmdd') and TRUNC (a.date_stamp) < to_date('20171023' , 'yyyymmdd')
--       and TRUNC (a.date_stamp) < (sysdate-30)
        GROUP BY TRUNC (a.date_stamp),  b.task      
)a  join BIB_META.DW_TASKS b on   a.task = b.task
)
ORDER BY DATE_STAMP DESC

;



-------------------------------------------ERROR BUSINESS AREA :-------------------------------------------------------------------

select  DATE_STAMP, task_business_area,  failed_runs  from

(

SELECT 
to_number(to_char(TRUNC (a.date_stamp),'YYYYMMDD')) DATE_STAMP,
 b.task,
COUNT ( b.dw_run_id) FAILED_RUNS,
MAX (ERROR_DESCRIPTION) max_error_descriptions
FROM bib_meta.dw_rt_run_errors a, bib_meta.dw_rt_runs b 
WHERE a.dw_run_id = b.dw_run_id
--   and TRUNC (a.date_stamp) >  (sysdate- 30) and TRUNC (a.date_stamp) < (sysdate-30)
and TRUNC (a.date_stamp) >  (sysdate- 40) 

GROUP BY TRUNC (a.date_stamp), b.task       
) a  left join BIB_META.DW_TASKS b on   a.task = b.task
ORDER BY DATE_STAMP DESC
;


------------------------------------------------------SPACE -----------------------------------------------------------------------------------------


select to_char(to_date( DATE_KEY,'yyyymmdd'),'yyyymmdd') DATE_KEY,  
round((TOTAL_MB/1024/1024),2) "Storage Usage", 
round ((FREE_MB/1024/1024),2) "Free TB" 
,PERC_FREE 
from BIB.FCT_MON_ASM_SPACE  where NAME='DATAC1' and DATE_KEY between 20170822 and 20170927
order by DATE_KEY  ;


 


select count( distinct SUBSTR(PARTITION_NAME,-2)) SUBPART, 
count(distinct SUBSTR(PARTITION_NAME,3,8)) DATE_KEY,
SUBSTR (PARTITION_NAME, 3, 6) MONTH_ID,
 a.owner as schema1, segment_type,segment_name as table_name, 
        retention_value||' '||retention_period as retention_period,
        retention_value,
        a.TABLESPACE_NAME,
        round(sum(((a.BYTES)/1024/1024/1024)),2) GB        
       
from dba_segments a, DW_TABLE_METADATA b 
where segment_name = table_name(+)
--and segment_name = 'FRU_SUBS_FULL_SUMD_HL_1'
group by SUBSTR (PARTITION_NAME, 3, 6) ,a.TABLESPACE_NAME,retention_value,
a.owner, segment_type,segment_name, retention_value||' '||retention_period;



----------------TOP SPACE CONSUMPTION TABLES- ----------------------------------------------------------------------------------------

SELECT DATE_KEY, TABLE_NAME, TABLESPACE_NAME,  MONTH_ID,  SUMGB,  AVGGB FROM 
(

SELECT MONTH_ID || SUBPART DATE_KEY,  TABLE_NAME, TABLESPACE_NAME,  MONTH_ID, 
SUM(GB) OVER (PARTITION BY MONTH_ID || SUBPART,  TABLE_NAME, TABLESPACE_NAME, MONTH_ID  ORDER BY MONTH_ID DESC ) SUMGB,
ROUND(AVG(GB) OVER (PARTITION BY MONTH_ID ORDER BY MONTH_ID DESC ),2) AVGGB
FROM
(
select
--count( distinct SUBSTR(PARTITION_NAME,-2)) SUBPART, 
  SUBSTR(PARTITION_NAME,-2) SUBPART, 
count(distinct SUBSTR(PARTITION_NAME,3,8)) DATE_KEY,
SUBSTR (PARTITION_NAME, 3, 6) MONTH_ID,
 a.owner as schema1, segment_type,segment_name as table_name, 
        retention_value||' '||retention_period as retention_period,
        a.TABLESPACE_NAME,
        round(sum(((a.BYTES)/1024/1024/1024))) GB        
from dba_segments a, bib_meta.DW_TABLE_METADATA b 
where segment_name = table_name(+)
--and segment_name in ( 'SV_XDR' , 'PCI_HISTORY' , 'SRC_OD1_MSISDN_SNAP_DD' ,'I_POSTPAID_ADDRESS')
group by 
   SUBSTR(PARTITION_NAME,-2),
SUBSTR (PARTITION_NAME, 3, 6) ,
a.TABLESPACE_NAME,
a.owner, segment_type,segment_name, retention_value||' '||retention_period
)
--ORDER BY MONTH_ID || SUBPART DESC
)
WHERE SUMGB > 10
order by SUMGB desc 
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

(
select diy_parameter  date_key, max(start_date) startdate from dw_rt_runs where task = 'STD_TE_I2S_POSTPAID_SUBSCRIBER_BASE_PULL'
and diy_parameter  >  '20170922' and  diy_parameter   <  '20171023'
group  by diy_parameter  )a ,
(select diy_parameter  date_key , min(start_date)  enddate from dw_rt_runs where task = 'STD_C2E_FCT_SUBS_FULL_SUMD'
and diy_parameter  >  '20170922' and  diy_parameter   <  '20171023'
group  by diy_parameter  )b
where a.date_key = b.date_key
order by a.date_key asc
)
a order by date_key;
/


--------------d-1 durations HR  -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


select a. *, case  when dur  >= 1 then 'NOT MET' else 'MET' end status ,
case  when dur  >= 1 then 1  else 0  end  MRK
from 
(
select a.date_key,  a.startdate, b.enddate, 
numtodsinterval(enddate-startdate,'day') duration ,
(enddate-startdate) dur
from
(
select trunc(start_date)   date_key, max(start_date) startdate from dw_rt_runs where task = 'STD_TF_S2S_HRIS_PERSONAL'
and  trunc(start_date)   >  '20170822'  
--and  trunc(start_date)   <  '20170923'
group  by  trunc(start_date)   )a ,
(select trunc(end_date)  date_key , min(end_date)  enddate from dw_rt_runs where task = 'STD_TE_S2E_FCT_HR_EMPLOYEE_EVENT'
and  trunc(start_date)   >  '20170822'  
--and  trunc(start_date)   <  '20170923'
group  by trunc(end_date)  )b
where a.date_key = b.date_key
order by a.date_key asc
)
a order by date_key;
/

-----------------------------------d-1 durations CC--------------------------------------------------------------------------------------------------------------------------------------------------------------------



select a. *, case  when dur  >= 1 then 'NOT MET' else 'MET' end status ,
case  when dur  >= 1 then 1  else 0  end  MRK
from 
(
select a.date_key,  a.startdate, b.enddate, 
numtodsinterval(enddate-startdate,'day') duration ,
(enddate-startdate) dur
from
(
select trunc(start_date)   date_key, max(start_date) startdate from dw_rt_runs where task = 'STD_EPRO_RELATIONAL_S2S_EXTRACT_SETUP'
and  trunc(start_date)   >  '20170828'
group  by  trunc(start_date)   )a ,
(select trunc(end_date)  date_key , min(end_date)  enddate from dw_rt_runs where task = 'STD_TE_S2E_FCT_CC_CALL_EVENT'
and trunc(end_date) > '20170828'
group  by trunc(end_date)  )b
where a.date_key = b.date_key
order by a.date_key asc
)
a order by date_key;
/




select * from  I_CONTROL@BIB_INT.MTN.CO.ZA
where object_name = 'I_SD_DEALER_SALE';



select a.*,lag( HWM,1,0) over (order by HWM) as HWM_PREV from I_CONTROl@BIB_INT.MTN.CO.ZA a where date_key <= 99999999 and object_name ='I_SD_DEALER_SALE';
