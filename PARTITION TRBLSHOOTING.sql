----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------CHECK TARGET TABLE---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select  TASK, TARGET_TABLE_NAME, A.* 
from dw_task_exec_steps A
where 1=1
and TASK  LIKE  '%SDP_CDR%'
--AND TARGET_TABLE_NAME LIKE '%COMP%'
;


select  *  from STG_CDR.CS6_CCN_RAW_CDR
--PARTITION for  (20171027)
WHERE MSISDN_NSK  = 27787266370
--and to_date(create_dt , 'yyyymmdd hh24:mi:ss') <  to_date('20171025  00:00:00' , 'yyyymmdd hh24:mi:ss') 
;



select * from dw_rt_runs where dw_batch_id = 7130011
and task = 'CS6_TE_S2C_FCT_CDR_PREPAID_RATED_MA';


ALTER TABLE BIB_CDR.FCT_CDR_TRANS_COMB SPLIT  PARTITION P_20170801 
INTO
(
PARTITION P_20170720 values less than (20170721),
PARTITION P_20170721 values less than (20170722),
PARTITION P_20170722 values less than (20170723),
PARTITION P_20170723 values less than (20170724),
PARTITION P_20170724 values less than (20170725),
PARTITION P_20170725 values less than (20170726),
PARTITION P_20170726 values less than (20170727),
PARTITION P_20170727 values less than (20170728),
PARTITION P_20170728 values less than (20170729),
PARTITION P_20170729 values less than (20170730),
PARTITION P_20170730 values less than (20170731),
PARTITION P_20170731 
) 
 PARALLEL ( DEGREE 15 );
;







select * from BIB_CDR.FCT_CDR_UNRATED PARTITION FOR (20170701)
--WHERE DATE_KEY <> '20170801'
;


select /*+ parallel 5 */ 
--* 
SUBSTR(CELL_ID, 0, LENGTH(CELL_ID) - 1),  BASE_STATION_ID
from stg_cdr.MAPS_INV_2G PARTITION FOR (20171020)
--WHERE BASE_STATION_ID  NOT LIKE '
;



select 'alter table '||OWNER||'.'||TABLE_NAME ||' add partition '||PARTITION_NM||
' values less than ('||less_than||') TABLESPACE '||table_space ||'; COMMIT;' stmt
from (
select level LVL,
'P_'||to_char(to_date('20171008','yyyymmdd')+level,'yyyymmdd') PARTITION_NM,
to_char(to_date('20171009','yyyymmdd') +level,'yyyymmdd') less_than,
'TS_BIB'   table_space
from dual connect by level<=32) a,(select OWNER, TABLE_NAME from dba_tables where owner ='BIB' AND TABLE_NAME NOT LIKE '%DDL%'
AND TABLE_NAME LIKE 'FCT_RGE_MOD_SNAPD%') B
ORDER BY TABLE_NAME , LVL
;



select 'ALTER TABLE STG_GEN.SCOM_PERFORMANCEDATAALLVIEW  ADD partition P_' || dw_batch_id ||' values less than (' ||  (dw_batch_id  + 1)||')  ;'
from dw_rt_runs where task = 'STD_TE_S2S_SCOM_PERFORMANCEDATAALLVIEW' and run_status = 'FAILED';



ALTER TABLE BIB.FCT_RGE_MOD_SNAPD SET INTERVAL();
ALTER TABLE BIB.FCT_RGE_MOD_SNAPD  ADD partition P_20170101 values less than (6107362)  ;
ALTER TABLE BIB.FCT_RGE_MOD_SNAPD  SET INTERVAL(1);




/
SELECT Pattern_Task FROM DW_TASKS WHERE TASK = 'CS5_TE_S2C_FCT_CDR_ADJUSTMENT_IBF_MA';
/





SELECT * FROM BIB_META.DW_TEMPLATES
WHERE 1=1
--AND VARIABLE_NAME LIKE  'SOURCE_SELECT_SQL' 
--AND TEMPLATE_NAME = 'SOURCE_SELECT_SQL'
;

--SELECT * FROM DW_TASKS WHERE
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------CHECK INTERVAL PARTITION-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select  interval, owner, a.*
from DBA_PART_TABLES A
--where  OWNER ='BIB_CDR'
where table_name  like 'PED_RW_20170201_XSP06';
/



select count(*)  from stg_cdr.PED_RW_20170201_XSP06;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------CHECK PARTITION EXISTS----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
begin
DW_BIB_MAINT_UTL.FIX_PARTITION_NAMES('STG_GEN', 'INT_SUBSCRIBER');
dw_bib_utl.RELEASE_SESSION_USER_LOCKS; 
end;
/


select * from BIB_CDR.FCT_CDR_ADJUSTMENT partition for (20170731)
WHERE CREATE_DT  <   to_date ('20171025 12:25:23' , 'YYYYMMDD HH24:MI:SS') 
;


ALTER TABLE BIB_CDR.FCT_CDR_ADJUSTMENT TRUNCATE PARTITION (P_20170731);


SELECT *
--partition_name, interval, high_value,table_name,table_owner
FROM   dba_tab_partitions
where    1=1  
--and table_owner = 'BIB'
AND TABLE_NAME LIKE '%TRANS_COMB%'
AND PARTITION_NAME = 'P_20170831'
--AND TABLESPACE_NAME  = 'TS_BIB_CDR_M_3M_201707'
;


ALTER TABLE BIB_CDR.FCT_CDR_PREPAID_RATED ADD PARTITION P_20170731 VALUES LESS THAN (20170801);



select * from stg_cdr.EMM_BIB_AUDIT_SUMMARY partition for (20171007)
wh;


begin
BIB_CTL.MK_FIX_PARTITION_NAMES('BIB_META', 'DW_RT_LOG_APP');
END;
/





ALTER TABLE STG_CDR.PED_RW_20170201_XSP06  MODIFY DEFAULT ATTRIBUTES TABLESPACE  TS_STG_CDR_M_6M_201703;


select /*+ parallel(10)*/ 
a.*,
 'ALTER INDEX ' || INDEX_OWNER || '.' ||INDEX_NAME || ' REBUILD SUBPARTITION ' || SUBPARTITION_NAME || '  tablespace (change me !!) parallel 16 ;'
--  'ALTER INDEX ' || INDEX_OWNER || '.' ||INDEX_NAME || ' modify default attributes   tablespace TS_BIB_CDR_M_3M_201607;'
-- STATUS , a.* 
from DBA_IND_SUBPARTITIONS a
where 1=1
--STATUS  in ('UNUSABLE')
-- AND  INDEX_OWNER in ('BIB_CDR')
and index_name  LIKE '%CS6_SDP_RAW_CDR%'
--and PARTITION_NAME    NOT like '%ind_tab_s%'
--AND TABLESPACE_NAME  LIKE 'TS_BIB_CDR_M_3M_201606'
ORDER BY PARTITION_NAME desc
;
/

select /*+ parallel(10)*/ A.*
-- 'alter index '||index_owner||'.'||index_name||' modify default attributes for partition  '|| PARTITION_NAME ||' tablespace TS_BIB_CDR_M_3M_201611;'
 
from dba_indexes A
where 1=1
and owner = 'BIB_META'
AND TABLE_NAME  LIKE '%LOG_APP%'
-- tablespace_name  like  'TS_BIB_CDR_M_3M_201610'
--and index_name like '%CS6'
--AND STATUS = 'UNUSABLE'
--AND PARTITION_NAME  NOT  LIKE '%201608%'
--and composite  = 'YES'
--ORDER BY PARTITION_NAME
;
/


/


SELECT * FROM STG_CDR.ER_XDR_SV ;

CREATE UNIQUE INDEX STG_CDR.BI_CS6_SDP_RAW_CDR LOCAL ON STG_CDR.CS6_SDP_RAW_CDR (SUBSCRIBER_NR, EVENT_DT, DW_SUBPART, CDR_ID) 
TABLESPACE TS_STG_CDR_M_6M_201701 PARALLEL 16;



select dbms_metadata.get_ddl('INDEX','UK_CS6_SDP_RAW_CDR','STG_CDR') from dual;
/

---------------WHO DID WHAT -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select * from bib_ctl.audit_ddl
where name = 'ER_SASN_CDR'
order by d desc;

--stg_cdr.audit_ddl


----------------------------------------------MANUAL ADD -------------------------------------------------------------------------------------------------------------------------------------------------------------




begin
BIB_CTL.DW_BIB_MAINT_UTL.PROVISION_CAL_PARTITIONS(
p_table_like => '%I_POSTPAID_SUBSCRIBER_BASE%',
p_schema => 'STG_GEN',
p_days_ahead => 
nvl(BIB_CTL.dw_bib_etl_utl.dw_clean_to_number(
BIB_CTL.dw_umd_utl.get_ovr_NVL('PROVISION_CAL_DAYS_AHEAD')),31)
,
p_timeout_minutes => 
nvl(BIB_CTL.dw_bib_etl_utl.dw_clean_to_number(
BIB_CTL.dw_umd_utl.get_ovr_NVL('PROVISION_CAL_TIMEOUT')),10)
,

p_limit => 
nvl(BIB_CTL.dw_bib_etl_utl.dw_clean_to_number(
BIB_CTL.dw_umd_utl.get_ovr_NVL('PROVISION_CAL_LIMIT')),1000)

);
end;
/


SELECT a.*
--'alter table '||table_owner||'.'||a.table_name||' move subpartition '||subpartition_name||' tablespace TS_'||TABLE_OWNER||'_M_6M_'||substr(partition_name,3,6)||' parallel 16;'
--'alter table '||table_owner||'.'||a.table_name||' move partition '||partition_name||' tablespace TS_'||TABLE_OWNER||'_M_6M_'||substr(partition_name,3,6)||' parallel 16;'
--'alter table '||table_owner||'.'||a.table_name||' move partition '||partition_name||' tablespace  TS_STG_GEN_M_3M_201709  parallel 16;'
, 'alter table '||table_owner || '.'||a.table_name||'  modify default attributes for partition ' ||partition_name||'  tablespace TS_'||table_owner||'_M_6M_'||substr(partition_name,3,6)||';'
FROM   all_tab_partitions a
WHERE  TABLESPACE_NAME  like  'TS_STG_GEN'
--and PARTITION_NAME like 'SUBS_POSTPAID_SOURCE%'
and TABLE_NAME  like 'I_FIN_FINANCIAL_PERIOD%'
--and SUBPARTITION_NAME like 'P_20160131%'
order by partition_name asc
;


select * from STG_GEN.I_SORB_DISCONNECTION PARTITION FOR (9059357);

alter table STG_GEN.I_SORB_DISCONNECTION drop partition P_9059357;

-----------------CHECK PARTITION CONFIG META ------------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM  BIB_META.DW_TABLE_METADATA 
where table_name like 'FUND_ACCOUNT_PROFILE'
--AND TABLESPACE_VARIABLE  LIKE 'TS_STG_GEN'
;
/

select * from STG_GEN.I_NETWORK_ELEMENT_STATS;

UPDATE  BIB_META.DW_TABLE_METADATA 
SET TABLESPACE_VARIABLE = 'TS_STG_GEN_M_3M_<YYYYMM>'
where table_name like 'SCOM%'
AND TABLESPACE_VARIABLE  LIKE 'TS_STG_GEN'
AND RETENTION_VALUE = 3 ;

COMMIT;



SELECT * FROM STG_GEN.I_FIN_FINANCIAL_PERIOD;

begin
DW_BIB_MAINT_UTL.FIX_PARTITION_NAMES('STG_GEN', 'ER_I_CDR_PREPAID_DED_SNAPD');
dw_bib_utl.RELEASE_SESSION_USER_LOCKS; 
end;
/


SELECT * FROM  BIB_META.DW_TABLE_METADATA 
WHERE 1 =1 
--AND PARTITION_FREQUENCY=null
AND  table_name like 'FCT_CDR_TAP_IN%';
/



select status, a. * from dba_tablespaces a
where 
tablespace_name LIKE '%201611'
--and status ='READ_ONLY'
; 
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
select /*+ parallel(10)*/ *
from DBA_PART_TABLES
where owner ='STG_GEN'
--and table_name ='ABLT_GSM_SERVICE_MAST'
;


---DROP PARTITION

SELECT 'ALTER TABLE STG_GEN.ABLT_GSM_SERVICE_MAST DROP partition' || '  ' ||PARTITION_NAME || ';' FROM    dba_tab_partitions
WHERE  table_name like '%ABLT_GSM_SERVICE_MAST'
AND    table_owner = 'STG_GEN'
AND  PARTITION_NAME IN
(
'P_5074855',
'P_5055867',
'P_4941899',
'P_5035866'

)
--AND RUN_STATUS = 'HOLD_MK'
;



 ---------WHICH COLUMNS ARE IN THE INDEX----------------------------------------------------------------------------------------------------------------------------------------------------------------------

select 
b.uniqueness, a.index_name, a.table_name, a.column_name 
from all_ind_columns a, all_indexes b
where a.index_name=b.index_name 
and a.table_name = upper('FCT_IMSI_USAGE_FIRST')
order by a.table_name, a.index_name, a.column_position;




 
 ---------WHICH COLUMNS ARE IN THE INDEX----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
select /*+ parallel(4)*/  *
owner, table_name, index_name, column_name
 FROM dba_ind_columns Where table_owner= 'STG_CDR'
and table_name  = 'CS5_CCN_VOICE_DA'
AND table_NAME='FCT_CDR_RECHARGE'
order by INDEX_NAME, COLUMN_POSITION
;


 
 
 
-----who  is blocking , locking -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select
(select username || ' - ' || osuser from v$session where sid=a.sid) blocker,
a.sid || ', ' ||
(select serial# from v$session where sid=a.sid) sid_serial,
' is blocking ',
(select username || ' - ' || osuser from v$session where sid=b.sid) blockee,
b.sid || ', ' ||
(select serial# from v$session where sid=b.sid) sid_serial
from v$lock a, v$lock b
where a.block = 1
and b.request > 0
and a.id1 = b.id1
and a.id2 = b.id2;




SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
FROM all_constraints cons, all_cons_columns cols
WHERE cols.table_name LIKE '%BRT_BILLING_ACCOUNT%'
AND cons.constraint_type = 'P'
AND cons.constraint_name = cols.constraint_name
AND cons.owner = cols.owner
ORDER BY cols.table_name, cols.position;





-----------duplicates dim ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select * from (
select
a.*,
-- address_key,address_cd,address_type_cd,
row_number () over (partition by ADDRESS_KEY, address_cd, address_type_cd order by address_key desc) rown,
count(1) over (partition by ADDRESS_KEY ,address_cd,address_type_cd ) cnt
from 
bib.dim_address a
)
where rown= 1 and current_record_ind='N';


-----------------------------------------------tablespace------------------------

select tablespace_name
from all_tables
where table_name like '%FCT_CDR_UNRATED%';

select * from dba_tablespaces;

select * from ALL_TAB_PARTITIONS where table_name like 'FCT_CDR_UNRATED' and partItion_name like 'P_20160316';


----------------END DATE GREATER THAN -------------------------------------------

select 
COUNT(*)
--TO_CHAR(RECORD_START_DT, 'YYYYMMDD HH24')
--,A.* 
from BIB.BRT_SUBSCRIBER partition (P6) A
WHERE 1=1
--MSISDN_NSK = 233548327862
AND CURRENT_RECORD_IND ='Y'
AND TO_CHAR(RECORD_START_DT, 'YYYYMMDD HH24') >= '20160712 15'

ORDER BY RECORD_START_DT DESC;





SELECT * FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME LIKE 'BRT_SUBSCRIBER'
AND OWNER LIKE 'BIB'
AND COLUMN_NAME LIKE 'RECORD_START_DT'
;

---------------------------------------------DROP PARITIONS LOG APP-----------------------------------------------------------------------------------------------------------------------------------------------

select A.*  , 'alter table ' || table_owner || '.' || table_name || ' drop partition ' || partition_name || ' update indexes ; ' 
from dba_tab_partitions A
where table_owner = 'STG_GEN' 
and table_name LIKE  '%EPROM_AGENTNOTREADYDETAIL%' 
--and partition_position <> 1
--and bib_supp.getpartname(table_owner, table_name, partition_name) <
--TO_DATE(' 2016-08-25 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')

;


begin
MK_FIX_PARTITION_NAMES('BIB_META', 'DW_RT_DO_BATCH_LOG'); 
--MK_FIX_PARTITION_NAMES('BIB_META', 'DW_RT_LOG_APP');
dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
end;
/

begin 
BIB_META.MK_FIX_PARTITION_NAMES('BIB_META', 'DW_RT_CLOB_LOG'); 
BIB_META.MK_FIX_PARTITION_NAMES('BIB_META', 'DW_RT_LOG_APP'); 
dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
end;
/


DESCRIBE BIB.FCT_BILLING_PAYMNT_ALLOCATION;

STG_GEN.I_POSTPAID_SUBS_BASE_OTHR;




CREATE UNIQUE INDEX STG_CDR.UK_CS6_SDP_RAW_CDR_BI  ON STG_CDR.CS6_SDP_RAW_CDR (SUBSCRIBER_NR, EVENT_DT, CDR_ID,  DW_SUBPART, DATE_KEY ) LOCAL
TABLESPACE TS_STG_CDR_M_6M_201706 PARALLEL 16;


 delete from STG_CDR.CS6_SDP_RAW_CDR
 
 where rowid in
 (
 select rowid
from
 (
 select  /*+parallel(30)*/ 
 rowid,
 row_number() over ( partition by  SUBSCRIBER_NR, EVENT_DT, CDR_ID,  DW_SUBPART, DATE_KEY  order by DATE_KEY desc  ) row_number
 from  STG_CDR.CS6_SDP_RAW_CDR  partition for (20170519)  b
 ) a
 where row_number > 1
 )
 ;
 
 
 
 
 
 ---------------MATCH THE BATCH----------------------------
 
 
 SELECT  
'alter table '||table_owner||'.'||a.table_name||' move partition '||partition_name||' tablespace TS_'||TABLE_OWNER||'_M_3M_'||substr(DW_DATE_KEY ,1,6)||' parallel 16;'
FROM 
(
SELECT 
SUBSTR(A.PARTITION_NAME, 3) BATCH_ID,
A.* 
--'alter table '||table_owner||'.'||a.table_name||' move subpartition '||subpartition_name||' tablespace TS_'||TABLE_OWNER||'_M_6M_'||substr(partition_name,3,6)||' parallel 16;'
--,'alter table '||table_owner||'.'||a.table_name||' move partition '||partition_name||' tablespace TS_'||TABLE_OWNER||'_M_6M_'||substr(partition_name,3,6)||' parallel 16;'
--'alter table '||table_owner||'.'||a.table_name||' move partition '||partition_name||' tablespace  TS_STG_GEN_M_3M_201709  parallel 16;'
--, 'alter table '||table_owner || '.'||a.table_name||'  modify default attributes for partition ' ||partition_name||'  tablespace TS_'||table_owner||'_M_6M_'||substr(partition_name,3,6)||';'
FROM   all_tab_partitions a
WHERE  TABLESPACE_NAME  like  'TS_STG_GEN'
--and PARTITION_NAME like 'SUBS_POSTPAID_SOURCE%'
and TABLE_NAME  like 'I_FIN_FINANCIAL_PERIOD%'
--and SUBPARTITION_NAME like 'P_20160131%'
) A,  DW_RT_BATCHES B
WHERE  A.BATCH_ID  = B.DW_BATCH_ID

;



select *  from dw_rt_log_app a
where error_sysdate >= sysdate-0.1
--where a.ERROR_SYSDATE >= TO_DATE('20141030 223751', 'yyyymmdd hh24miss')  --  31-OCT-2014 01:52:37
and ( process_name like 'DW_PARTITION%'
       or process_name = 'DW_IDENTIFY_BATCH_PARTITIONS'
       or process_name = 'DW_IDENTIFY_DATE_PARTITIONS'
       or process_name = 'DW_IDENTIFY_EMPTY_PARTITIONS'
       or process_name = 'DW_EXPORT_PARTITIONS'
       or process_name = 'DW_DROP_PARTITIONS'
      or process_name = 'DW_CLEAR_PARTITIONS_WIP'
      or ( (a.process_name = 'DW_REMOTE_JOB' or a.process_name like 'DW_PARTTN%')  and a.message like '%PARTTN%')
)
--and dw_log_id > 5068259062                                                         
order by error_sysdate desc,dw_log_id desc;
 
 ---------check if main job ran
 
 
select *  from dw_rt_log_app
where error_sysdate >= sysdate-3
and ( process_name like '%MAINTENANCE%'
      or process_name = 'DW_DROP_PARTITIONS');
      
      
      
 select to_char(a.error_sysdate,'YYYYMMDD HH24:MI:SS') log_date_time, b.task, process_name,
 message, error_code, id_1 env, id_2 sid, dw_batch_id, dw_log_id from dw_rt_log_app a
 left outer join dw_tasks b on (a.dw_task_id = b.dw_task_id) WHERE 1=1 and error_sysdate >= sysdate -1 
   and process_name like '%SET_STORE_IN_FOR_B%' 
 and error_code = 'E'order by dw_log_id DESC
 ;
  
  
  
  
  select to_char(a.error_sysdate,'YYYYMMDD HH24:MI:SS') log_date_time, b.task, process_name, message, error_code, id_1 env,
  id_2 sid, dw_batch_id, dw_log_id from dw_rt_log_app a left outer join dw_tasks b on (a.dw_task_id = b.dw_task_id) 
  WHERE 1=1 and error_sysdate >= sysdate -1 and process_name like '%MOVE_CAL_PARTITIONS%' order by dw_log_id DESC;

