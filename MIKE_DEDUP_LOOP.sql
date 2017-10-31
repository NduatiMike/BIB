
declare
FCT_JOB varchar2(50);   -- unique name
v_job_name varchar2(50):='';
DATES VARCHAR2(32);
SUBPART VARCHAR2(32);


begin

    FOR COUNTER  IN 
--    
                         ( SELECT DT, SUB FROM  IBF_DUPS_MK
                           WHERE 1=1
--                          AND COUNTER  > 90
--                          AND COUNTER <=  125
                            AND DT = 20170326
                             AND SUB = 8
                          )
--                      
                      LOOP
              
                      DATES:= COUNTER.DT   ; 
                   
                      SUBPART :=  COUNTER.SUB;
                      
--                      FOR 
--                      
--                               I IN 0..9
--                      
--                                  LOOP
                      
                     FCT_JOB := 'MIKE_IBF'|| DATES ||SUBPART;
                     
                      v_job_name:='DEDUP'||substr(FCT_JOB,1,20);
                     
                     

 
                                            dbms_scheduler.create_job(job_name=>v_job_name,
                                                                      job_type=>'PLSQL_BLOCK',
                                                                      job_action=>
                                            '
                                            begin
                                            DW_BIB_DELETE_DUPS_SUBPART_CX (
                                               in_schema      => ''STG_CDR'',
                                               in_table       => ''IBF_XDR'',  --- Put the table name
                                               in_date_key    =>'||DATES||' ,  -- put date
                                               in_subpart     =>'||SUBPART ||',   -- Put the subpart 
                                               in_group_by    => ''A_MSISDN, SUBMIT_DATE_TIME, EDR_ID, UNIQUE_RECORD_ID,  WALLET , DW_SUBPART, DATE_KEY'' ,
                                               in_pex_pre       =>''PED_MIKEIBF'',
                                               in_parallel_select =>16,
                                               in_incl_excl =>''EXCLUDING '',
                                               in_compress => '' COMPRESS for OLTP '',
                                               in_CTAS_TS =>'' ''
                                                   , pi_extra_part =>'' partition by range (DW_SS) interval(1) (partition P0 values less than (1)) ''
                                                   , pi_extra_collist=>'' ,DW_SS ''
                                                   , pi_extra_vallist=>'' ,mod(nvl(DW_FILE_ID,0),100) DW_SS''  )
                                            ;
                                            end;
                                         ',
                                            
                                            enabled=>false,
                                            auto_drop=>true);
 
 
dbms_scheduler.set_attribute(v_job_name,'instance_id',3);   ----<<< Must be the same instance id for all
DBMS_SCHEDULER.ENABLE (v_job_name);


--    END LOOP;
    
    END LOOP;
    
end;
/













create unique index STG_CDR.UK_CS6_CCN_RAW_CDR_BI ON STG_CDR.CS6_CCN_RAW_CDR (CHARGE_START_TIME,MSISDN_NSK,CHRONO_NR,SOURCE_FILE_NAME,CHARGE_STOP_TIME,CCN_NODE_NM,UNIQUE_RECORD_ID,SERVICE_ID,DATE_KEY,DW_SUBPART) UNUSABLE LOCAL TABLESPACE TS_STG_CDR_M_6M_201707 PARALLEL 16;

ALTER INDEX STG_CDR.UK_CS6_CCN_RAW_CDR_BI  INVISIBLE;

ALTER TABLE STG_CDR.IBF_XDR EXCHANGE SUBPARTITION for (20170519,1) WITH TABLE STG_CDR.PED_IBFM_20170519_SP01  EXCLUDING INDEXES WITHOUT VALIDATION;

select 
* 
--'ALTER TABLE  STG_CDR.IBF_XDR EXCHANGE SUBPARTITION for   ' || SUBSTR(PROCESS_NAME ,36) ||  '   WITH TABLE  ' || SUBSTR(MESSAGE, 27) || '  EXCLUDING INDEXES WITHOUT VALIDATION;'
from dw_rt_log_app a 
where error_sysdate >= sysdate-1
and process_name like 'DELETE DUPS SUBPART:STG_CDR.IBF_XDR(20170326,8)' 
--and message like '%-14278 ORA-14278: column type%'
--and message like '%Create PEX table started.STG_CDR.PED_IBFM_%'
--order by message DESC
ORDER BY ERROR_SYSDATE DESC
;



SELECT SUBSTR(PROCESS_NAME, 4) DATE_KEY
--,  COUNT(MESSAGE) 
FROM dw_rt_log_app a 
WHERE PROCESS_NAME LIKE '%DELETE DUPS SUBPART:STG_CDR.CS6_CCN_RAW_CDR%'
AND MESSAGE LIKE '%dropped!%'
--GROUP BY SUBSTR(PROCESS_NAME, 0,44)
;


                  select   *  from 
                                                 
                                                    (
                                                    select   /*PARALLEL (20)*/  a .*,   row_number() over (partition by  MSISDN_NSK, WALLET_CD ,DATE_KEY,DW_SUBPART
                                                    ORDER BY  DATE_KEY DESC ) DUPS
                                                     FROM STG_GEN.I_CDR_PREPAID_DED_SNAPD  SUBPARTITION FOR (20170720, 0 ) a
                                                  
                                                     ) 
                                                     
                                                      where dups > 1;
                                                      
                                                      
                                                      
                                                      DESCRIBE DED_DUPS;
                                                      
                                                      
                                                      



select *  
 from   dba_tab_cols 
 where  table_name = 'PED_IBFM_20170322_SP03';

 
 
alter table STG_CDR.PED_IBFM_20170324_SP08
 MODIFY 
 (
);


--------set default so the length can change -------------

 alter table STG_CDR.PED_IBFM_20170324_SP08
 add
 (
);



 alter table STG_CDR.PED_IBFM_20170323_SP09 set usable COLUMN (
 );
 
 
  alter table STG_CDR.PED_IBFM_20170323_SP09
 MODIFY 
 (
);
 
 
alter table STG_CDR.PED_IBFM_20170323_SP09
 MODIFY 
 (

);
 
 

 
 ALTER TABLE STG_CDR.IBF_XDR EXCHANGE SUBPARTITION for (20170322,8) WITH TABLE STG_CDR.PED_IBFM_20170322_SP08  EXCLUDING INDEXES WITHOUT VALIDATION;
 
exec dbms_stats.drop_extended_stats('STG_CDR', 'IBF_XDR', '(DATE_KEY, DW_FILE_ID )');


----TO CREATE THESE EXTENDED STATS ----------

--SYS_OP_COMBINED_HASH("BATCH_ID","BATCH_ID")
--SYS_OP_COMBINED_HASH("REC_TYPE","A_MSISDN")
--SYS_OP_COMBINED_HASH("REASON_FOR_DEBIT_CREDIT","ADDITIONAL_DESCRIPTION")
--SYS_OP_COMBINED_HASH("EDR_VERSION","SOURCE_CHANNEL","REC_TYPE","REASON_FOR_DEBIT_CREDIT","COMMAND","ADDITIONAL_DESCRIPTION")
--SYS_OP_COMBINED_HASH("DATE_KEY","DW_FILE_ID")


SELECT  *  FROM  IBF_DUPS_MK
                          WHERE 1=1
--                          AND COUNTER  > 1
--                          AND COUNTER <=  30
                          AND DT = 20170326
                          AND SUB = 8
                          ;

 
select 
*
--'ALTER TABLE  STG_CDR.IBF_XDR EXCHANGE SUBPARTITION for   ' || SUBSTR(PROCESS_NAME ,36) ||  '   WITH TABLE  ' || SUBSTR(MESSAGE, 27) || '  EXCLUDING INDEXES WITHOUT VALIDATION;'
--'ALTER TABLE  STG_CDR.IBF_XDR EXCHANGE SUBPARTITION for   ' || SUBSTR(PROCESS_NAME ,36) ||  '   WITH TABLE  ' || SUBSTR(MESSAGE, 27) || '  EXCLUDING INDEXES WITHOUT VALIDATION;'
from dw_rt_log_app a 
where error_sysdate >= sysdate-1
--and process_name like 'DELETE DUPS SUBPART:STG_CDR.IBF_XDR(20170326,8%' 
--and message like '%-14278 ORA-14278: column type%'
and message like '%DELETE%'
--and error_sysdate < to_date ('20170810 14:28:00' ,  'YYYYMMDD HH24:MI:SS')
and error_sysdate > to_date ('20170810 14:45:00' ,  'YYYYMMDD HH24:MI:SS')
--and SUBSTR(PROCESS_NAME ,37, 8)  in
--(
--SELECT DT FROM  IBF_DUPS_MK
--                          WHERE COUNTER  > =1
--                          AND COUNTER <=  125
--)
--order by message DESC
ORDER BY ERROR_SYSDATE DESC
;


ALTER TABLE STG_CDR.IBF_XDR EXCHANGE SUBPARTITION for (20170326,8) WITH TABLE STG_CDR.PED_MIKEIBF20170326_SP08  EXCLUDING INDEXES WITHOUT VALIDATION;



SELECT * FROM STG_CDR.IBF_XDR SUBPARTITION FOR (20170326,8);


delete from STG_CDR.CS6_CCN_RAW_CDR
 where rowid in
 (
 select  rowid
from
 (
 select  /* +PARALLEL 20*/
 rowid,
 row_number() over ( partition by CHARGE_START_TIME,MSISDN_NSK,CHRONO_NR,SOURCE_FILE_NAME,CHARGE_STOP_TIME,CCN_NODE_NM,UNIQUE_RECORD_ID,SERVICE_ID,DATE_KEY,DW_SUBPART order by DATE_KEY   ) row_number
 from STG_CDR.CS6_CCN_RAW_CDR SUBPARTITION FOR  (20170326,8)  b
 ) a
 where row_number > 1
 )
 ;
 
 