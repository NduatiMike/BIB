
--SV
--ls > /data01/infa/PROD/scripts/SDP_MV_MK.csv
--SV work to move mike_clean_work_o





drop table SDP_MV_MK;

CREATE TABLE SDP_MV_MK  (short_filename VARCHAR (1000 BYTE))
ORGANIZATION EXTERNAL
   (TYPE ORACLE_LOADER
         DEFAULT DIRECTORY "DW_SCRIPT_DIR_SAPROD" ACCESS PARAMETERS ( FIELDS TERMINATED BY ',')
         LOCATION ('SDP_MV_MK.csv'));
         
         drop table MK_XDR purge;
         /  
         
------------------ ------------------import the table -------------------------------------------------------------------------------------------

  create table SDP_MV_MK_O  as 
  select  *  from SDP_MV_MK;
  

SELECT SUBSTR(a.SHORT_FILENAME,-19,8) , count(*) FROM  MK_SDP_MV23 a
group by  SUBSTR(a.SHORT_FILENAME,-19,8) 
order by SUBSTR(a.SHORT_FILENAME,-19,8) desc;
         
             
    
 ------------------ ------------------Add columns to the table -------------------------------------------------------------------------------------------
    
    ALTER TABLE SDP_MV_MK_O  ADD (min_processing_status VARCHAR (1000 BYTE), max_processing_status VARCHAR (1000 BYTE), file_count number);
/ 


------------------ ------------------merge to get status per file  -------------------------------------------------------------------------------------------


MERGE INTO SDP_MV_MK_O a
     USING (  SELECT /*+ parallel 30*/ 
                     short_filename,
                     MIN (processing_status) min_processing_status,
                     MAX (processing_status) max_processing_status,
                     COUNT (*) file_count
                FROM dw_rt_files
               WHERE     dw_task_id = dw_get_task_id ('TF_S2S_CS6_SDP_CDR')
                 AND short_filename IN (SELECT short_filename FROM SDP_MV_MK_O)
            GROUP BY short_filename) b
        ON ((a.short_filename) = (b.short_filename))
WHEN MATCHED
THEN
   UPDATE SET
      a.min_processing_status = b.min_processing_status,
      a.max_processing_status = b.max_processing_status,
      a.file_count = b.file_count;
      
COMMIT;
/

-------------------  ------------------------------------------------------------------------------
         

 
 
 
 select *  from dw_rt_files where 1=1
--and  dw_batch_id  in 
--(
--5175980
--)
--and dw_file_key = 310406230
--AND DW_TASK_ID = 12471
--AND PROCESSING_STATUS = 'FAILED'
--AND TRUNC(REGISTER_DATETIME) = TO_DATE ('20170303' ,  'YYYYMMDD') ;
and short_filename like '%TAPIN_24-17-0201:49:51_9108'
--and short_filename in (select * from mike_ibf_o)
--and processing status like '%ARCH%'
;

 drop table MK_SDP_MV23 purge;
 
 select  /* +PARALLEL 30 */ 
-- row_number () over (partition by short_filename order by short_filename desc ) counts,
 'mv -f  /data01/infa/PROD/TF_S2S_CS6_SDP_CDR/work/'||a.short_filename||'  /data01/infa/PROD/TF_S2S_CS6_SDP_CDR/archive/  '  
from SDP_MV_MK_O a join  dw_rt_files b   
 on  a.short_filename =  b.short_filename
 left join dw_rt_batches c
on   b.dw_batch_id  = c.dw_batch_id
where 1=1
AND  a.MAX_processing_statuS   like  '%ARCHIVING%'
and SUBSTR(a.SHORT_FILENAME,-19,8)  =  '20171028' 
--and  SUBSTR(a.SHORT_FILENAME,-19,8)  = '20171023'
and c.load_status =  'AUDITED'
;


select * from MK_SDP_MV23 A
where 1=1
--AND Max_Processing_Status <> 'LOADED'
--and SUBSTR(a.SHORT_FILENAME,-19,8)  =  '20171019' 
and  SUBSTR(a.SHORT_FILENAME,-19,8)  = '20171023'
;
 
 
 

 
-------------------get the files ------------------------------------------------------------------------------

select distinct  'rm  -f '||  SHORT_FILENAME    from mike_voicetr_work_o  where min_processing_status  is null
--  and max_processing_status = 'ARCHIVING'
 ;
 
 
 select  'rm  -f '||  SHORT_FILENAME,   a.*  from dw_rt_files a
where  1=1
--and dw_batch_id = 5148631 
and dw_task_id = 12471
and processing_status = 'FAILED'
and short_filename  in 
 (
   select  SHORT_FILENAME    from mike_ggsn_o  
--   where min_processing_status = 'FAILED'
--  and max_processing_status = 'FAILED'
  ) 
--  and processing_status NOT IN   ( 'ARCHIVED') 
--  ('LOADED','ARCHFAILED','ARCHIVED','ARCHIVING' , 'LOADING')
 ;
 
 
-- SELECT PROCESSING_STATUS FROM DW_RT_FILES
 
 update dw_rt_files set processing_status = 'LOADED'
 WHERE DW_FILE_KEY IN
 (
 select keys from
 (
 select dw_file_key  keys, SHORT_FILENAME from dw_rt_files where 
 short_filename in
(
  select DISTINCT SHORT_FILENAME from mike_voicetr_work_o  where min_processing_status = 'ARCHIVING' 
  and max_processing_status =  'ARCHIVING'
)
--and processing_status not in ('LOADED','ARCHFAILED','ARCHIVED','ARCHIVING' , 'LOADING')
and processing_status IN   ('ARCHIVING' )
--group by SHORT_FILENAME
)
)
 ;
 
 
 
 
 
-- select dw_file_key  keys,  'rm  -f '||  SHORT_FILENAME  from dw_rt_files where 
--update dw_rt_files set processing_status = 'LOADED'
--where dw_file_key in
--(
 select  *    from dw_rt_files where 
 short_filename in
(
  select  SHORT_FILENAME from TTFILE06-8672_20170323130140.dat  where min_processing_status = 'ARCHIVING' 
  and max_processing_status =  'ARCHIVING'
)
--and processing_status not in ('LOADED','ARCHFAILED','ARCHIVED','ARCHIVING' , 'LOADING')
and processing_status IN   ('ARCHIVING' )
--and ARCHIVE_START_DATETIME <  sysdate - 1

--and dw_task_id = 23104
--group by SHORT_FILENAME
--)
;   


