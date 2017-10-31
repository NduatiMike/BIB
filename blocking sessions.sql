select 
   (select username from v$session where sid=a.sid) blocker,
   a.sid,
   ' is blocking ',
   (select username from v$session where sid=b.sid) blockee,
   b.sid
from 
   v$lock a, 
   v$lock b
where 
   a.block = 1
and 
   b.request > 0
and 
   a.id1 = b.id1
and 
   a.id2 = b.id2; 
   
   
 ALTER SYSTEM KILL SESSION '426,63324, @3';
   
   select
   c.owner,
   c.object_name,
   c.object_type,
   b.sid,
   b.serial#,
   b.status,
   b.osuser,
   b.machine
from
   v$locked_object a ,
   v$session b,
   dba_objects c
where
   b.sid = a.session_id
and
   a.object_id = c.object_id;
   
   
   
   
  
  SELECT username U_NAME, owner OBJ_OWNER,
object_name, object_type, s.osuser,
DECODE(l.block,
  0, 'Not Blocking',
  1, 'Blocking',
  2, 'Global') STATUS,
  DECODE(v.locked_mode,
    0, 'None',
    1, 'Null',
    2, 'Row-S (SS)',
    3, 'Row-X (SX)',
    4, 'Share',
    5, 'S/Row-X (SSX)',
    6, 'Exclusive', TO_CHAR(lmode)
  ) MODE_HELD
FROM v$locked_object v, dba_objects d,
gv$lock l, gv$session s
WHERE v.object_id = d.object_id
AND (v.object_id = l.id1)
AND v.session_id = s.sid
AND OWNER <>  'SYS'
ORDER BY username, session_id;


select SESSION_ID || ','  , A.*from DBA_DDL_LOCKS A
where 1=1
AND name like '%I_CDR_PREPAID_SNAPD%'  
order by SESSION_ID;



select 
u.inst_id, schemaname, u.module, u.client_info, u.logon_time,  u.sid, serial#, machine, status, s.sql_text, substr(event,1,509) event ,seconds_in_wait, u.blocking_session, 
u.final_blocking_session,
'alter system kill session  ''' ||to_char(sid)||','||to_char("SERIAL#")||',@'||u.inst_id||'''  immediate;' kill_session
from gv$session u 
left outer join gv$sqltext s 
on (s.hash_value = u.sql_hash_value and s.inst_id = u.inst_id)
where 1=1
and nvl(s.piece,0)=0
--AND SCHEMANAME LIKE '%BIB_CTL%'
--and u.final_blocking_session is not null
--and u.module like 'TE_S2E_FCT_CDR_EVD_ERS'
--and upper(machine) like '%OBIEE_BIB_RW%'
--and u.sid in ( 
--
--) 
--and serial# like '21821'
--and upper(schemaname) like '%OBIEE_BIB_RW%'
order by  u.logon_time, u.seconds_in_wait desc, u.sid, s.piece asc;


                      
begin
DW_BIB_UTL.DROP_CTL_JOB('FW_EXEC_RUN_BATCH_NA_PRIO_6');
  dw_bib_utl.RELEASE_SESSION_USER_LOCKS;
end;
/

select * from DBA_SCHEDULER_JOBS
where owner = 'BIB_CTL'
AND CLIENT_ID LIKE '%MICH%';



DBA_OBJECTS ------------------------------------ All objects in the database
DBA_SCHEDULER_PROGRAMS---------------All scheduler programs in the database
DBA_SCHEDULER_JOBS----------------------- All scheduler jobs in the database
DBA_SCHEDULER_JOB_CLASSES---------- All scheduler classes in the database
DBA_SCHEDULER_WINDOWS-------------- All scheduler windows in the database
DBA_SCHEDULER_PROGRAM_ARGS----- All arguments of all scheduler programs in the database
DBA_SCHEDULER_JOB_ARGS--------------- All arguments with set values of all scheduler jobs in the
database

DBA_SCHEDULER_JOB_LOG------------------Logged information for all scheduler jobs
DBA_SCHEDULER_JOB_RUN_DETAILS--- The details of a job run
DBA_SCHEDULER_WINDOW_LOG---------- Logged information for all scheduler windows
DBA_SCHEDULER_WINDOW_DETAILS---- The details of a window
DBA_SCHEDULER_WINDOW_GROUPS----- All scheduler window groups in the database




select * from
   v$session
   where sid in 
   (861,
376,
1569,
1193
)
;



select dw_utl_remote.USER_LOCK_ACQUIRE_DEADL_RETRY ((12215*1000)+100) from dual;

dw_utl_remote.user_lock_release ((12215*1000)+100);