DECLARE

V_DW_PARTS NUMBER;
V_JOB_NAME VARCHAR2 (200);
V_MSISDN_NSK NUMBER;
V_ID VARCHAR2 (20);



BEGIN


FOR X  IN  (SELECT  SUB_PART, MSISDN_NSK, IDS  FROM JST_BRT_MSISDN_ALL2
                  WHERE 1=1
                  AND SUB_PART  BETWEEN 1 AND 10
--                    AND SUB_PART = 0
                 
                  )

    LOOP 
    
    V_DW_PARTS  :=X.SUB_PART ;
    V_MSISDN_NSK := X.MSISDN_NSK;
    V_ID := X.IDS;
    V_JOB_NAME :=  'BIB_CTL.BRTFXX'|| V_ID || '_' || V_DW_PARTS;
    


    DBMS_SCHEDULER.CREATE_JOB (
            job_name =>V_JOB_NAME,
            job_type => 'PLSQL_BLOCK',
            job_action => 'BEGIN

   FOR i IN  '||V_DW_PARTS||'..'||V_DW_PARTS||'

   LOOP
      FOR r
         IN (SELECT *
               FROM (SELECT  /* +parallel 10*/ MSISDN_NSK,
                              BRT_SUBSCRIBER_KEY,
                              record_start_dt,
                              record_end_dt,
                              LAG (record_end_dt)OVER (PARTITION BY MSISDN_NSK ORDER BY BRT_SUBSCRIBER_KEY) PREV_END_DT,
                              LEAD(record_start_dt) OVER (PARTITION BY MSISDN_NSK ORDER BY record_start_dt ) NEXT_START_DT,
                                 LEAD(record_start_dt, 2) OVER (PARTITION BY MSISDN_NSK ORDER BY record_start_dt ) NEXT_NEXT_START_DT,
                              ROW_NUMBER () OVER (PARTITION BY MSISDN_NSK ORDER BY BRT_SUBSCRIBER_KEY) ROWN
                         FROM BIB.BRT_SUBSCRIBER  PARTITION FOR ('||V_DW_PARTS ||')
                         WHERE MSISDN_NSK = '||V_MSISDN_NSK||'
                        
                     ORDER BY MSISDN_NSK, BRT_SUBSCRIBER_KEY)
              WHERE 1=1
             
                    )
      LOOP
         UPDATE BIB.BRT_SUBSCRIBER  PARTITION FOR ('||V_DW_PARTS ||')
        SET record_end_dt  =  r.NEXT_START_DT
          WHERE brt_subscriber_key = r.brt_subscriber_key
            AND  MSISDN_NSK = '||V_MSISDN_NSK||'
            and RECORD_END_DT  >=   r.NEXT_NEXT_START_DT
            AND  r.NEXT_START_DT IS NOT NULL
            
         
          ;

         COMMIT;

  

      END LOOP;
      
   END LOOP;
   
END ;',

            number_of_arguments => 0,
            start_date => NULL,
            repeat_interval => NULL,
            end_date => NULL,
            enabled => TRUE,
            auto_drop => FALSE,
            comments => '');

         
     
 
    DBMS_SCHEDULER.SET_ATTRIBUTE( 
             name => V_JOB_NAME, 
             attribute => 'store_output', value => TRUE);
    DBMS_SCHEDULER.SET_ATTRIBUTE( 
             name =>V_JOB_NAME, 
             attribute => 'logging_level', value => DBMS_SCHEDULER.LOGGING_OFF);
      
   
  END LOOP;
    
END;
