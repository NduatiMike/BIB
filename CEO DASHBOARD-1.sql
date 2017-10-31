  WITH 
MAIN_SEL AS
(
   SELECT /*+PARALLEL(a,16) */ SUBS.DATE_KEY AS DATE_KEY
         ,SUBSTR(SUBS.DATE_KEY,1,4) AS YEAR                  
         ,TO_NUMBER(SUBSTR(SUBS.DATE_KEY,5,2)) AS MONTH_ID     
         ,CASE WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '01' THEN 'January'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '02' THEN 'February'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '03' THEN 'January'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '04' THEN 'April'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '05' THEN 'May'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '06' THEN 'June'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '07' THEN 'July'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '08' THEN 'August'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '09' THEN 'September'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '10' THEN 'October'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '11' THEN 'November'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '12' THEN 'December'
          ELSE 'UNKNOWN' END AS MONTH_NAME
        ,SUBSTR(SUBS.DATE_KEY,7,2) AS DAYNUM
-----------------------------------------------------------
--SUBSCRIBER
,SUM(NVL(OPENING_CNT,0))       OPENING_BASE
,sum(NVL(new_cnt,0))         NEW_ADDITIONS 
,SUM(NVL(returner_cnt,0))    RETURNEES
,sum(NVL(new_cnt,0)) + SUM(NVL(returner_cnt,0))   AS           GROSS_ADDITIONS          
,SUM(NVL(churner_cnt,0)) AS CHURN
,SUM(NVL(CLOSING_CNT,0))                                    TOTAL_SUBSCRIBERS
,SUM((NVL(CLOSING_CNT,0) + NVL(OPENING_CNT,0))/2) AS AVERAGE_SUBSCRIBERS
,SUM(NVL(CLOSING_CNT,0) - NVL(OPENING_CNT,0)) as Net_Additions  

,SUM(NVL(RGS30_CNT,0))        RGS30_COUNT       
--,SUM(MIGRATION_CNT)    MIGRATION_COUNT 

-- NUMBER FOR LIFE
,SUM(NVL(N4L_CNT,0))              AS MIGRATION_COUNT      
,SUM(NVL(N4L_HYBRID_IN_CNT,0))    AS MIGRATION_HYBRID_IN            
,SUM(NVL(N4L_HYBRID_OUT_CNT,0))   AS MIGRATION_HYBRID_OUT             
,SUM(NVL(N4L_POSTPAID_IN_CNT,0))  AS MIGRATION_POSTPAID_IN             
,SUM(NVL(N4L_POSTPAID_OUT_CNT,0)) AS MIGRATION_POSTPAID_OUT             
,SUM(NVL(N4L_PREPAID_IN_CNT,0))   AS MIGRATION_PREPAID_IN     
,SUM(NVL(N4L_PREPAID_OUT_CNT,0))  AS MIGRATION_PREPAID_OUT      
FROM BIB.FRU_SUBSCRIBER_SNAPD_HL SUBS
where
SUBS.DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
SUBS.DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY
SUBS.DATE_KEY 
,SUBSTR(SUBS.DATE_KEY,1,4)                  
,TO_NUMBER(SUBSTR(SUBS.DATE_KEY,5,2))     
,CASE WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '01' THEN 'January'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '02' THEN 'February'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '03' THEN 'January'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '04' THEN 'April'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '05' THEN 'May'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '06' THEN 'June'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '07' THEN 'July'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '08' THEN 'August'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '09' THEN 'September'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '10' THEN 'October'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '11' THEN 'November'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '12' THEN 'December'
else 'UNKNOWN' END 
,SUBSTR(SUBS.DATE_KEY,7,2)
),
RECHARGE_VALUES ASrechar
(
SELECT /*+ parallel 10*/ DATE_KEY,SUM(RECHARGE_AMT) RECHARGE_AMOUNT
FROM BIB.FCT_PREPAID_USAGE_SUMD
where
DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY
DATE_KEY
),
--
-- REVENUE
REVENUE_VALUES AS
(
Select 
C.DATE_KEY
,SUM(Case When B.Usage_Type_Cd ='SMS' Then C.Charge_Amt Else 0 End) SMS_REVENUE
,SUM(Case When B.Usage_Type_Cd ='DAT' Then C.Charge_Amt Else 0 End) DATA_REVENUE
,SUM(Case When B.Usage_Type_Cd ='VOI' Then C.Charge_Amt Else 0 End) VOICE_REVENUE
,SUM(Case When B.Usage_Type_Cd ='CON' Then C.Charge_Amt Else 0 End) CONTENT_REVENUE --Include EBB + CONTENT
,SUM(Case When B.Usage_Type_Cd ='VAS' Then C.Charge_Amt Else 0 End) USSD_REVENUE
,SUM(Case When B.Usage_Type_Cd ='MMS' Then C.Charge_Amt Else 0 End) MMS_REVENUE
,SUM(Case When B.Usage_Type_Cd NOT IN ('SMS','DAT','VOI','CON','MMS','VAS') Then C.Charge_Amt Else 0 End) OTHER_REVENUE
,SUM(C.Charge_Amt) TOTAL_REVENUE
From BIB.FRU_USAGE_ALL_SUMD C       
INNER Join BIB.DIM_USAGE_TYPE B
On C.Usage_Type_Key = B.Usage_Type_Key
where
C.DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
C.DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY 
C.date_key
),
-- DATA REVENUE
DATA_REVENUE AS
(
SELECT
C.DATE_KEY
,SUM(CHARGE_AMT) AS DATA_REVENUE
FROM 
BIB.FRU_USAGE_DATA_SUMD C
where
C.DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
C.DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY
 C.DATE_KEY
)
------------
--MAIN QUERY
SELECT /*+PARALLEL(a,16) */
MAIN.DATE_KEY
,MAIN.MONTH_NAME AS MONTH_NAME
-----------------------------------------------------------
--SUBSCRIBER
,SUM(OPENING_BASE)         AS "Openingbase"
,sum(NEW_ADDITIONS)        AS "New Additions" 
,SUM(RETURNEES)            AS "Returnees"
,sum(GROSS_ADDITIONS)      AS "Gross Additions"          
,SUM(CHURN)                AS churn
,SUM(TOTAL_SUBSCRIBERS)    AS "ClosingBase"
,SUM(NET_ADDITIONS)        AS "net additions"  
,SUM(RGS30_COUNT)          AS "RGS30"       
--,SUM(MIGRATION_CNT)    "MIGRATION COUNT" 
,SUM(NVL(RECH.RECHARGE_AMOUNT,0))  "Recharge" -- Remove extra-time

-- REVENUE
,SUM(NVL(REV.VOICE_REVENUE,0))      AS "Voice Revenue"
,SUM(NVL(REV.SMS_REVENUE,0))        AS "SMS Revenue"
,SUM(NVL(REV.DATA_REVENUE,0))       AS "Data Revenue"
,SUM(NVL(REV.CONTENT_REVENUE,0))    AS "Content Revenue"
,SUM(NVL(REV.USSD_REVENUE,0))       AS "USSD Revenue"
,SUM(NVL(REV.MMS_REVENUE,0))        AS "MMS Revenue"
,SUM(NVL(REV.OTHER_REVENUE,0))      AS "Other Revenue"
,SUM(NVL(REV.TOTAL_REVENUE,0))      AS "Total Revenue"
,SUM(NVL(REV.SMS_REVENUE,0) + NVL(DREV.DATA_REVENUE,0)+ NVL(REV.MMS_REVENUE,0) + NVL(REV.USSD_REVENUE,0)+ NVL(REV.CONTENT_REVENUE,0))/sum(NVL(REV.TOTAL_REVENUE,0)) *100 AS "% Non Voice Spend"
-- NUMBER FOR LIFE
,SUM(MIGRATION_COUNT)        AS "Migrations"       
,SUM(MIGRATION_HYBRID_IN)    AS "N4L Hybrid In"             
,SUM(MIGRATION_HYBRID_OUT)   AS "N4L Hybrid Out"             
,SUM(MIGRATION_POSTPAID_IN)  AS "N4L Postpaid In"              
,SUM(MIGRATION_POSTPAID_OUT) AS "N4L Postpaid Out"             
,SUM(MIGRATION_PREPAID_IN)   AS "N4L Prepaid In"     
,SUM(MIGRATION_PREPAID_OUT)  AS "N4L Prepaid Out"      
FROM                          
MAIN_SEL MAIN
    LEFT OUTER JOIN RECHARGE_VALUES RECH ON MAIN.DATE_KEY = RECH.DATE_KEY 
    LEFT OUTER JOIN REVENUE_VALUES REV ON MAIN.DATE_KEY = REV.DATE_KEY 
    LEFT OUTER JOIN DATA_REVENUE DREV ON MAIN.DATE_KEY = DREV.DATE_KEY 
GROUP BY
MAIN.YEAR                 
,MAIN.MONTH_ID     
,MAIN.MONTH_NAME 
,MAIN.DATE_KEY
ORDER BY 1,2,3,4,5   



-----------------------------payment_option

/

WITH 
MAIN_SEL AS
(
   SELECT /*+PARALLEL(a,16) */ SUBS.DATE_KEY AS DATE_KEY
         ,SUBSTR(SUBS.DATE_KEY,1,4) AS YEAR                  
         ,TO_NUMBER(SUBSTR(SUBS.DATE_KEY,5,2)) AS MONTH_ID     
         ,CASE WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '01' THEN 'January'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '02' THEN 'February'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '03' THEN 'January'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '04' THEN 'April'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '05' THEN 'May'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '06' THEN 'June'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '07' THEN 'July'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '08' THEN 'August'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '09' THEN 'September'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '10' THEN 'October'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '11' THEN 'November'
               WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '12' THEN 'December'
          ELSE 'UNKNOWN' END AS MONTH_NAME
        ,SUBSTR(SUBS.DATE_KEY,7,2) AS DAYNUM
        ,p.PAYMENT_OPTION_CD
-----------------------------------------------------------
--SUBSCRIBER
,SUM(NVL(OPENING_CNT,0))       OPENING_BASE
,sum(NVL(new_cnt,0))         NEW_ADDITIONS 
,SUM(NVL(returner_cnt,0))    RETURNEES
,sum(NVL(new_cnt,0)) + SUM(NVL(returner_cnt,0))   AS           GROSS_ADDITIONS          
,SUM(NVL(churner_cnt,0)) AS CHURN
,SUM(NVL(CLOSING_CNT,0))                                    TOTAL_SUBSCRIBERS
,SUM((NVL(CLOSING_CNT,0) + NVL(OPENING_CNT,0))/2) AS AVERAGE_SUBSCRIBERS
,SUM(NVL(CLOSING_CNT,0) - NVL(OPENING_CNT,0)) as Net_Additions  

,SUM(NVL(RGS30_CNT,0))        RGS30_COUNT       
--,SUM(MIGRATION_CNT)    MIGRATION_COUNT 

-- NUMBER FOR LIFE
,SUM(NVL(N4L_CNT,0))              AS MIGRATION_COUNT      
,SUM(NVL(N4L_HYBRID_IN_CNT,0))    AS MIGRATION_HYBRID_IN            
,SUM(NVL(N4L_HYBRID_OUT_CNT,0))   AS MIGRATION_HYBRID_OUT             
,SUM(NVL(N4L_POSTPAID_IN_CNT,0))  AS MIGRATION_POSTPAID_IN             
,SUM(NVL(N4L_POSTPAID_OUT_CNT,0)) AS MIGRATION_POSTPAID_OUT             
,SUM(NVL(N4L_PREPAID_IN_CNT,0))   AS MIGRATION_PREPAID_IN     
,SUM(NVL(N4L_PREPAID_OUT_CNT,0))  AS MIGRATION_PREPAID_OUT      
FROM BIB.FRU_SUBSCRIBER_SNAPD_HL SUBS
left outer join BIB.DIM_PACKAGE p On SUBS.package_key = p.package_key
where
SUBS.DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
SUBS.DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY
SUBS.DATE_KEY
,p.PAYMENT_OPTION_CD
,SUBSTR(SUBS.DATE_KEY,1,4)                  
,TO_NUMBER(SUBSTR(SUBS.DATE_KEY,5,2))     
,CASE WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '01' THEN 'January'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '02' THEN 'February'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '03' THEN 'January'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '04' THEN 'April'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '05' THEN 'May'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '06' THEN 'June'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '07' THEN 'July'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '08' THEN 'August'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '09' THEN 'September'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '10' THEN 'October'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '11' THEN 'November'
WHEN SUBSTR(SUBS.DATE_KEY,5,2) = '12' THEN 'December'
else 'UNKNOWN' END 
,SUBSTR(SUBS.DATE_KEY,7,2), p.PAYMENT_OPTION_CD
),
RECHARGE_VALUES AS
(
SELECT /*+ parallel 10*/ DATE_KEY,
p.PAYMENT_OPTION_CD, 
SUM(RECHARGE_AMT) RECHARGE_AMOUNT
FROM BIB.FCT_PREPAID_USAGE_SUMD c
left outer join BIB.DIM_PACKAGE p On C.package_key = p.package_key
where
DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY
DATE_KEY,p.PAYMENT_OPTION_CD
),
--
-- REVENUE
REVENUE_VALUES AS
(
Select 
C.DATE_KEY
,p.PAYMENT_OPTION_CD
,SUM(Case When B.Usage_Type_Cd ='SMS' Then C.Charge_Amt Else 0 End) SMS_REVENUE
,SUM(Case When B.Usage_Type_Cd ='DAT' Then C.Charge_Amt Else 0 End) DATA_REVENUE
,SUM(Case When B.Usage_Type_Cd ='VOI' Then C.Charge_Amt Else 0 End) VOICE_REVENUE
,SUM(Case When B.Usage_Type_Cd ='CON' Then C.Charge_Amt Else 0 End) CONTENT_REVENUE --Include EBB + CONTENT
,SUM(Case When B.Usage_Type_Cd ='VAS' Then C.Charge_Amt Else 0 End) USSD_REVENUE
,SUM(Case When B.Usage_Type_Cd ='MMS' Then C.Charge_Amt Else 0 End) MMS_REVENUE
,SUM(Case When B.Usage_Type_Cd NOT IN ('SMS','DAT','VOI','CON','MMS','VAS') Then C.Charge_Amt Else 0 End) OTHER_REVENUE
,SUM(C.Charge_Amt) TOTAL_REVENUE
From BIB.FRU_USAGE_ALL_SUMD C       
left outer Join BIB.DIM_USAGE_TYPE B On C.Usage_Type_Key = B.Usage_Type_Key
left outer join BIB.DIM_PACKAGE p On C.package_key = p.package_key --,p.PAYMENT_OPTION_CD
where
C.DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
C.DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY 
C.date_key, p.PAYMENT_OPTION_CD
),
-- DATA REVENUE
DATA_REVENUE AS
(
SELECT
C.DATE_KEY, p.PAYMENT_OPTION_CD
,SUM(CHARGE_AMT) AS DATA_REVENUE
FROM 
BIB.FRU_USAGE_DATA_SUMD C left outer join BIB.DIM_PACKAGE p On C.package_key = p.package_key
where
C.DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
C.DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY
 C.DATE_KEY, p.PAYMENT_OPTION_CD
)
------------
--MAIN QUERY
SELECT /*+PARALLEL(a,16) */
MAIN.DATE_KEY
,MAIN.MONTH_NAME AS MONTH_NAME
,MAIN.PAYMENT_OPTION_CD
-----------------------------------------------------------
--SUBSCRIBER
,SUM(OPENING_BASE)         AS "Openingbase"
,sum(NEW_ADDITIONS)        AS "New Additions" 
,SUM(RETURNEES)            AS "Returnees"
,sum(GROSS_ADDITIONS)      AS "Gross Additions"          
,SUM(CHURN)                AS churn
,SUM(TOTAL_SUBSCRIBERS)    AS "ClosingBase"
,SUM(NET_ADDITIONS)        AS "net additions"  
,SUM(RGS30_COUNT)          AS "RGS30"       
--,SUM(MIGRATION_CNT)    "MIGRATION COUNT" 
,SUM(NVL(RECH.RECHARGE_AMOUNT,0))  "Recharge" -- Remove extra-time

-- REVENUE
,SUM(NVL(REV.VOICE_REVENUE,0))      AS "Voice Revenue"
,SUM(NVL(REV.SMS_REVENUE,0))        AS "SMS Revenue"
,SUM(NVL(REV.DATA_REVENUE,0))       AS "Data Revenue"
,SUM(NVL(REV.CONTENT_REVENUE,0))    AS "Content Revenue"
,SUM(NVL(REV.USSD_REVENUE,0))       AS "USSD Revenue"
,SUM(NVL(REV.MMS_REVENUE,0))        AS "MMS Revenue"
,SUM(NVL(REV.OTHER_REVENUE,0))      AS "Other Revenue"
,SUM(NVL(REV.TOTAL_REVENUE,0))      AS "Total Revenue"
,SUM(NVL(REV.SMS_REVENUE,0) + NVL(DREV.DATA_REVENUE,0)+ NVL(REV.MMS_REVENUE,0) + NVL(REV.USSD_REVENUE,0)+ NVL(REV.CONTENT_REVENUE,0))/NULLIF(sum(NVL(REV.TOTAL_REVENUE,0)),0) * 100 AS "% Non Voice Spend"
-- NUMBER FOR LIFE
,SUM(MIGRATION_COUNT)        AS "Migrations"       
,SUM(MIGRATION_HYBRID_IN)    AS "N4L Hybrid In"             
,SUM(MIGRATION_HYBRID_OUT)   AS "N4L Hybrid Out"             
,SUM(MIGRATION_POSTPAID_IN)  AS "N4L Postpaid In"              
,SUM(MIGRATION_POSTPAID_OUT) AS "N4L Postpaid Out"             
,SUM(MIGRATION_PREPAID_IN)   AS "N4L Prepaid In"     
,SUM(MIGRATION_PREPAID_OUT)  AS "N4L Prepaid Out"      
FROM                          
MAIN_SEL MAIN
    LEFT OUTER JOIN RECHARGE_VALUES RECH ON (MAIN.DATE_KEY = RECH.DATE_KEY and MAIN.PAYMENT_OPTION_CD = RECH.PAYMENT_OPTION_CD)
    LEFT OUTER JOIN REVENUE_VALUES REV ON (MAIN.DATE_KEY = REV.DATE_KEY  and  MAIN.PAYMENT_OPTION_CD = REV.PAYMENT_OPTION_CD)
    LEFT OUTER JOIN DATA_REVENUE DREV ON (MAIN.DATE_KEY = DREV.DATE_KEY and  MAIN.PAYMENT_OPTION_CD = DREV.PAYMENT_OPTION_CD) 
GROUP BY
MAIN.YEAR                 
,MAIN.MONTH_ID     
,MAIN.MONTH_NAME 
,MAIN.DATE_KEY,
--MAIN.PAYMENT_OPTION_CD
ORDER BY 1,2,3,4,5   

;


--select date_key,  sum(RECHARGE_AMOUNT) from 
--(
SELECT /*+ parallel 10*/ DATE_KEY,
p.PAYMENT_OPTION_CD, 
SUM(RECHARGE_AMT) RECHARGE_AMOUNT
FROM BIB.FCT_PREPAID_USAGE_SUMD c
left outer join BIB.DIM_PACKAGE p On C.package_key = p.package_key
where
DATE_KEY  >= to_char(sysdate - 30,'YYYYMMDD') AND 
DATE_KEY  <=  to_char(sysdate - 1,'YYYYMMDD')
GROUP BY
DATE_KEY,p.PAYMENT_OPTION_CD
order by DATE_KEY desc
--)
--group by date_key 
--order by DATE_KEY desc
;

