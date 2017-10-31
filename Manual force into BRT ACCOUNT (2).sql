

SELECT DISTINCT INVOICE_DETAILS.ACCOUNT_NO, ba.BRT_BILLING_ACCOUNT_KEY
  FROM (SELECT * FROM STG_GEN.I_BILLING_INVOICE_ITEM WHERE BATCH_ID = 3249541 AND ORA_HASH (TRIM (ACCOUNT_NO), 9) = 4) INVOICE_DETAILS
       LEFT OUTER JOIN BIB.BRT_BILLING_ACCOUNT BA
          ON(BA.ACCOUNT_NO = INVOICE_DETAILS.ACCOUNT_NO
            AND INVOICE_DETAILS.INVOICE_DT > RECORD_START_DT
            AND INVOICE_DETAILS.INVOICE_DT <= RECORD_END_DT)
where ba.BRT_BILLING_ACCOUNT_KEY is null ---only return missing accounts, this should be logged with source to check
/



SELECT DISTINCT INVOICE_DETAILS.ACCOUNT_NO, ba.BRT_BILLING_ACCOUNT_KEY
  FROM (SELECT *
          FROM STG_GEN.I_BILLING_INVOICE_ITEM
         WHERE     BATCH_ID = 3249541
               AND ORA_HASH (TRIM (ACCOUNT_NO), 9) = 4
               AND account_no = 'X5113714') INVOICE_DETAILS
       LEFT OUTER JOIN BIB.BRT_BILLING_ACCOUNT BA
          ON (    BA.ACCOUNT_NO = INVOICE_DETAILS.ACCOUNT_NO
              AND INVOICE_DETAILS.INVOICE_DT > RECORD_START_DT
              AND INVOICE_DETAILS.INVOICE_DT <= RECORD_END_DT)
              /
and BRT_BILLING_ACCOUNT_KEY is null;
 
/





--------manual force into BRT
BEGIN

INSERT into BIB.BRT_BILLING_ACCOUNT
(ACCOUNT_NO,CUSTOMER_KEY,BILLING_CYCLE_KEY,CURRENCY_KEY,ACCOUNT_STATUS_KEY,CREDIT_CLASS_KEY
,BILL_ADDRESS_KEY,STREET_ADDRESS_KEY,POSTAL_ADDRESS_KEY,BILLING_ACCOUNT_KEY
,RECORD_START_DT, BATCH_ID,  RECORD_END_DT , RECORD_FORCE_SOURCE , CURRENT_RECORD_IND, BRT_BILLING_ACCOUNT_KEY)
SELECT
gt.ACCOUNT_NO,gt.CUSTOMER_KEY,gt.BILLING_CYCLE_KEY,gt.CURRENCY_KEY,gt.ACCOUNT_STATUS_KEY,
gt.CREDIT_CLASS_KEY,gt.BILL_ADDRESS_KEY,gt.STREET_ADDRESS_KEY,gt.POSTAL_ADDRESS_KEY,gt.BILLING_ACCOUNT_KEY
, to_date('19000101 00:00:00','YYYYMMDD HH24:MI:SS') RECORD_START_DT
, 3402000 BATCH_ID ,to_date('99991231 23:59:59','YYYYMMDD HH24:MI:SS')RECORD_END_DT , 'MANUAL' RECORD_FORCE_SOURCE, 'Y' CURRENT_RECORD_IND
, BIB_CTL.BRT_BILLING_ACCOUNT_SEQ.nextval BRT_BILLING_ACCOUNT_KEY

from

                          (
                          SELECT
                          BIB_CTL.PKG_ADDRESS.lku_T2(B.ADDRESS_CD,'BILLING',AD_BILLING.ADDRESS_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) BILL_ADDRESS_KEY,
                          BIB_CTL.PKG_ADDRESS.lku_T2(B.ADDRESS_CD,'STREET',AD_STREET.ADDRESS_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) STREET_ADDRESS_KEY,
                          BIB_CTL.PKG_ADDRESS.lku_T2(B.ADDRESS_CD,'POSTAL',AD_POSTAL.ADDRESS_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) POSTAL_ADDRESS_KEY,
                          BIB_CTL.PKG_BILLING_ACCOUNT.lku_T2(B.ACCOUNT_NO,BILL_ACC.BILLING_ACCOUNT_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) BILLING_ACCOUNT_KEY,
                          BIB_CTL.PKG_CUSTOMER.lku_T2(B.CUSTOMER_CD,CUST.CUSTOMER_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) CUSTOMER_KEY,
                          B.* from ( select
                          BIB_CTL.PKG_CREDIT_CLASS.lku(A.CREDIT_CLASS_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') CREDIT_CLASS_KEY,
                          BIB_CTL.PKG_ACCOUNT_STATUS.lku(A.ACCOUNT_STATUS_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') ACCOUNT_STATUS_KEY,
                          BIB_CTL.PKG_CURRENCY.lku(A.CURRENCY_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') CURRENCY_KEY,
                          BIB_CTL.PKG_BILLING_CYCLE.lku(A.BILLING_CYCLE_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') BILLING_CYCLE_KEY,
                          A.* from ( 
                          SELECT /*+ parallel 10 */
                                TO_DATE (20170101, 'yyyymmdd') EVENT_DT, ACCOUNT_NO, NULL CREDIT_CLASS_CD,
                                 NULL ACCOUNT_STATUS_CD, NULL CURRENCY_CD, NULL BILLING_CYCLE_CD, NULL ADDRESS_CD, NULL CUSTOMER_CD
                            FROM BIB.DIM_BILLING_ACCOUNT a
                           WHERE account_no = 'X5113714' ----------------required account number, you can also put the select that return the affected account 
                          )A
                          )B
                          left outer join
                           BIB.dim_address AD_BILLING
                          on (AD_BILLING.address_type_cd='BILLING' and AD_BILLING.record_start_dt < B.event_dt and AD_BILLING.record_end_dt >= B.event_dt
                          and to_char(B.ADDRESS_CD) = AD_BILLING.ADDRESS_CD)
                          left outer join
                           BIB.dim_address AD_STREET
                          on (AD_STREET.address_type_cd='STREET' and AD_STREET.record_start_dt < B.event_dt and AD_STREET.record_end_dt >= B.event_dt
                          and to_char(B.ADDRESS_CD) = AD_STREET.ADDRESS_CD)
                          left outer join
                          BIB.dim_address AD_POSTAL
                          on (AD_POSTAL.address_type_cd='POSTAL' and AD_POSTAL.record_start_dt < B.event_dt and AD_POSTAL.record_end_dt >= B.event_dt
                          and to_char(B.ADDRESS_CD) = AD_POSTAL.ADDRESS_CD)
                          left outer join BIB.DIM_BILLING_ACCOUNT bill_acc
                          on ( to_char(b.account_no) = bill_acc.account_no and bill_acc.record_start_dt < b.event_dt and bill_acc.record_end_dt >= b.event_dt)
                          left outer join BIB.DIM_CUSTOMER cust
                          on ( to_char(B.customer_cd) = cust.customer_cd and cust.record_start_dt < b.event_dt and cust.record_end_dt >= b.event_dt)
                           ) gt;
 COMMIT;
 
 
 
END; 

commit;

/


SELECT
BIB_CTL.PKG_ADDRESS.lku_T2(B.ADDRESS_CD,'BILLING',AD_BILLING.ADDRESS_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) BILL_ADDRESS_KEY,
BIB_CTL.PKG_ADDRESS.lku_T2(B.ADDRESS_CD,'STREET',AD_STREET.ADDRESS_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) STREET_ADDRESS_KEY,
BIB_CTL.PKG_ADDRESS.lku_T2(B.ADDRESS_CD,'POSTAL',AD_POSTAL.ADDRESS_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) POSTAL_ADDRESS_KEY,
BIB_CTL.PKG_BILLING_ACCOUNT.lku_T2(B.ACCOUNT_NO,BILL_ACC.BILLING_ACCOUNT_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) BILLING_ACCOUNT_KEY,
BIB_CTL.PKG_CUSTOMER.lku_T2(B.CUSTOMER_CD,CUST.CUSTOMER_KEY,'STD_TE_S2E_BRT_BILLING_ACCOUNT',B.EVENT_DT) CUSTOMER_KEY,
B.* from ( select
BIB_CTL.PKG_CREDIT_CLASS.lku(A.CREDIT_CLASS_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') CREDIT_CLASS_KEY,
BIB_CTL.PKG_ACCOUNT_STATUS.lku(A.ACCOUNT_STATUS_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') ACCOUNT_STATUS_KEY,
BIB_CTL.PKG_CURRENCY.lku(A.CURRENCY_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') CURRENCY_KEY,
BIB_CTL.PKG_BILLING_CYCLE.lku(A.BILLING_CYCLE_CD,'STD_TE_S2E_BRT_BILLING_ACCOUNT') BILLING_CYCLE_KEY,
A.* from ( 
SELECT /*+ parallel 10 */
      TO_DATE (20170101, 'yyyymmdd') EVENT_DT, ACCOUNT_NO, NULL CREDIT_CLASS_CD,
       NULL ACCOUNT_STATUS_CD, NULL CURRENCY_CD, NULL BILLING_CYCLE_CD, NULL ADDRESS_CD, NULL CUSTOMER_CD
  FROM BIB.DIM_BILLING_ACCOUNT a
 WHERE account_no = 'A392393' ----------------required account number, you can also put the select that return the affected account 
)A
)B
left outer join
 BIB.dim_address AD_BILLING
on (AD_BILLING.address_type_cd='BILLING' and AD_BILLING.record_start_dt < B.event_dt and AD_BILLING.record_end_dt >= B.event_dt
and to_char(B.ADDRESS_CD) = AD_BILLING.ADDRESS_CD)
left outer join
 BIB.dim_address AD_STREET
on (AD_STREET.address_type_cd='STREET' and AD_STREET.record_start_dt < B.event_dt and AD_STREET.record_end_dt >= B.event_dt
and to_char(B.ADDRESS_CD) = AD_STREET.ADDRESS_CD)
left outer join
BIB.dim_address AD_POSTAL
on (AD_POSTAL.address_type_cd='POSTAL' and AD_POSTAL.record_start_dt < B.event_dt and AD_POSTAL.record_end_dt >= B.event_dt
and to_char(B.ADDRESS_CD) = AD_POSTAL.ADDRESS_CD)
left outer join BIB.DIM_BILLING_ACCOUNT bill_acc
on ( to_char(b.account_no) = bill_acc.account_no and bill_acc.record_start_dt < b.event_dt and bill_acc.record_end_dt >= b.event_dt)
left outer join BIB.DIM_CUSTOMER cust
on ( to_char(B.customer_cd) = cust.customer_cd and cust.record_start_dt < b.event_dt and cust.record_end_dt >= b.event_dt)
 ;
 