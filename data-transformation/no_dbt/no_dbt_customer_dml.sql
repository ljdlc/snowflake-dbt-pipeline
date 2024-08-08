MERGE INTO SF_TPCDS2.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS2.RAW.CUSTOMER t2
ON t1.C_SALUTATION = t2.C_SALUTATION
   AND t1.C_PREFERRED_CUST_FLAG = t2.C_PREFERRED_CUST_FLAG
   AND COALESCE(t1.C_FIRST_SALES_DATE_SK, 0) = COALESCE(t2.C_FIRST_SALES_DATE_SK, 0)
   AND t1.C_CUSTOMER_SK = t2.C_CUSTOMER_SK
   AND t1.C_LOGIN = t2.C_LOGIN
   AND COALESCE(t1.C_CURRENT_CDEMO_SK, 0) = COALESCE(t2.C_CURRENT_CDEMO_SK, 0)
   AND t1.C_FIRST_NAME = t2.C_FIRST_NAME
   AND COALESCE(t1.C_CURRENT_HDEMO_SK, 0) = COALESCE(t2.C_CURRENT_HDEMO_SK, 0)
   AND t1.C_CURRENT_ADDR_SK = t2.C_CURRENT_ADDR_SK
   AND t1.C_LAST_NAME = t2.C_LAST_NAME
   AND t1.C_CUSTOMER_ID = t2.C_CUSTOMER_ID
   AND COALESCE(t1.C_LAST_REVIEW_DATE_SK, 0) = COALESCE(t2.C_LAST_REVIEW_DATE_SK, 0)
   AND COALESCE(t1.C_BIRTH_MONTH, 0) = COALESCE(t2.C_BIRTH_MONTH, 0)
   AND t1.C_BIRTH_COUNTRY = t2.C_BIRTH_COUNTRY
   AND COALESCE(t1.C_BIRTH_YEAR, 0) = COALESCE(t2.C_BIRTH_YEAR, 0)
   AND COALESCE(t1.C_BIRTH_DAY, 0) = COALESCE(t2.C_BIRTH_DAY, 0)
   AND t1.C_EMAIL_ADDRESS = t2.C_EMAIL_ADDRESS
   AND COALESCE(t1.C_FIRST_SHIPTO_DATE_SK, 0) = COALESCE(t2.C_FIRST_SHIPTO_DATE_SK, 0)
WHEN NOT MATCHED THEN
    INSERT (
        C_SALUTATION, 
        C_PREFERRED_CUST_FLAG, 
        C_FIRST_SALES_DATE_SK, 
        C_CUSTOMER_SK, 
        C_LOGIN, 
        C_CURRENT_CDEMO_SK, 
        C_FIRST_NAME, 
        C_CURRENT_HDEMO_SK, 
        C_CURRENT_ADDR_SK, 
        C_LAST_NAME, 
        C_CUSTOMER_ID, 
        C_LAST_REVIEW_DATE_SK, 
        C_BIRTH_MONTH, 
        C_BIRTH_COUNTRY, 
        C_BIRTH_YEAR, 
        C_BIRTH_DAY, 
        C_EMAIL_ADDRESS, 
        C_FIRST_SHIPTO_DATE_SK,
        START_DATE,
        END_DATE)
    VALUES (
        t2.C_SALUTATION, 
        t2.C_PREFERRED_CUST_FLAG, 
        t2.C_FIRST_SALES_DATE_SK, 
        t2.C_CUSTOMER_SK, 
        t2.C_LOGIN, 
        t2.C_CURRENT_CDEMO_SK, 
        t2.C_FIRST_NAME, 
        t2.C_CURRENT_HDEMO_SK, 
        t2.C_CURRENT_ADDR_SK, 
        t2.C_LAST_NAME, 
        t2.C_CUSTOMER_ID, 
        t2.C_LAST_REVIEW_DATE_SK, 
        t2.C_BIRTH_MONTH, 
        t2.C_BIRTH_COUNTRY, 
        t2.C_BIRTH_YEAR, 
        t2.C_BIRTH_DAY, 
        t2.C_EMAIL_ADDRESS, 
        t2.C_FIRST_SHIPTO_DATE_SK,
        CURRENT_DATE(),
        NULL
        );