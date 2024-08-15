-- Update customer_snapshot table with new entries

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

-- Update customer_snapshot table rows with updated info with an end date

MERGE INTO SF_TPCDS2.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS2.RAW.CUSTOMER t2
ON t1.C_CUSTOMER_SK = t2.C_CUSTOMER_SK
WHEN MATCHED 
    AND (
        t1.C_SALUTATION != t2.C_SALUTATION
        OR t1.C_PREFERRED_CUST_FLAG != t2.C_PREFERRED_CUST_FLAG
        OR COALESCE(t1.C_FIRST_SALES_DATE_SK, 0) != COALESCE(t2.C_FIRST_SALES_DATE_SK, 0)
        OR t1.C_LOGIN != t2.C_LOGIN
        OR COALESCE(t1.C_CURRENT_CDEMO_SK, 0) != COALESCE(t2.C_CURRENT_CDEMO_SK, 0)
        OR t1.C_FIRST_NAME != t2.C_FIRST_NAME
        OR COALESCE(t1.C_CURRENT_HDEMO_SK, 0) != COALESCE(t2.C_CURRENT_HDEMO_SK, 0)
        OR t1.C_CURRENT_ADDR_SK != t2.C_CURRENT_ADDR_SK
        OR t1.C_LAST_NAME != t2.C_LAST_NAME
        OR t1.C_CUSTOMER_ID != t2.C_CUSTOMER_ID
        OR COALESCE(t1.C_LAST_REVIEW_DATE_SK, 0) != COALESCE(t2.C_LAST_REVIEW_DATE_SK, 0)
        OR COALESCE(t1.C_BIRTH_MONTH, 0) != COALESCE(t2.C_BIRTH_MONTH, 0)
        OR t1.C_BIRTH_COUNTRY != t2.C_BIRTH_COUNTRY
        OR COALESCE(t1.C_BIRTH_YEAR, 0) != COALESCE(t2.C_BIRTH_YEAR, 0)
        OR COALESCE(t1.C_BIRTH_DAY, 0) != COALESCE(t2.C_BIRTH_DAY, 0)
        OR t1.C_EMAIL_ADDRESS != t2.C_EMAIL_ADDRESS
        OR COALESCE(t1.C_FIRST_SHIPTO_DATE_SK, 0) != COALESCE(t2.C_FIRST_SHIPTO_DATE_SK, 0)
        )
    THEN UPDATE 
    SET END_DATE = CURRENT_DATE();

-- Insert values into the customer_dim table in the analytics schema

CREATE OR REPLACE TABLE SF_TPCDS2.ANALYTICS.CUSTOMER_DIM AS
    (SELECT 
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
        CA_STREET_NAME,
        CA_SUITE_NUMBER,
        CA_STATE,
        CA_LOCATION_TYPE,
        CA_COUNTRY,
        CA_ADDRESS_ID,
        CA_COUNTY,
        CA_STREET_NUMBER,
        CA_ZIP,
        CA_CITY,
        CA_STREET_TYPE,
        CA_GMT_OFFSET,
        CD_DEP_EMPLOYED_COUNT,
        CD_DEP_COUNT,
        CD_CREDIT_RATING,
        CD_EDUCATION_STATUS,
        CD_PURCHASE_ESTIMATE,
        CD_MARITAL_STATUS,
        CD_DEP_COLLEGE_COUNT,
        CD_GENDER,
        HD_BUY_POTENTIAL,
        HD_INCOME_BAND_SK,
        HD_DEP_COUNT,
        HD_VEHICLE_COUNT,
        IB_LOWER_BOUND,
        IB_UPPER_BOUND,
        START_DATE,
        END_DATE
    FROM SF_TPCDS2.INTERMEDIATE.CUSTOMER_SNAPSHOT
    LEFT JOIN TPCDS2.RAW.CUSTOMER_ADDRESS ON C_CURRENT_ADDR_SK = CA_ADDRESS_SK
    LEFT JOIN TPCDS2.RAW.CUSTOMER_DEMOGRAPHICS ON C_CURRENT_CDEMO_SK = CD_DEMO_SK
    LEFT JOIN TPCDS2.RAW.HOUSEHOLD_DEMOGRAPHICS ON C_CURRENT_HDEMO_SK = HD_DEMO_SK
    LEFT JOIN TPCDS2.RAW.INCOME_BAND ON HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
    );