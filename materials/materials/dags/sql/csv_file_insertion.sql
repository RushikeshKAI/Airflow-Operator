COPY bridge_table(MyUnknownColumn, CHANNEL_DIM_SKEY, FOLIO_DIM_SKEY, KARVY_REF_NO, FOLIO_ID, SCHEME_CODE, SCHEME_PLAN, TRXN_PIN_CODE, TRXN_CAT, TRXN_TYPE, TXN_SUB_TYPE, INSTRUMENT_TYPE, INSTRUMENT_BANK, LOAD_PERCENTAGE, TRXN_DATE, TXN_PROCESSING_DATE, TRXN_BATCH_CLOSE_DATE, TRXN_STATUS, NO_OF_UNITS, PRICE_PER_UNIT, APPLICABLE_NAV_DATE, APPLICABLE_NAV, APPLICABLE_POP, GROSS_AMOUNT, REMARKS, RECORD_VALID_FLAG, INVESTOR_LOCATION, PIN_CODE_DIM_SKEY, ONLINE_OFFLINE_FLAG, INVESTOR_TYPE, TRXN_PROCESS_FLAG, TRXN_CLASSIFICATION, MF_SALES_TRXN_FACT_SKEY,	CHANNEL_ID, RIA_CODE, is_excluded, is_reversal)
FROM '/home/rushikesh/Learning-Projects/Airflow-Operator/materials/transactions_sample-transactions_sample.csv'
DELIMITER ','
CSV HEADER;


-- FORCE_NULL (TRXN_PIN_CODE,TRXN_TYPE,TXN_SUB_TYPE,INSTRUMENT_TYPE,INSTRUMENT_BANK,TRXN_STATUS,PRICE_PER_UNIT,REMARKS,RECORD_VALID_FLAG,ONLINE_OFFLINE_FLAG,INVESTOR_TYPE,TRXN_PROCESS_FLAG,TRXN_CLASSIFICATION,CHANNEL_ID,RIA_CODE,is_excluded,is_reversal)