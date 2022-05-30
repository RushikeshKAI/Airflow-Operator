# Bridegweave Required Packages
import pandas as pd
import csv
import os
# import numpy as np
# from sqlalchemy import create_engine, types
# import datetime as dt
# import itertools
# from functools import reduce
import time
import json
# import urllib
# from functools import partial
# from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
# from dateutil.relativedelta import relativedelta
# import joblib
# import pickle5
# import pymysql
# pymysql.install_as_MySQLdb()
# import gc
# Airflow Packages
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from textwrap import dedent
# from airflow.providers.mysql.operators.mysql import MySqlOperator


def get_bridgeweave_data():
    # sql_stmt = "SELECT * FROM bridge_table WHERE TRXN_DATE BETWEEN '2020-04-01' AND '2020-04-30';"
    sql_stmt = "SELECT * FROM bridge_table;"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()

def process_bridgeweave_data(ti):
    b_data = ti.xcom_pull(task_ids=['task_get_bridgeweave_data'])
    if not b_data:
        raise Exception('No data.')

    b_data = pd.DataFrame(
        data=b_data[0],
        columns=['MyUnknownColumn','CHANNEL_DIM_SKEY','FOLIO_DIM_SKEY','KARVY_REF_NO','FOLIO_ID','SCHEME_CODE','SCHEME_PLAN','TRXN_PIN_CODE','TRXN_CAT','TRXN_TYPE','TXN_SUB_TYPE','INSTRUMENT_TYPE','INSTRUMENT_BANK','LOAD_PERCENTAGE','TRXN_DATE','TXN_PROCESSING_DATE','TRXN_BATCH_CLOSE_DATE','TRXN_STATUS','NO_OF_UNITS','PRICE_PER_UNIT','APPLICABLE_NAV_DATE','APPLICABLE_NAV','APPLICABLE_POP','GROSS_AMOUNT','REMARKS','RECORD_VALID_FLAG','INVESTOR_LOCATION','PIN_CODE_DIM_SKEY','ONLINE_OFFLINE_FLAG','INVESTOR_TYPE','TRXN_PROCESS_FLAG','TRXN_CLASSIFICATION','MF_SALES_TRXN_FACT_SKEY','CHANNEL_ID','RIA_CODE','is_excluded','is_reversal']
    )
    # b_data = b_data.drop('MyUnknownColumn', axis=1)
    b_data.to_csv(Variable.get('tmp_bridgeweave_data_csv_location'), index=False)
    print("CSV file has been successfully created...")

def store_brideweave_data():
    if not os.path.isfile('./dags/data/transactions_sample-transactions_sample.csv'):
        with open('./dags/data/transactions_sample-transactions_sample.csv', 'w'):
            pass
    
    sql_stmt = '''COPY bridge_table FROM STDIN WITH (FORMAT CSV, FORCE_NULL (TRXN_PIN_CODE,TRXN_TYPE,TXN_SUB_TYPE,INSTRUMENT_TYPE,INSTRUMENT_BANK,TRXN_STATUS,PRICE_PER_UNIT,REMARKS,RECORD_VALID_FLAG,ONLINE_OFFLINE_FLAG,INVESTOR_TYPE,TRXN_PROCESS_FLAG,TRXN_CLASSIFICATION,CHANNEL_ID,RIA_CODE,is_excluded,is_reversal));'''
    pg_hook = PostgresHook(
        postgres_conn_id='postgres'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    with open('./dags/data/transactions_sample-transactions_sample.csv', 'r+') as f:
        next(f)
        cursor.copy_expert(sql_stmt, f)
        # f = csv.reader(f, delimiter=',')
        # cursor.copy_expert(sql_stmt, f, 'bridge_table', null='', columns=('MyUnknownColumn','CHANNEL_DIM_SKEY','FOLIO_DIM_SKEY','KARVY_REF_NO','FOLIO_ID','SCHEME_CODE','SCHEME_PLAN','TRXN_PIN_CODE','TRXN_CAT','TRXN_TYPE','TXN_SUB_TYPE','INSTRUMENT_TYPE','INSTRUMENT_BANK','LOAD_PERCENTAGE','TRXN_DATE','TXN_PROCESSING_DATE','TRXN_BATCH_CLOSE_DATE','TRXN_STATUS','NO_OF_UNITS','PRICE_PER_UNIT','APPLICABLE_NAV_DATE','APPLICABLE_NAV','APPLICABLE_POP','GROSS_AMOUNT','REMARKS','RECORD_VALID_FLAG','INVESTOR_LOCATION','PIN_CODE_DIM_SKEY','ONLINE_OFFLINE_FLAG','INVESTOR_TYPE','TRXN_PROCESS_FLAG','TRXN_CLASSIFICATION','MF_SALES_TRXN_FACT_SKEY','CHANNEL_ID','RIA_CODE','is_excluded','is_reversal'))
        pg_conn.commit()
    print("Data Saved Sucessfully to DB")
    # /tmp/bridgeweave_data.csv


def reduce_mem_usage(df):
    df.columns = df.columns.get_level_values(0)
    print("Shape of dataframe is: ", df.shape)
    bool_cols = df.select_dtypes('bool').columns.tolist()
    df.replace([-np.inf,np.inf],0,inplace=True)
    for c in bool_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    # df=df.convert_dtypes()
    start_mem_usg = df.memory_usage().sum() / 1024 ** 2
    print("Memory usage of dataframe is :",start_mem_usg," MB")
    for col in df.select_dtypes(['int64', 'float64']).columns:
        df[col] = df[col].fillna(0)
        # keep track of for IsInt?, max and min
        IsInt = True
        mx = df[col].max()
        mn = df[col].min()

        # print(col,mx,mn)

        # test if column can be converted to an integer
        # print(df[col][:2])
        if df[col].dtype == 'float64':
            asint = df[col].astype(np.int64)
            result = (df[col] - asint)
            result = result.sum()
            IsInt = result > -0.01 and result < 0.01

        # Make Integer/unsigned Integer datatypes
        if IsInt:
            if mn >= 0:
                if mx < 255:
                    df[col] = df[col].astype(np.uint8)
                elif mx < 65535:
                    df[col] = df[col].astype(np.uint16)
                elif mx < 4294967295:
                    df[col] = df[col].astype(np.uint32)
                else:
                    #df[col] = df[col].astype(np.uint64)
                    pass
            else:
                if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
                    #pass
                    # Make float datatypes 32 bit
        else:
            df[col] = df[col].astype(np.float32)
        # print(df[col].dtype)

    # Print final result
    print("___MEMORY USAGE AFTER COMPLETION:___")
    mem_usg = df.memory_usage().sum() / 1024 ** 2
    print("Memory usage is: ", mem_usg, " MB")
    print("This is ", 100 * mem_usg / start_mem_usg, "% of the initial size")


with DAG('bridgeweave_postgres_testing_dag', start_date=datetime(2022, 5, 17), schedule_interval='@daily', catchup=False) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='sql/CREATE_BRIDGEWEAVE_TABLE.sql'
    )

    # Fetch Bridgeweave data
    task_get_bridgeweave_data = PythonOperator(
        task_id='task_get_bridgeweave_data',
        python_callable=get_bridgeweave_data,
        do_xcom_push=True
    )

    # Process the bridegweave_data data
    export_bridgeweave_data = PythonOperator(
        task_id='export_bridgeweave_data',
        python_callable=process_bridgeweave_data
    )

    # Insert data to PostgreSQL DB through CSV File
    insert_data = PythonOperator(
        task_id='insert_data',
        # postgres_conn_id='postgres',
        python_callable=store_brideweave_data
        # sql='sql/csv_file_insertion.sql'
        # bash_command=('''psql -h postgres -d airflow -U postgres -c "\copy bridge_table(MyUnknownColumn, CHANNEL_DIM_SKEY, FOLIO_DIM_SKEY, KARVY_REF_NO, FOLIO_ID, SCHEME_CODE, SCHEME_PLAN, TRXN_PIN_CODE, TRXN_CAT, TRXN_TYPE, TXN_SUB_TYPE, INSTRUMENT_TYPE, INSTRUMENT_BANK, LOAD_PERCENTAGE, TRXN_DATE, TXN_PROCESSING_DATE, TRXN_BATCH_CLOSE_DATE, TRXN_STATUS, NO_OF_UNITS, PRICE_PER_UNIT, APPLICABLE_NAV_DATE, APPLICABLE_NAV, APPLICABLE_POP, GROSS_AMOUNT, REMARKS, RECORD_VALID_FLAG, INVESTOR_LOCATION, PIN_CODE_DIM_SKEY, ONLINE_OFFLINE_FLAG, INVESTOR_TYPE, TRXN_PROCESS_FLAG, TRXN_CLASSIFICATION, MF_SALES_TRXN_FACT_SKEY,	CHANNEL_ID, RIA_CODE, is_excluded, is_reversal)
        #     from '/home/rushikesh/Learning-Projects/Airflow-Operator/materials/transactions_sample-transactions_sample.csv'
        #     DELIMITER ','
        #     CSV HEADER";'''
	    # )
    )
    
    # get_data = PostgresOperator(
    #     task_id='get_data',
    #     postgres_conn_id='postgres',
    #     sql='sql/GET_BRIDEWEAVE_DATA.sql',
    #     params={"begin_date": "2020-04-01", "end_date": "2020-04-30"},
    # )


    # Truncate Table
    truncate_table = PostgresOperator(
        task_id='truncate_bridgeweave_table',
        postgres_conn_id='postgres',
        sql="TRUNCATE TABLE bridge_table"
    )

    # Alter table columns
    # alter_table_columns = PostgresOperator(
    #     task_id='alter_table',
    #     postgres_conn_id='postgres',
    #     sql='sql/ALTER_BRIDGETABLE_VALUES.sql'
    # )

    # store_data = PostgresOperator(
    #     task_id='store',
    #     postgres_conn_id='postgres',
    #     sql='sql/INSERT_INTO_BRIDGEWEAVE_TABLE.sql'
    #     # [
    #     #     'SELECT * FROM my_table'
    #     # ]
    #     # parameters={
    #     #     'filename': '{{ ti.xcom_pull(task_ids=["my_task"])[0] }}'
    #     # }
    # )

    # is_data_available = BranchDayOfWeekOperator(
    #     task_id='data_available',
    #     follow_task_ids_if_true=['task_get_bridgeweave_data'],
    #     follow_task_ids_if_false=['end'],
    #     weekday=WeekDay.WEDNESDAY,
    #     use_task_execution_day=True
    # )

    # end = DummyOperator(
    #     task_id='end'
    # )


    # Brideweave config

    # trxn_table_name='mf_sales_transaction_fact_11_JANtill_16FEB'
    trxn_table_name='mf_sales_transaction_fact_15Martill_5April'
    customer_table_name='FOLIO_CUSTOMER_DIM_feb_anony'
    # max_date='2022-01-31'
    max_date='2022-03-31'#31 march
    month='mar'
    old_month='feb'



    create_table >> truncate_table >> insert_data >> task_get_bridgeweave_data >> export_bridgeweave_data
