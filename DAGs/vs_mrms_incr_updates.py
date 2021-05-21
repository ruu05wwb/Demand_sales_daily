import cx_Oracle
import pandas as pd
import requests
from datetime import datetime, timedelta

#from airflow.operators import BaseOperator
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from typing import Union
from airflow.providers.telegram.operators.telegram import TelegramOperator
start_time = datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
magic_conn_params = BaseHook.get_connection('magic_dataguard')
gdw_conn_params = BaseHook.get_connection('gdwprod_analyst_ruu05wwb')
written_rows = {}
def months_to_reload(magic_conn):
    start_time = datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
    db_curr_dt = pd.read_sql_query('SELECT SYSDATE AS MRMS_SYSDATE FROM DUAL', magic_conn)
    curr_dt = db_curr_dt.loc[0, 'MRMS_SYSDATE']
    curr_ym=curr_dt.year*100+curr_dt.month
    months_to_reload_sql = requests.get('https://raw.githubusercontent.com/ruu05wwb/Demand_sales_daily/main/ORACLE/MRMS_INGESTION/MRMS_MONTHS_TO_RELOAD.sql').text
    months_to_reload_sql = months_to_reload_sql.replace('{SYS_YM}', str(curr_dt.year*10000+curr_dt.month*100+curr_dt.day))
    months_to_reload = pd.read_sql_query(months_to_reload_sql, magic_conn)
    if curr_ym%100==1:
       ym_offset=88
    else:
       ym_offset=0
    months_to_reload.sort_values(['ORD_MO_YR_ID'], ascending=False, inplace=True)
    for ind, col in months_to_reload['ORD_MO_YR_ID'].iteritems():
        if months_to_reload['ORD_MO_YR_ID'][ind]<curr_ym-1-ym_offset and curr_dt.hour not in (22, 23):
            months_to_reload.drop([ind], inplace=True)
    return months_to_reload

def tg_alert(message):
    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        token='1720321654:AAE_7rs00A4k0PFr1tHdqlIgPwPIj3SgolU',
        chat_id='-1001200916725',
        text = message,
        dag = dag
    )
    return send_message_telegram_task.execute()


def mrms_read_gdw_write(yearmonth, magic_conn, gdw_conn):
    sql = requests.get(
        'https://raw.githubusercontent.com/ruu05wwb/Demand_sales_daily/main/ORACLE/MRMS_INGESTION/MRMS_INVOICES_STG.sql').text
    sql = sql.replace('{FOCUS_YM}', yearmonth)
    df_stg = pd.read_sql_query(sql, magic_conn)
    cursor = gdw_conn.cursor()
    cursor.execute('''CALL DWSEAI01.EAIDMP0003_EXEC('TRUNCATE TABLE DWSEAI01.MRMS_INVOICES_STG')''')
    cursor.executemany(
        'INSERT /* +APPEND */ INTO DWSEAI01.MRMS_INVOICES_STG values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)',
        df_stg.values.tolist(),
        batcherrors=True)
    written_rows[yearmonth] = cursor.rowcount
    add_partition_if_not_exists_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/Demand_sales_daily/main/ORACLE/DDLs/ADD_PARTITION_IF_NOT_EXISTS_template.sql').text
    add_partition_if_not_exists = add_partition_if_not_exists_gh.replace('{YEARMONTH}', yearmonth).replace('{TARGET_TABLE}', 'MRMS_INVOICES').replace('{PART_NM_TEMPL}', 'MRMS_INVOICES')
    cursor.execute(add_partition_if_not_exists)
    cursor.execute('ALTER TABLE DWSEAI01.MRMS_INVOICES EXCHANGE PARTITION MRMS_INVOICES_{} WITH TABLE DWSEAI01.MRMS_INVOICES_STG WITHOUT VALIDATION'.format(yearmonth))
    

def send_mes():
    end_time = datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
    tg_mes = '''Updated {} month(s) in DWSEAI01.MRMS_INVOICES from Magic Bonus. \n
    Count of rows written: \n\n'''.format(str(len(written_rows)))
    for rec in written_rows:
        tg_mes = tg_mes + str(rec)+': '+str(written_rows[rec])+'\n'
    tg_mes = tg_mes + '\nStart time (Airflow server, UTC): '+ str(start_time)+'\n'
    tg_mes = tg_mes + 'End time (Airflow server, UTC): '+ str(end_time)
    tg_alert(tg_mes)

def reload_iter(execution_date, **kwargs):
    magic_conn = cx_Oracle.connect(magic_conn_params.login, magic_conn_params.password, magic_conn_params.host, encoding="UTF-8")
    focus_months = months_to_reload(magic_conn)
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    for ind, col in focus_months['ORD_MO_YR_ID'].iteritems():
        mrms_read_gdw_write(str(focus_months['ORD_MO_YR_ID'][ind]), magic_conn, gdw_conn)
    gdw_conn.close()
    magic_conn.close()
    send_mes()

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['vasiliy_smirnov@amway.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=15)
}

with DAG('vs_mrms_incr_updates.py',
         description='Incremental updates of the GDW table from MRMS',
         schedule_interval='0 7,3/6 * * *',
         default_args=DEFAULT_ARGS,
         start_date=datetime(2021, 4, 3),
         catchup=False) as dag:

    RELOAD = PythonOperator(task_id='reload_iter', python_callable=reload_iter, provide_context=True, dag=dag)    

RELOAD
