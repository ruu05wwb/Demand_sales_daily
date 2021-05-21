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

gdw_conn_params = BaseHook.get_connection('gdwprod_analyst_ruu05wwb')

def curr_ym_months(**kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    db_curr_dt = pd.read_sql_query('SELECT SYSDATE AS GDW_SYSDATE FROM DUAL', gdw_conn)
    curr_dt = db_curr_dt.loc[0, 'GDW_SYSDATE']
    curr_ymm=curr_dt.year*12+curr_dt.month
    kwargs['ti'].xcom_push(key='curr_ym', value=curr_ymm) #!!!!!!!!!!!Change back to value=curr_ymm after 201802-04 reload!!!!!!!!!!!
    gdw_conn.close()

def tg_alert(message, **op_kwargs):
    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        token='1720321654:AAE_7rs00A4k0PFr1tHdqlIgPwPIj3SgolU',
        chat_id='-1001200916725',
        text = message,
        dag = dag
    )
    return send_message_telegram_task.execute()

def oms_temp_rms(ymm_offset, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    target_ymm = op_kwargs['ti'].xcom_pull(task_ids='curr_ym', key='curr_ym')-ymm_offset
    add_partition_if_not_exists_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/EIA_OMS_DM/main/COMMON_DDLs/OMS_ADD_PARTITION_IF_NOT_EXISTS_template.sql').text
    add_partition_if_not_exists = add_partition_if_not_exists_gh.replace('{YMM}', str(target_ymm)).replace('{TARGET_TABLE}', 'OMS_TEMP_RMS_PART').replace('{PART_NM_TEMPL}', 'OMS_TEMP')
    cursor = gdw_conn.cursor()
    cursor.execute(add_partition_if_not_exists)
    trunc_stg_sql='''
    CALL DWSEAI01.EAIDMP0003_EXEC('TRUNCATE TABLE 
    '''+'OMS_TEMP_RMS_STG'+ '''    ')
    '''
    cursor.execute(trunc_stg_sql)
    load_data_sql_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/EIA_OMS_DM/main/OMS/Current_data_flow_after_1st_year_NEW.sql').text
    load_data_sql = load_data_sql_gh.replace('{YMM}', str(target_ymm))
    cursor.execute(load_data_sql)
    compress_sql = '''
    CALL DWSEAI01.DWP90005_COMPRESS('OMS_TEMP_RMS_STG')
    '''
    cursor.execute(compress_sql)
    gdw_conn.close()


def exch_part(ymm_offset, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    target_ymm = op_kwargs['ti'].xcom_pull(task_ids='curr_ym', key='curr_ym')-ymm_offset
    ymm2ym_sql = 'SELECT DWSEAI01.YMM2YM('+str(target_ymm)+') AS YM FROM DUAL'
    target_ym = pd.read_sql_query(ymm2ym_sql, gdw_conn)
    ex_sql='ALTER TABLE DWSEAI01.OMS_TEMP_RMS_PART EXCHANGE PARTITION OMS_TEMP_'+str(target_ym.loc[0, 'YM'])+' WITH TABLE DWSEAI01.OMS_TEMP_RMS_STG WITHOUT VALIDATION'
    cursor = gdw_conn.cursor()
    cursor.execute(ex_sql)
    gdw_conn.close()   

def part_stats(ymm_offset, target_table, target_part_template, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    target_ymm = op_kwargs['ti'].xcom_pull(task_ids='curr_ym', key='curr_ym')-ymm_offset
    ymm2ym_sql = 'SELECT DWSEAI01.YMM2YM('+str(target_ymm)+') AS YM FROM DUAL'
    target_ym = pd.read_sql_query(ymm2ym_sql, gdw_conn)
    ex_sql='CALL DWSEAI01.DWP90007_GATHER_PART_STATS(\''+target_table+ '\',\''+ target_part_template + '_'+ str(target_ym.loc[0, 'YM']) + '\')'
    cursor = gdw_conn.cursor()
    cursor.execute(ex_sql)
    gdw_conn.close()   

def mview_refresh(**op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='CALL DWSEAI01.EAIDMP0003_EXEC(\'BEGIN DBMS_MVIEW.REFRESH(list=>\'\'DWSEAI01.OMS_MV\'\', method=>\'\'P\'\'); END;\')'
    cursor = gdw_conn.cursor()
    cursor.execute(sql)
    gdw_conn.close()       


def oms_mktg_temp(ymm_offset, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    target_ymm = op_kwargs['ti'].xcom_pull(task_ids='curr_ym', key='curr_ym')-ymm_offset
    add_partition_if_not_exists_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/EIA_OMS_DM/main/COMMON_DDLs/OMS_ADD_PARTITION_IF_NOT_EXISTS_template.sql').text
    add_partition_if_not_exists = add_partition_if_not_exists_gh.replace('{YMM}', str(target_ymm)).replace('{TARGET_TABLE}', 'OMS_MKTG_TEMP').replace('{PART_NM_TEMPL}', 'OMS_MKTG_TEMP')
    cursor = gdw_conn.cursor()
    cursor.execute(add_partition_if_not_exists)
    ymm2ym_sql = 'SELECT DWSEAI01.YMM2YM('+str(target_ymm)+') AS YM FROM DUAL'
    target_ym = pd.read_sql_query(ymm2ym_sql, gdw_conn)
    trunc_part_sql='CALL DWSEAI01.EAIDMP0003_EXEC(\'ALTER TABLE DWSEAI01.OMS_MKTG_TEMP TRUNCATE PARTITION OMS_MKTG_TEMP_{}\')'.format(str(target_ym.loc[0, 'YM']))
    cursor.execute(trunc_part_sql)
    load_data_sql_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/EIA_OMS_DM/main/OMS_MKTG/OMS_MKTG_TEMP_NEW.sql').text
    load_data_sql = load_data_sql_gh.replace('{YMM}', str(target_ymm))
    cursor.execute(load_data_sql)
    compress_sql = 'CALL DWSEAI01.EAIDMP0003_EXEC(\'BEGIN FOR REC IN (SELECT DISTINCT \'\'ALTER TABLE OMS_MKTG_TEMP MOVE SUBPARTITION \'\'||TAB_SUBPARTS.SUBPARTITION_NAME||\'\' PARALLEL 16 COMPRESS FOR QUERY HIGH\'\' AS SQL_CMD FROM USER_TAB_SUBPARTITIONS TAB_SUBPARTS WHERE TABLE_NAME=\'\'OMS_MKTG_TEMP\'\' AND PARTITION_NAME=\'\'OMS_MKTG_TEMP_{}\'\') LOOP EXECUTE IMMEDIATE REC.SQL_CMD; END LOOP; END;\')'.format(str(target_ym.loc[0, 'YM']))
    cursor.execute(compress_sql)
    gdw_conn.close()

def oms_mktg(ymm_offset, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    target_ymm = op_kwargs['ti'].xcom_pull(task_ids='curr_ym', key='curr_ym')-ymm_offset
    cursor = gdw_conn.cursor()
    load_data_sql_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/EIA_OMS_DM/main/OMS_MKTG/OMS_MKTG.sql').text
    load_data_sql = load_data_sql_gh.replace('{YMM}', str(target_ymm))
    cursor.execute(load_data_sql)
    compress_sql = '''
    CALL DWSEAI01.DWP90005_COMPRESS('OMS_MKTG')
    '''
    cursor.execute(compress_sql)
    stats_sql = '''
    CALL DWSEAI01.DWP90004_GATHER_STATISTICS('OMS_MKTG')
    '''
    cursor.execute(stats_sql)
    gdw_conn.close()

def gpk_hist_gdw(ymm_offset, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    target_ymm = op_kwargs['ti'].xcom_pull(task_ids='curr_ym', key='curr_ym')-ymm_offset
    cursor = gdw_conn.cursor()
    load_data_sql_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/EIA_OMS_DM/main/OMS/GPK_HIST_GDW.sql').text
    load_data_sql = load_data_sql_gh.replace('{YMM}', str(target_ymm))
    cursor.execute(load_data_sql)
    compress_sql = '''
    CALL DWSEAI01.DWP90005_COMPRESS('GPK_HIST_GDW')
    '''
    cursor.execute(compress_sql)
    stats_sql = '''
    CALL DWSEAI01.DWP90004_GATHER_STATISTICS('GPK_HIST_GDW')
    '''
    cursor.execute(stats_sql)
    gdw_conn.close()


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['vasiliy_smirnov@amway.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
with DAG('vs_eu_monthly',
         description='Monthly refresh of EU datamart',
         schedule_interval='40 4 3,4,7,8 * *',
         default_args=DEFAULT_ARGS,
         start_date=datetime(2021, 4, 3),
         catchup=False) as dag:

    GET_YMM = PythonOperator(task_id='curr_ym', python_callable=curr_ym_months, provide_context=True, dag=dag)

    GPK_HIST_GDW_PPM = PythonOperator(task_id='gpk_hist_gdw_ppm', python_callable=gpk_hist_gdw, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 2})
    GPK_HIST_GDW_PM = PythonOperator(task_id='gpk_hist_gdw_pm', python_callable=gpk_hist_gdw, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1})

    OMS_TEMP_RMS_PPM = PythonOperator(task_id='oms_temp_rms_ppm', python_callable=oms_temp_rms, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 2})
    OMS_TEMP_EXCH_PART_PPM=PythonOperator(task_id='oms_temp_exch_part_ppm', python_callable=exch_part, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 2})
    OMS_TEMP_PART_STATS_PPM=PythonOperator(task_id='oms_temp_part_stats_ppm', python_callable=part_stats, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 2, 'target_table': 'OMS_TEMP_RMS_PART', 'target_part_template': 'OMS_TEMP'})

    OMS_TEMP_RMS_PM = PythonOperator(task_id='oms_temp_rms_pm', python_callable=oms_temp_rms, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1})
    OMS_TEMP_EXCH_PART_PM=PythonOperator(task_id='oms_temp_exch_part_pm', python_callable=exch_part, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1})
    OMS_TEMP_PART_STATS_PM=PythonOperator(task_id='oms_temp_part_stats_pm', python_callable=part_stats, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1, 'target_table': 'OMS_TEMP_RMS_PART', 'target_part_template': 'OMS_TEMP'})
    MVIEW_REFRESH_PPM=PythonOperator(task_id='mview_refresh_ppm', python_callable=mview_refresh, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1, 'target_table': 'OMS_TEMP_RMS_PART', 'target_part_template': 'OMS_TEMP'})
    MVIEW_REFRESH_PM=PythonOperator(task_id='mview_refresh_pm', python_callable=mview_refresh, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1, 'target_table': 'OMS_TEMP_RMS_PART', 'target_part_template': 'OMS_TEMP'})

    OMS_MKTG_TEMP_PPM = PythonOperator(task_id='oms_mktg_temp_ppm', python_callable=oms_mktg_temp, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 2})
    OMS_MKTG_TEMP_PM = PythonOperator(task_id='oms_mktg_temp_pm', python_callable=oms_mktg_temp, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1})
   
    OMS_MKTG_TEMP_PART_STATS_PPM=PythonOperator(task_id='oms_mktg_temp_part_stats_ppm', python_callable=part_stats, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 2, 'target_table': 'OMS_MKTG_TEMP', 'target_part_template': 'OMS_MKTG_TEMP'})
    OMS_MKTG_TEMP_PART_STATS_PM=PythonOperator(task_id='oms_mktg_temp_part_stats_pm', python_callable=part_stats, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1, 'target_table': 'OMS_MKTG_TEMP', 'target_part_template': 'OMS_MKTG_TEMP'})

    OMS_MKTG_PPM = PythonOperator(task_id='oms_mktg_ppm', python_callable=oms_mktg, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 2})
    OMS_MKTG_PM = PythonOperator(task_id='oms_mktg_pm', python_callable=oms_mktg, provide_context=True, dag=dag, op_kwargs={'ymm_offset': 1})
   
    TG_OMS_TEMP=PythonOperator(task_id='tg_oms_temp', python_callable=tg_alert, provide_context=True, dag=dag, op_kwargs={'message': 'OMS_TEMP_RMS_PART updated'})
    TG_ALL=PythonOperator(task_id='tg_all', python_callable=tg_alert, provide_context=True, dag=dag, op_kwargs={'message': 'All monthly OMS tables updated'})


GPK_HIST_GDW_PPM.set_upstream(GET_YMM)
GPK_HIST_GDW_PM.set_upstream(GPK_HIST_GDW_PPM)
OMS_TEMP_RMS_PPM.set_upstream(GPK_HIST_GDW_PM)
OMS_TEMP_EXCH_PART_PPM.set_upstream(OMS_TEMP_RMS_PPM)
OMS_TEMP_PART_STATS_PPM.set_upstream(OMS_TEMP_EXCH_PART_PPM)
MVIEW_REFRESH_PPM.set_upstream(OMS_TEMP_EXCH_PART_PPM)
OMS_MKTG_TEMP_PPM.set_upstream(OMS_TEMP_PART_STATS_PPM)
OMS_TEMP_RMS_PM.set_upstream([OMS_TEMP_PART_STATS_PPM, MVIEW_REFRESH_PPM])
OMS_TEMP_EXCH_PART_PM.set_upstream(OMS_TEMP_RMS_PM)
OMS_TEMP_PART_STATS_PM.set_upstream(OMS_TEMP_EXCH_PART_PM)
MVIEW_REFRESH_PM.set_upstream(OMS_TEMP_EXCH_PART_PM)
OMS_MKTG_TEMP_PART_STATS_PPM.set_upstream(OMS_MKTG_TEMP_PPM)
TG_OMS_TEMP.set_upstream(OMS_TEMP_PART_STATS_PM)
OMS_MKTG_TEMP_PM.set_upstream(OMS_TEMP_PART_STATS_PM)
OMS_MKTG_TEMP_PM.set_upstream(OMS_MKTG_TEMP_PART_STATS_PPM)
OMS_MKTG_TEMP_PART_STATS_PM.set_upstream(OMS_MKTG_TEMP_PM)
OMS_MKTG_PPM.set_upstream(OMS_MKTG_TEMP_PART_STATS_PPM)
OMS_MKTG_PM.set_upstream(OMS_MKTG_TEMP_PART_STATS_PM)
TG_ALL.set_upstream(OMS_MKTG_PM)
