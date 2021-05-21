import cx_Oracle
import pandas as pd

import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import telebot
from pandas.plotting import table
#from airflow.operators import BaseOperator
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from typing import Union
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

start_time = datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
gdw_conn_params = BaseHook.get_connection('gdwprod_analyst_ruu05wwb')
written_rows = {}
max_months_to_reload_at_one_run=2 #in addition to the current and previous months which are reloaded every time

def ds_pending_months(execution_date, **kwargs):
    start_time = datetime.now().strftime("%d-%m-%Y, %H:%M:%S")
    ds_pending_months_sql = requests.get('https://raw.githubusercontent.com/ruu05wwb/Demand_sales_daily/main/ORACLE/DS_PENDING_MONTHS.sql').text
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    cursor = gdw_conn.cursor()
    cursor.execute(ds_pending_months_sql)
    gdw_conn.close()

def mview_refresh(**kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='CALL DWSEAI01.EAIDMP0003_EXEC(\'BEGIN DBMS_MVIEW.REFRESH(list=>\'\'DS_PEND_M_SRC\'\'); END;\')'
    cursor = gdw_conn.cursor()
    cursor.execute(sql)
    gdw_conn.close()    

#total amount of months to reload (maximum offset (month_rank) at that run)
def put_to_xcom(**kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    db_curr_hr = pd.read_sql_query('SELECT EXTRACT(HOUR FROM FROM_TZ(CAST(SYSTIMESTAMP AS TIMESTAMP), \'UTC\')) AS GDW_SYSDATE_HR FROM DUAL', gdw_conn)
    curr_hr = db_curr_hr.loc[0, 'GDW_SYSDATE_HR']
    sql='SELECT MAX(MNTH_RNK) AS MNTHS_CNT FROM DWSEAI01.DS_PEND_M_SRC'
    cursor = gdw_conn.cursor()
    months_cnt_df = pd.read_sql_query(sql, gdw_conn)
    if curr_hr in (22, 23):
        req_months_cnt=months_cnt_df.loc[0, 'MNTHS_CNT']
    else:
        req_months_cnt=min(3, months_cnt_df.loc[0, 'MNTHS_CNT'])
    kwargs['ti'].xcom_push(key='req_months_cnt', value=req_months_cnt)
    kwargs['ti'].xcom_push(key='gdw_run_hr', value=curr_hr)
    gdw_conn.close()

def tg_image(img):
   bot = telebot.TeleBot('1720321654:AAE_7rs00A4k0PFr1tHdqlIgPwPIj3SgolU')
   chat_id = -1001200916725
   img_o=open(img, 'rb')
   bot.send_photo(chat_id, img_o)

def tg_alert(**kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='''
    SELECT SRC, NVL(UPDT.\"Updated\", \'-\') AS \"Updated\", NVL(PEND.\"Deferred to next run\", \'-\') AS \"Deferred to next run(s)\" FROM 
(SELECT SRC,
  LISTAGG(ORD_MO_YR_ID, \', \') WITHIN GROUP(
ORDER BY ORD_MO_YR_ID DESC) AS \"Updated\"
FROM (SELECT SRC, ORD_MO_YR_ID, UPDT_ON_LAST_RUN_FLG, MAX(UPDT_ON_LAST_RUN_FLG) OVER(PARTITION BY SRC) AS MAX_UPDT_ON_LAST_RUN_FLG
FROM DWSEAI01.DS_PENDING_MONTHS)
WHERE UPDT_ON_LAST_RUN_FLG=1 AND UPDT_ON_LAST_RUN_FLG<>0
GROUP BY SRC
ORDER BY SRC) UPDT
FULL OUTER JOIN
(SELECT SRC,
  LISTAGG(ORD_MO_YR_ID, \', \') WITHIN GROUP(
ORDER BY ORD_MO_YR_ID DESC) AS \"Deferred to next run\"
FROM DWSEAI01.DS_PENDING_MONTHS
WHERE UPDT_ON_LAST_RUN_FLG =0
GROUP BY SRC
ORDER BY SRC) PEND
USING(SRC)
'''
    df = pd.read_sql_query(sql, gdw_conn)
    plt.subplots_adjust(left=0.8, right=1.6, top=0.2, bottom=0.1)
    ax = plt.subplot(111, frame_on=False)  # no visible frame
    ax.xaxis.set_visible(False)  # hide the x axis
    ax.yaxis.set_visible(False)  # hide the y axis
    blankIndex = [''] * len(df)
    df.index = blankIndex
    table(ax, df, rowLabels=['']*df.shape[0], loc='center')
    plt.savefig('/home/airflow/dags/results.png', bbox_inches = 'tight', orientation='landscape')
    tg_image('/home/airflow/dags/results.png')
    gdw_conn.close()

def select_next_task(i, **op_kwargs):
   if i<=int(op_kwargs['ti'].xcom_pull(task_ids='put_to_xcom', key='req_months_cnt')):
       return 'trunc_temp_tbl_'+str(i)
   else:
       return 'tg'    

def as400_updates(month_rank, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='''
    SELECT * FROM DWSEAI01.DS_PEND_M_SRC
    WHERE MNTH_RNK='''+str(month_rank)
    cursor = gdw_conn.cursor()
    target_month = pd.read_sql_query(sql, gdw_conn)
    ex_sql='CALL RUU05WWB.DS_DET_DAILY_AS400('+str(target_month.loc[0, 'EU_AS400'])+', '+str(target_month.loc[0, 'ORD_MO_YR_ID']) +','+str(month_rank)+')'
    cursor.execute(ex_sql)
    pend_m_updt_sql='UPDATE DWSEAI01.DS_PENDING_MONTHS SET UPDT_ON_LAST_RUN_FLG=1 WHERE ORD_MO_YR_ID='+str(target_month.loc[0, 'EU_AS400'])+' AND SRC=\'EU_AS400\''
    cursor.execute(pend_m_updt_sql)
    gdw_conn.commit() 
    gdw_conn.close()

def atlas_updates(month_rank, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='''
    SELECT * FROM DWSEAI01.DS_PEND_M_SRC
    WHERE CASE WHEN MNTH_RNK<=2 THEN 0 ELSE UPDT_ON_LAST_RUN_FLG END =0 AND MNTH_RNK='''+str(month_rank)
    cursor = gdw_conn.cursor()
    target_month = pd.read_sql_query(sql, gdw_conn)
    ex_sql='CALL RUU05WWB.DS_DET_DAILY_ATLAS('+str(target_month.loc[0, 'EU_ATLAS'])+', '+str(target_month.loc[0, 'ORD_MO_YR_ID']) +', '+str(month_rank)+')'
    cursor.execute(ex_sql)
    pend_m_updt_sql='UPDATE DWSEAI01.DS_PENDING_MONTHS SET UPDT_ON_LAST_RUN_FLG=1 WHERE ORD_MO_YR_ID='+str(target_month.loc[0, 'EU_ATLAS'])+' AND SRC=\'EU_ATLAS\''
    cursor.execute(pend_m_updt_sql)
    gdw_conn.commit() 
    gdw_conn.close()

def hybr_eom_updates(month_rank, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='''
    SELECT * FROM DWSEAI01.DS_PEND_M_SRC
    WHERE MNTH_RNK='''+str(month_rank)
    cursor = gdw_conn.cursor()
    target_month = pd.read_sql_query(sql, gdw_conn)
    ex_sql='CALL RUU05WWB.DS_DET_DAILY_HYBR_EOM('+str(target_month.loc[0, 'EU_HYBR_EOM'])+', '+str(target_month.loc[0, 'ORD_MO_YR_ID']) +', '+str(month_rank)+')'
    cursor.execute(ex_sql)
    pend_m_updt_sql='UPDATE DWSEAI01.DS_PENDING_MONTHS SET UPDT_ON_LAST_RUN_FLG=1 WHERE ORD_MO_YR_ID='+str(target_month.loc[0, 'EU_HYBR_EOM'])+' AND SRC=\'EU_HYBR_EOM\''
    cursor.execute(pend_m_updt_sql)
    gdw_conn.commit()     
    gdw_conn.close()  

def hybr_non_eom_updates(month_rank, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='''
    SELECT * FROM DWSEAI01.DS_PEND_M_SRC
    WHERE MNTH_RNK='''+str(month_rank)
    cursor = gdw_conn.cursor()
    target_month = pd.read_sql_query(sql, gdw_conn)
    ex_sql='CALL RUU05WWB.DS_DET_DAILY_HYBR_NON_EOM('+str(target_month.loc[0, 'EU_HYBR_NON_EOM'])+', '+str(target_month.loc[0, 'ORD_MO_YR_ID']) +', '+str(month_rank)+')'
    cursor.execute(ex_sql)
    pend_m_updt_sql='UPDATE DWSEAI01.DS_PENDING_MONTHS SET UPDT_ON_LAST_RUN_FLG=1 WHERE ORD_MO_YR_ID='+str(target_month.loc[0, 'EU_HYBR_NON_EOM'])+' AND SRC=\'EU_HYBR_NON_EOM\''
    cursor.execute(pend_m_updt_sql)
    gdw_conn.commit()       
    gdw_conn.close()   

def exch_part(month_rank, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='''
    SELECT * FROM DWSEAI01.DS_PEND_M_SRC
    WHERE MNTH_RNK='''+str(month_rank)
    cursor = gdw_conn.cursor()
    target_month = pd.read_sql_query(sql, gdw_conn)
    add_partition_if_not_exists_gh = requests.get('https://raw.githubusercontent.com/ruu05wwb/Demand_sales_daily/main/ORACLE/DDLs/ADD_PARTITION_IF_NOT_EXISTS_template.sql').text
    add_partition_if_not_exists = add_partition_if_not_exists_gh.replace('{YEARMONTH}', str(target_month.loc[0, 'ORD_MO_YR_ID'])).replace('{TARGET_TABLE}', 'DS_DET_DAILY').replace('{PART_NM_TEMPL}', 'DS_DET_DAILY')
    cursor.execute(add_partition_if_not_exists)
    if month_rank%2==0:
        stg_table='DWSEAI01.DS_DET_DAILY_EVEN_STG'
    else:
        stg_table='DWSEAI01.DS_DET_DAILY_ODD_STG'
    ex_sql='ALTER TABLE DWSEAI01.DS_DET_DAILY EXCHANGE PARTITION DS_DET_DAILY_'+str(target_month.loc[0, 'ORD_MO_YR_ID'])+' WITH TABLE '+stg_table
    cursor.execute(ex_sql)
    gdw_conn.close()   

def trunc_temp_tbl(month_rank, **op_kwargs):
    gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
    sql='''
    SELECT * FROM DWSEAI01.DS_PEND_M_SRC
    WHERE MNTH_RNK='''+str(month_rank)
    cursor = gdw_conn.cursor()
    target_month = pd.read_sql_query(sql, gdw_conn)
    if month_rank%2==0:
        stg_table='DWSEAI01.DS_DET_DAILY_EVEN_STG'
    else:
        stg_table='DWSEAI01.DS_DET_DAILY_ODD_STG'
    ex_sql='''
    CALL DWSEAI01.EAIDMP0003_EXEC('TRUNCATE TABLE 
    '''+stg_table+ '''    ')
    '''
    cursor.execute(ex_sql)
    gdw_conn.close()      

def updt_pend_months(**op_kwargs):
    if int(op_kwargs['ti'].xcom_pull(task_ids='put_to_xcom', key='gdw_run_hr')) in (22, 23):
        gdw_conn = cx_Oracle.connect(gdw_conn_params.login, gdw_conn_params.password, gdw_conn_params.host, encoding="UTF-8")
        cursor = gdw_conn.cursor()
        pend_m_updt_sql='DELETE FROM DWSEAI01.DS_PENDING_MONTHS WHERE UPDT_ON_LAST_RUN_FLG>0 AND ORD_MO_YR_ID NOT IN (SELECT DISTINCT ORD_MO_YR_ID FROM DWSEAI01.DS_PEND_M_SRC WHERE MNTH_RNK IN (1, 2))'
        cursor.execute(pend_m_updt_sql)
        gdw_conn.commit() 
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

with DAG('vs_ds_det_daily_incr_updt',
         description='Incremental updates of the GDW table from MRMS',
         schedule_interval='30 7,3/6 * * *',
         default_args=DEFAULT_ARGS,
         start_date=datetime(2021, 5, 2),
         catchup=False) as dag:
    MNTHS_LIST_GEN = PythonOperator(task_id='mnths_list_gen', python_callable=ds_pending_months, provide_context=True, dag=dag)
    MVIEW_REFRESH=PythonOperator(task_id='refresh_list_of_pending_months', python_callable=mview_refresh, provide_context=True, dag=dag)
    PUT_XCOM = PythonOperator(task_id='put_to_xcom', python_callable=put_to_xcom, provide_context=True, dag=dag)
    trunc=[]
    as400=[]
    hybr_eom=[]
    hybr_non_eom=[]
    atlas=[]
    exch=[]

    for i in range(1,max_months_to_reload_at_one_run+3):    
        trunc.append(PythonOperator(task_id='trunc_temp_tbl_'+str(i), python_callable=trunc_temp_tbl, provide_context=True, dag=dag, op_kwargs={'month_rank': i}),)
        as400.append(PythonOperator(task_id='as400_'+str(i), python_callable=as400_updates, provide_context=True, dag=dag, op_kwargs={'month_rank': i}),)
        atlas.append(PythonOperator(task_id='atlas_'+str(i), python_callable=atlas_updates, provide_context=True, dag=dag, op_kwargs={'month_rank': i}),)
        hybr_eom.append(PythonOperator(task_id='hybr_eom_'+str(i), python_callable=hybr_eom_updates, provide_context=True, dag=dag, op_kwargs={'month_rank': i}),)
        hybr_non_eom.append(PythonOperator(task_id='hybr_non_eom_'+str(i), python_callable=hybr_non_eom_updates, provide_context=True, dag=dag, op_kwargs={'month_rank': i}),)
        exch.append(PythonOperator(task_id='exch_part_'+str(i), python_callable=exch_part, provide_context=True, dag=dag, op_kwargs={'month_rank': i}),)

    TG_END=PythonOperator(task_id='tg', python_callable=tg_alert, provide_context=True, dag=dag, trigger_rule=TriggerRule.NONE_FAILED)
    UPDT_PENDING_MONTHS_LIST=PythonOperator(task_id='updt_pend_months', python_callable=updt_pend_months, provide_context=True, dag=dag)

next_mnth_sel=[]

for i in range(3, max_months_to_reload_at_one_run+3):
    next_mnth_sel.append(BranchPythonOperator(task_id=str(i)+'_months_ago_select', python_callable=select_next_task, provide_context=True, dag=dag, op_kwargs={'i': i}))

MNTHS_LIST_GEN>>MVIEW_REFRESH>>PUT_XCOM>>trunc[0]>>trunc[1]
for i in range(0, 2): #for 1st 2 months load - don't increase
    trunc[1].set_downstream(as400[i])
    trunc[1].set_downstream(atlas[i])
    trunc[1].set_downstream(hybr_eom[i])
    trunc[1].set_downstream(hybr_non_eom[i])
    exch[0].set_upstream(as400[i])
    exch[0].set_upstream(atlas[i])
    exch[0].set_upstream(hybr_eom[i])
    exch[0].set_upstream(hybr_non_eom[i])
exch[0]>>exch[1]
for i in range(0, max_months_to_reload_at_one_run):
    exch[i+1]>>next_mnth_sel[i]
    next_mnth_sel[i].set_downstream(TG_END)
    next_mnth_sel[i].set_downstream(trunc[i+2])
    trunc[i+2].set_downstream(as400[i+2])
    trunc[i+2].set_downstream(atlas[i+2])
    trunc[i+2].set_downstream(hybr_eom[i+2])
    trunc[i+2].set_downstream(hybr_non_eom[i+2])
    exch[i+2].set_upstream(as400[i+2])
    exch[i+2].set_upstream(atlas[i+2])
    exch[i+2].set_upstream(hybr_eom[i+2])
    exch[i+2].set_upstream(hybr_non_eom[i+2])

exch[max_months_to_reload_at_one_run+1]>>TG_END>>UPDT_PENDING_MONTHS_LIST
