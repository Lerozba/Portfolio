import airflow.utils.dates
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow_clickhouse_plugin.sensors.clickhouse_sql_sensor import ClickHouseSqlSensor

from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import PythonOperator
import boto3

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 5, 31),
    'email': ['example@mail.ru'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='clickhouse ETL',
    default_args=default_args,
    description='dag_for_diplom',
    schedule_interval='@daily',
    tags=['***'],
)

# сенсор, который будет проверять наличие нового файла
wait_file=ClickHouseSqlSensor(
    task_id='wait_file',
    database='CitiBike',
    sql="SELECT count(ride_id) \
        FROM s3('https://storage.yandexcloud.net/citibike-input/{{ yesterday_ds_nodash }}-citibike-tripdata.csv',
    clickhouse_conn_id='storage-citibike-input',
    success=lambda cnt: cnt > 0,
    dag=dag
)


# обновляем основную таблицу
update_citibike_all=ClickHouseOperator(
    task_id='update_citibike_all',
    database='CitiBike',
    sql=
        '''
            INSERT INTO CitiBike.citibike_all (
                ride_id, rideable_type, started_at, ended_at, start_station_name, start_station_id,
                end_station_name, end_station_id, start_lat, start_lng, end_lat, end_lng, 
                member_casual)
            SELECT ride_id, rideable_type, started_at, ended_at, start_station_name, 
                start_station_id, end_station_name, end_station_id, start_lat, start_lng, end_lat, 
                end_lng, member_casual
            FROM s3('https://storage.yandexcloud.net/citibike-input/{{ yesterday_ds_nodash }}\-citibike-tripdata.csv',
                'CSVWithNames',
                'ride_id String, rideable_type String, started_at DateTime, ended_at DateTime, 
                start_station_name String, start_station_id String, end_station_name String, 
                end_station_id String, start_lat String, start_lng String, end_lat String, 
                end_lng String, member_casual String')
        ''',
    clickhouse_conn_id='clickhouse_CitiBike',
    dag=dag,
)

# удаляем файлы-источники
def del_source_files():
    s3=boto3.resource('s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id='YC*ZP',
        aws_secret_access_key='YC*-***sw')
    bucket = s3.Bucket('citibike-input')
    bucket.object_versions.delete()

delete_source_files=PythonOperator(
    task_id='delete_source_files',
    python_callable=del_source_files,
    dag=dag,
)

# обновляем отчетные таблицы
update_daily_count_ride=ClickHouseOperator(
    task_id='update_daily_count_ride',
    database='CitiBike',
    sql=
        '''
            INSERT INTO CitiBike.daily_count_ride (date_ride, count_ride)
            SELECT a.started_at::Date, count(a.ride_id)
            FROM CitiBike.citibike_all as a
            GROUP BY a.started_at::Date
            WHERE a.started_at::Date = '{{ (data_interval_start – timedelta(days=1)) }}'::Timestamp::Date
        ''',
    clickhouse_conn_id='clickhouse_CitiBike',
    dag=dag,
)

update_daily_average_duration_ride=ClickHouseOperator(
    task_id='update_daily_average_duration_ride',
    database='CitiBike',
    sql=
        '''
            INSERT INTO CitiBike.daily_average_duration_ride (date_ride, average_duration_ride)
            SELECT a.started_at::Date, Round((AVG(a.ended_at-a.started_at))/60, 0)
            FROM CitiBike.citibike_all as a
            GROUP BY a.started_at::Date
            WHERE a.started_at::Date = '{{ (data_interval_start – timedelta(days=1)) }}'::Timestamp::Date
        ''',
    clickhouse_conn_id='clickhouse_CitiBike',
    dag=dag,
)

update_daily_diff_members=ClickHouseOperator(
    task_id='update_daily_diff_members',
    database='CitiBike',
    sql=
        '''
            INSERT INTO CitiBike.daily_diff_members (
                date_ride, 
                member_casual, 
                count_ride, 
                average_duration_ride
            )
            SELECT a.started_at::Date, a.member_casual, count(a.ride_id), 
                   Round((AVG(a.ended_at-a.started_at))/60, 0)
            FROM CitiBike.citibike_all as a
            GROUP BY a.started_at::Date
            WHERE a.started_at::Date = '{{ (data_interval_start – timedelta(days=1)) }}'::Timestamp::Date
        ''',
    clickhouse_conn_id='clickhouse_CitiBike',
    dag=dag,
)

# удалим предыдущие варианты файлов отчетов из бакета
def clear_bucket():
    s3=boto3.resource('s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id='YC*ZP',
        aws_secret_access_key='YC*-***sw')
    objects_to_delete = [{'Key': 'daily_count_ride.csv'}, {'Key': 'daily_average_duration_ride.csv'}, {'Key': 'daily_diff_members.csv'}]
    s3.delete_objects(Bucket='citibike-daily-report', Delete={'Objects': objects_to_delete})

delete_old_reports=PythonOperator(
    task_id='delete_old_reports',
    python_callable=clear_bucket,
    dag=dag,
)

# в пустой бакет сохраняем файлы отчетов 
daily_count_ride_to_backet=ClickHouseOperator(
    task_id='daily_count_ride_to_backet',
    database='CitiBike',
    sql=
        '''
            INSERT INTO FUNCTION 
                s3('https://storage.yandexcloud.net/citibike-daily-report/daily_count_ride.csv', 
                'CSV', 
                'date_ride Date, count_ride Int32') 
            SELECT date_ride, count_ride 
            FROM CitiBike.daily_count_ride'
        ''',
    clickhouse_conn_id='clickhouse_CitiBike',
    dag=dag,
)

daily_average_duration_ride_to_backet=ClickHouseOperator(
    task_id='daily_average_duration_ride_to_backet',
    database='CitiBike',
    sql=
        '''
            INSERT INTO FUNCTION 
                s3('https://storage.yandexcloud.net/citibike-daily-report/daily_average_duration_ride.csv', 
                'CSV', 
                'date_ride Date, average_duration_ride Float32') 
            SELECT date_ride, average_duration_ride 
            FROM CitiBike.daily_ average_duration_ride
        ''',
    clickhouse_conn_id='clickhouse_CitiBike',
    dag=dag,
)

daily_diff_members_to_backet=ClickHouseOperator(
    task_id='daily_diff_members_to_backet',
    database='CitiBike',
    sql=
        '''
            INSERT INTO FUNCTION 
                s3('https://storage.yandexcloud.net/citibike-daily-report/daily_diff_members.csv', 
                'CSV', 
                'date_ride Date, member_casual String, count_ride Int32, average_duration_ride Float32') 
            SELECT date_ride, member_casual, count_ride, average_duration_ride 
            FROM CitiBike.daily_diff_members
        ''',
    clickhouse_conn_id='clickhouse_CitiBike',
    dag=dag,
)

# отправляем отчет на почту
email_success=EmailOperator(
    task_id='email_success', 
    to='example@mail.ru',
    subject='reports updated',
    html_content=dedent("""Все отчеты обновлены"""),
    trigger_rule=TriggerRule.all_success,
    dag=dag,
)

email_fail=EmailOperator(
    task_id='email_fail', 
    to='example@mail.ru',
    subject='reports not updated',
    html_content=dedent("""Что-то сломалось"""),
    trigger_rule=TriggerRule.one_failed,
    dag=dag,
)

wait_file >> update_citibike_all >> delete_source_files >> [update_daily_count_ride, update_daily_average_duration_ride, update_daily_diff_members]
[update_daily_count_ride, update_daily_average_duration_ride, update_daily_diff_members] >> delete_old_reports
delete_old_reports >> [daily_count_ride_to_backet, daily_average_duration_ride_to_backet, daily_diff_members_to_backet]
[daily_count_ride_to_backet, daily_average_duration_ride_to_backet, daily_diff_members_to_backet] >> email_success
[daily_count_ride_to_backet, daily_average_duration_ride_to_backet, daily_diff_members_to_backet] >> email_fail



