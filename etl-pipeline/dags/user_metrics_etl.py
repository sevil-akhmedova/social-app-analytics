"""Ежедневный ETL пользовательских метрик ленты и мессенджера."""

import os
from datetime import datetime, timedelta

import pandas as pd
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Подключения к ClickHouse. Учётные данные задаются в окружении Airflow.
connection_test = {
    'host': os.getenv('CLICKHOUSE_HOST'),
    'user': os.getenv('CLICKHOUSE_WRITE_USER'),
    'password': os.getenv('CLICKHOUSE_WRITE_PASSWORD'),
    'database': 'test'
}

connection = {
    'host': os.getenv('CLICKHOUSE_HOST'),
    'user': os.getenv('CLICKHOUSE_READ_USER'),
    'password': os.getenv('CLICKHOUSE_READ_PASSWORD'),
    'database': 'simulator_20260620'
}


default_args = {
    'owner': 's.ahmedova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 5, 2),
}


@dag(
    dag_id='sevil_ahmedova_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)
def sevil_ahmedova_etl():

    # Метрики из feed_actions
    @task
    def feed_metrics():
        context = get_current_context()
        ds = context['ds']
        query_feed = f"""
            SELECT
                user_id,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes
            FROM simulator_20260620.feed_actions
            WHERE toDate(time) = '{ds}'
            GROUP BY user_id
        """
        return ph.read_clickhouse(query_feed, connection=connection)

    # Метрики из message_actions
    @task
    def message_metrics():
        context = get_current_context()
        ds = context['ds']
        query_sent = f"""
            SELECT
                user_id,
                count() AS messages_sent,
                uniq(receiver_id) AS users_sent
            FROM simulator_20260620.message_actions
            WHERE toDate(time) = '{ds}'
            GROUP BY user_id
        """
        df_sent = ph.read_clickhouse(query_sent, connection=connection)

        # Отдельно называем отправителя sender_id, чтобы не спутать его
        # с алиасом user_id для получателя.
        query_received = f"""
            SELECT
                receiver_id AS user_id,
                count() AS messages_received,
                uniq(sender_id) AS users_received
            FROM (
                SELECT
                    receiver_id,
                    user_id AS sender_id
                FROM simulator_20260620.message_actions
                WHERE toDate(time) = '{ds}'
            )
            GROUP BY receiver_id
        """
        df_received = ph.read_clickhouse(query_received, connection=connection)

        df_message = df_sent.merge(df_received, on='user_id', how='outer')
        return df_message.fillna(0)

    # Объединяем метрики ленты и мессенджера
    @task
    def merge_metrics(df_feed, df_message):
        df_total = df_feed.merge(df_message, on='user_id', how='outer')
        df_total = df_total.fillna(0)
        df_total['user_id'] = df_total['user_id'].astype('uint64')
        return df_total

    # Справочник пользователей из обеих таблиц
    @task
    def user_info():
        query_info = """
            SELECT DISTINCT
                user_id, gender, age, os
            FROM simulator_20260620.feed_actions
            UNION DISTINCT
            SELECT DISTINCT
                user_id, gender, age, os
            FROM simulator_20260620.message_actions
        """
        return ph.read_clickhouse(query_info, connection=connection)

    # Добавляем пол, возраст и операционную систему
    @task
    def add_user_info(df_total, df_info):
        return df_total.merge(df_info, on='user_id', how='left')

    # Вспомогательная функция для трёх срезов
    def get_slice(df, dimension_name):
        metrics = [
            'views', 'likes', 'messages_received',
            'messages_sent', 'users_received', 'users_sent'
        ]
        df_slice = df.groupby(dimension_name)[metrics].sum().reset_index()
        df_slice['dimension'] = dimension_name
        return df_slice.rename(columns={dimension_name: 'dimension_value'})

    @task
    def gender_slice(df_total_info):
        return get_slice(df_total_info, 'gender')

    @task
    def age_slice(df_total_info):
        return get_slice(df_total_info, 'age')

    @task
    def os_slice(df_total_info):
        return get_slice(df_total_info, 'os')

    # Собираем срезы в одну таблицу
    @task
    def concat_slices(df_gender, df_age, df_os):
        df_final = pd.concat([df_gender, df_age, df_os], ignore_index=True)
        context = get_current_context()
        df_final['event_date'] = context['ds']

        # В ClickHouse dimension_value хранится в формате String.
        df_final['dimension_value'] = df_final['dimension_value'].astype(str)

        metrics = [
            'views', 'likes', 'messages_received',
            'messages_sent', 'users_received', 'users_sent'
        ]
        for metric in metrics:
            df_final[metric] = df_final[metric].astype('uint64')

        return df_final[[
            'event_date', 'dimension', 'dimension_value',
            'views', 'likes', 'messages_received',
            'messages_sent', 'users_received', 'users_sent'
        ]]

    # Загружаем агрегаты в ClickHouse
    @task
    def load(df_final):
        query_create = """
            CREATE TABLE IF NOT EXISTS test.sevil_table (
                event_date Date,
                dimension String,
                dimension_value String,
                views UInt64,
                likes UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64
            ) ENGINE = MergeTree()
            ORDER BY event_date
        """
        ph.execute(query_create, connection=connection_test)
        ph.to_clickhouse(
            df=df_final,
            table='sevil_table',
            index=False,
            connection=connection_test
        )

    df_feed = feed_metrics()
    df_message = message_metrics()
    df_total = merge_metrics(df_feed, df_message)
    df_info = user_info()
    df_total_info = add_user_info(df_total, df_info)

    df_gender = gender_slice(df_total_info)
    df_age = age_slice(df_total_info)
    df_os = os_slice(df_total_info)

    df_final = concat_slices(df_gender, df_age, df_os)
    load(df_final)


sevil_ahmedova_etl = sevil_ahmedova_etl()
