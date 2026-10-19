"""Ежедневный отчёт по метрикам ленты новостей в Telegram."""

import io
import os
from datetime import datetime, timedelta

import pandas as pd
import pandahouse as ph
import matplotlib.pyplot as plt
import seaborn as sns
import telebot
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Учётные данные и параметры Telegram передаются через переменные окружения.
connection = {
    'host': os.getenv('CLICKHOUSE_HOST'),
    'user': os.getenv('CLICKHOUSE_READ_USER'),
    'password': os.getenv('CLICKHOUSE_READ_PASSWORD'),
    'database': 'simulator_20260620'
}

my_token = os.getenv('TELEGRAM_BOT_TOKEN')
chat_id = os.getenv('TELEGRAM_CHAT_ID')


default_args = {
    'owner': 's.ahmedova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 7, 1)
}


@dag(
    dag_id='sevil_feed_report',
    default_args=default_args,
    schedule_interval='0 11 * * *',
    catchup=False
)
def sevil_feed_report():

    @task
    def send_report():
        bot = telebot.TeleBot(token=my_token)

        # В обычном ежедневном запуске Airflow 2 ds соответствует
        # календарной дате, за которую формируем отчёт.
        context = get_current_context()
        ds = context['ds']

        # Метрики ленты за отчётный день.
        q_yesterday = f"""
            SELECT
                toDate(time) AS date,
                uniq(user_id) AS DAU,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                round((likes / views) * 100, 2) AS CTR
            FROM simulator_20260620.feed_actions
            WHERE toDate(time) = '{ds}'
            GROUP BY date
        """
        df_yesterday = ph.read_clickhouse(q_yesterday, connection=connection)

        # Динамика метрик за семь дней, включая отчётный.
        q_7days = f"""
            SELECT
                toDate(time) AS date,
                uniq(user_id) AS DAU,
                countIf(action = 'view') AS views,
                countIf(action = 'like') AS likes,
                round((likes / views) * 100, 2) AS CTR
            FROM simulator_20260620.feed_actions
            WHERE toDate(time) >= toDate('{ds}') - 6
              AND toDate(time) <= toDate('{ds}')
            GROUP BY date
            ORDER BY date
        """
        df_7days = ph.read_clickhouse(q_7days, connection=connection)

        # Готовим текст сообщения.
        raw_date = df_yesterday['date'].iloc[0]
        formatted_date = pd.to_datetime(raw_date).strftime('%d.%m.%Y')

        msg = f"""Отчет за {formatted_date}

DAU: {df_yesterday.DAU.iloc[0]}
Просмотры: {df_yesterday.views.iloc[0]}
Лайки: {df_yesterday.likes.iloc[0]}
CTR: {df_yesterday.CTR.iloc[0]:.2f}%
"""

        # Четыре графика динамики за неделю.
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 10))
        fig.suptitle('Метрики ленты за предыдущие 7 дней', fontsize=16)
        axes = axes.flatten()

        sns.lineplot(data=df_7days, x='date', y='DAU', ax=axes[0])
        axes[0].set_title('DAU')

        sns.lineplot(data=df_7days, x='date', y='views', ax=axes[1])
        axes[1].set_title('Просмотры')

        sns.lineplot(data=df_7days, x='date', y='likes', ax=axes[2])
        axes[2].set_title('Лайки')

        sns.lineplot(data=df_7days, x='date', y='CTR', ax=axes[3])
        axes[3].set_title('CTR (%)')

        for ax in axes:
            ax.tick_params(axis='x', rotation=45)
            ax.set_xlabel('')
            ax.set_ylabel('')
            ax.grid(True)

        plt.tight_layout(rect=[0, 0, 1, 0.96])

        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png')
        plot_object.seek(0)
        plot_object.name = 'feed_report.png'
        plt.close(fig)

        bot.send_message(chat_id=chat_id, text=msg)
        bot.send_photo(chat_id=chat_id, photo=plot_object)
        plot_object.close()

    send_report()


sevil_feed_report = sevil_feed_report()
