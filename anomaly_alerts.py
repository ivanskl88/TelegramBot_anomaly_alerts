# импортируем бибилиотеки
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
import telegram
import io
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta


# напишем функцию, которая будет проверять наши данные каждые 15 минут на аномалии (в качестве метода: будем считать квартили)
# на вход функция получит 3 аргумента: df - сам датафрейм
#                                       metrics - метрики которые будем проверять
#                                       n - количество 15-минуток назад, которые будем учитывать при расчете
#                                       a - коэфициент определяющий ширину интервала
def check_anomaly(df, metric, n=7, a=3):
    # расчитаем 25 и 75 квантиль со сдвигом, чтобы не учитывать последнее значение метрики и периодом = n
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)

    # расчитаем межквартильный размах за периодн n
    df['iqr'] = df['q75'] - df['q25']

    # расчитаем верхнии и нижнии границы
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']

    # сгладим наши границы
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    # проверим данные на выбросы
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df


# напишем функцию которая будет каждые 15 минут проверять наши данные на выбросы и если обнаружит, то отправит сообщение в групповой чат.
def run_alerts(chat_id=None):
    # создадим бота
    chat_id = chat_id or 'my_chat_id'
    token = 'bot_token'
    bot = telegram.Bot(token=token)

    # напишем запрос и получим наши данные
    con = {
        'host': '*****',
        'password': '*****',
        'user': '*****',
        'database': '*****'
    }

    query = '''
    SELECT t1.ts,
           t1.date,
           t1.hm,
           t1.users_feed,
           t1.views,
           t1.likes,
           t1.ctr_to_likes,
           t2.users_messenger,
           t2.sent_messages
    FROM
    (SELECT 
           toStartOfFifteenMinutes(time) AS ts,
           toDate(time) AS date,
           formatDateTime(ts, '%R') AS hm,
           COUNT(DISTINCT user_id) AS users_feed,
           countIf(user_id, action = 'view') AS views,
           countIf(user_id, action = 'like') AS likes,
           ROUND(likes / views, 3) AS ctr_to_likes
    FROM simulator_20230220.feed_actions
    WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
    GROUP BY ts, date, hm) t1

    FULL OUTER JOIN

    (SELECT toStartOfFifteenMinutes(time) AS ts,
            toDate(time) AS date,
            formatDateTime(ts, '%R') AS hm,
            COUNT(DISTINCT user_id) AS users_messenger,
            COUNT(user_id) AS sent_messages
    FROM simulator_20230220.message_actions
    WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
    GROUP BY ts, date, hm) t2

    ON  t1.ts = t2.ts 
        AND t1.date = t2.date
        AND t1.hm = t2.hm

    ORDER BY ts    
    '''

    # сохраним данные в переменной data
    data = ph.read_clickhouse(query=query, connection=con)

    # # сформируем список метрик
    metrics_list = ['users_feed', 'views', 'likes', 'ctr_to_likes', 'users_messenger', 'sent_messages']

    # напишем цикл который будет проверять список наних метрик и в случае обнаружения выьроса отправит в телеграм сообщение
    for metric in metrics_list:
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)

        if is_alert == 1:
            msg = f'''
            Метркиа {metric}:
            текущее значение {df[metric].iloc[-1]} 
            отклонение от предыдущего значения {((df[metric].iloc[-1] / df[metric].iloc[-2]) - 1):.2%}

            сслыка на дашборд с оперативными данными : http://superset.lab.karpov.courses/r/3314
                  '''

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
            ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

            ax.set(xlabel='месяц-день время')
            ax.set(ylabel=metric)

            ax.set_title(metric)

            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{metric}_plot.png'
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)


# Автоматизируем наш отчет

# зададим дефолтные параметры
default_args = {
    'owner': 'i-skljannyj',
    'depends_on_past': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 3, 28)
}

# Интервал запуска DAG
schedule_interval = '15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alerts_our_aplication_iskl():
    # запишем id группы
    group_id = 'group_id'

    @task
    def task_alerts():
        run_alerts(chat_id=group_id)

    task_alerts()


alerts_our_aplication_iskl = alerts_our_aplication_iskl()




