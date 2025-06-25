import clickhouse_connect
import pandas as pd

def load_data_from_clickhouse(**kwargs):
    key = kwargs.get('key') or kwargs['dag_run'].conf.get('key')

    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='admin',
        password='admin_password'
    )

    result = client.query('SELECT * FROM web_server_logs')

    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    ti = kwargs['ti']
    ti.xcom_push(key='clickhouse_df', value=df)
