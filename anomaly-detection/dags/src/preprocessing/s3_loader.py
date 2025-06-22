import boto3
import pandas as pd
import io

def load_data_from_s3(**kwargs):
    key = kwargs.get('key') or kwargs['dag_run'].conf.get('key')
    s3_client = boto3.client(
        's3',
        aws_access_key_id='ACCESS_KEY',
        aws_secret_access_key='SECRET_ACCESS_KEY',
        region_name='ap-southeast-3'
    )

    response = s3_client.get_object(Bucket='ta-18221', Key=key)
    file_stream = io.BytesIO(response['Body'].read())
    df = pd.read_excel(file_stream, engine='openpyxl')
    
    csv_string = df.to_csv(index=False)
    
    ti = kwargs['ti']
    ti.xcom_push(key='s3_data', value=csv_string)