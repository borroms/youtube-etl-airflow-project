from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from googleapiclient.discovery import build
import pandas as pd
import boto3

api_key = ''

channel_ids = ['UCnz-ZXXER4jOvuED5trXfEA', #techTFQ
               'UCJQJAI7IjbLcpsjWdSzYz0Q', #Thu Vu data analytics
               'UCChmJrVa8kDg05JfCmxpLRw', #Darshil Parmar
               'UC8butISFwT-Wl7EV0hUK0BQ', #freeCodeCamp.org
               'UCh8IuVJvRdporrHi-I9H7Vw'] #Unfold Data Science

youtube = build('youtube', 'v3', developerKey=api_key)

def extract_data(channel_ids):
    all_data = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response = request.execute()

    for item in response.get('items', []):
        data = {
            'channel_name': item['snippet']['title'],
            'subscribers': item['statistics']['subscriberCount'],
            'views': item['statistics']['viewCount'],
            'total_videos': item['statistics']['videoCount']
        }
        all_data.append(data)
        
    return all_data

def transform_data(**context):
    # Retrieve data from XCom
    ti = context['ti']
    all_data = ti.xcom_pull(task_ids='extract_data')

    channel_data = pd.DataFrame(all_data)

    channel_data['subscribers'] = pd.to_numeric(channel_data['subscribers'])
    channel_data['views'] = pd.to_numeric(channel_data['views'])
    channel_data['total_videos'] = pd.to_numeric(channel_data['total_videos'])
    
    # Convert DataFrame to dictionary
    channel_data_dict = channel_data.to_dict(orient='records')
    
    return channel_data_dict

def load_data_to_s3(**context):
    # Retrieve data from XCom
    ti = context['ti']
    channel_data_list = ti.xcom_pull(task_ids='transform_data')

    #Convert list of dictionaries back to DataFrame
    channel_data = pd.DataFrame(channel_data_list)

    # Save CSV locally
    channel_data.to_csv("yt_channels.csv", index=False)

    # Upload to S3
    s3 = boto3.client('s3')
    s3.upload_file("yt_channels.csv", "airflow-youtube-api-data-project", "yt_channels.csv")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('youtube_etl_dag',
          default_args=default_args,
          description='Extract, transform and load data from YouTube API to S3',
          schedule_interval='@daily',
          catchup=False)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=[channel_ids],
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Provide context to access XCom
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_to_s3',
    python_callable=load_data_to_s3,
    provide_context=True,  # Provide context to access XCom
    dag=dag
)

# Define task dependencies
extract_task >> transform_task >> load_task
