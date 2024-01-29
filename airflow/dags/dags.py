import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from transformation import extract_movies_to_df, extract_users_to_df, transform_avg_ratings, load_df_to_db


# #Define the etl function
# def etl():
#     movies_df = extract_movies_to_df()
#     users_df = extract_users_to_df()
#     transformed_df = transform_avg_ratings(movies_df,users_df)
#     load_df_to_db(transformed_df)
    
# Define the etl function
def etl():
    try:
        print("Extracting data...")
        movies_df = extract_movies_to_df()
        users_df = extract_users_to_df()

        print("Transforming data...")
        transformed_df = transform_avg_ratings(movies_df, users_df)

        print("Loading data to DB...")
        load_df_to_db(transformed_df)

        print("ETL process completed successfully!")
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        raise
    


# Define default_args for your DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email':['uncofits@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
}

# Instantiate a DAG
dag = DAG(
    dag_id ='etl_pipeline',
    default_args=default_args,
    description='ETL pipeline Airflow DAG',
    schedule_interval='0 * * * *',  # Set the schedule interval as per your requirements
    
)

# Create a task using PythonOperator
etl_task = PythonOperator(
    task_id= 'etl_task',  # Unique task ID
    python_callable= etl,  # Specify the Python callable to be executed
    dag=dag,  # Assign the DAG to the task
)

etl()