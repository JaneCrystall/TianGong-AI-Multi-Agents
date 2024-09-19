from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests

# Function to make the API request
def call_rag_service():
    url = "https://langserve.tiangong.earth/main/rag"
    email = Variable.get("email")
    password = Variable.get("password")

    headers = {
        "email": email,
        "password": password,
        "Content-Type": "application/json"
    }
    payload = {
        "query": "哪些公司使用了阿里云来帮助减排？"
    }
    
    response = requests.post(url, headers=headers, json=payload)
    return response.json()

def process_output(ti):
    # 通过 XCom 获取上游任务的输出
    rag_output = ti.xcom_pull(task_ids='call_rag_service')['messages'][3]['kwargs']['content']
    rag_output = str(rag_output)
    
    # 保存到文件
    with open("rag_service_processed_output.txt", "w") as f:
        f.write(rag_output)
    return rag_output



# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
}

# Define the DAG
with DAG(
    dag_id="rag_service",
    default_args=default_args,
    description="A simple DAG to call the RAG service",
    schedule_interval=None,
    tags=["rag_service"],
    catchup=False,
) as dag:

    # Create a task to call the RAG service
    call_service_task = PythonOperator(
        task_id='call_rag_service',
        python_callable=call_rag_service
    )

    process_output_task = PythonOperator(
        task_id='process_output',
        python_callable=process_output,
    )

    # 设置任务依赖关系
    call_service_task >> process_output_task
