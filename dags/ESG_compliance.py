import json
import os
import subprocess
from datetime import timedelta
from functools import partial

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from httpx import RequestError

agent_url = "http://localhost:64321/functions/v1/main/esg"
report_ids = ["d4c03aac-19f2-4d73-8c42-529553973c1c"]
top_k = 3
file_path = "test.md"
TOKEN = Variable.get("BEARER_TOKEN")

"""
def post_request(
    post_url: str,
    formatter: callable = None,
    ti: any = None,
    data_to_send: dict = None,
):
    if not data_to_send:
        data_to_send = formatter(ti)
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    with httpx.Client(headers=headers, timeout=600) as client:
        try:
            response = client.post(
                post_url,
                json=data_to_send
            )
            response.raise_for_status()
            data = response.json()
            return data
        except httpx.RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")
"""

def curl_executor(prompt: str):
    data = {
        "query": prompt,
        "filter": {
            "reportId": report_ids,
            "topK": top_k
        }
    }
    curl_command = [
        "curl", "-i", "--location", "--request", "POST",
        f"{agent_url}",
        "--header", f"Authorization: Bearer {TOKEN}",
        "--header", "Content-Type: application/json",
        "--data", json.dumps(data)
    ]
    try:
        response = subprocess.run(curl_command, capture_output=True, text=True)
        json_data = response.stdout
        return json_data
    except subprocess.CalledProcessError as e:
        print("An error occurred:", e)
    except Exception as e:
        print("An unexpected error occurred:", str(e))

def task_PyOpr(
    task_id: str,
    callable_func,
    # retries: int = 3,
    # retry_delay=timedelta(seconds=3),
    execution_timeout=timedelta(minutes=10),
    op_kwargs: dict = None,
):
    return PythonOperator(
        task_id=task_id,
        python_callable=callable_func,
        # retries=retries,
        # retry_delay=retry_delay,
        execution_timeout=execution_timeout,
        op_kwargs=op_kwargs,
    )

def agent_formatter(ti, prompt: str = "", task_ids: list = None):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)["output"]
            results.append(data)
        with open(file_path, "w") as file:
            file.write(data)

default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="ESG_compliance",
    default_args=default_args,
    description="ESG compliance in new framework",
    schedule_interval=None,
    tags=["esg_compliance"],
    catchup=False,
) as dag:
    a1 = task_PyOpr(
        task_id="a1",
        callable_func=curl_executor,
        op_kwargs={
            "prompt": "该报告是否披露了公司有哪些负责监管气候风险的董事会？"
        }
    )
    a2 = task_PyOpr(
        task_id="a2",
        callable_func=curl_executor,
        op_kwargs={
            "prompt": "该报告是否披露了公司有负责监管气候风险的董事会获知这些风险的途径和过程？"
        }
    )
    [a1, a2]