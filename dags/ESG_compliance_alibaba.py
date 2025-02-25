import json
import os
from datetime import timedelta
from functools import partial

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"
summary_url = "/openai_agent/invoke"

session_id = "20240409"

agent_prompt = "我是一个ESG(Environment, Social and Governance)领域的专家，熟练掌握各种ESG报告的核心要求；同时也非常擅长中文文本检索，能够根据需求检索出相应的中文文本；我还非常擅长中文文本分析与撰写，能够提取出给定文本的核心内容，并用流畅的中文进行总结"

task_summary_prompt = """
根据输入的内容，为每个任务写一个总结段落。您必须按照Markdown格式中的结构，如下所示（将标题更改为任务ID）：
    ### A.1
    先用一句话总结信息披露，然后进行具体解释（不要分点回答）
    ### A.2
    先用一句话总结信息披露，然后进行具体解释（不要分点回答）
    ...
    (继续添加其他任务ID及其总结)
    """

def post_request(
    post_url: str,
    formatter: callable,
    ti: any,
    data_to_send: dict = None,
):
    if not data_to_send:
        data_to_send = formatter(ti)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=600) as client:
        try:
            response = client.post(post_url, json=data_to_send)

            response.raise_for_status()

            data = response.json()

            return data

        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")


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

def agent_formatter(ti, prompt: str = "", task_ids: list = None, session_id: str = ""):
    if task_ids:
        results = []
        for task_id in task_ids:
            data = ti.xcom_pull(task_ids=task_id)
            task_output = data["output"]["output"]  # Output from agent
            task_output_with_id = (
                f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
            )
            results.append(task_output_with_id)
        content = "\n\n".join(results)  # Join all task results into a single string
        formatted_data = {
            "input": {
                "system input": agent_prompt,
                "human input": prompt + "\n\n INPUT:" + content
                },
            "config": {"configurable": {"session_id": session_id}},
        }
        return formatted_data
    else:
        pass
"""
def merge(ti):
    result_0 = ti.xcom_pull(task_ids="Governance_overview")["output"]["output"]

    concatenated_result = (
        " # Governance\n"
        + result_0
        + "\n\n"
    )

    markdown_file_name = "ESG_compliance_report_EN.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result
"""
A_task_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "A_1",
        "A_2"
    ],
    session_id=session_id,
)


agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
# openai = partial(post_request, post_url=openai_url, formatter=openai_formatter)
A_task_summary_agent = partial(post_request, post_url=summary_url, formatter=A_task_summary_formatter)

default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="ESG_test",
    default_args=default_args,
    description="ESG compliance Agent DAG",
    schedule_interval=None,
    tags=["ESG_test"],
    catchup=False,
) as dag:
    
    A_1 = task_PyOpr(
        task_id="A_1",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "system input": agent_prompt,
                    "human input": """请问阿里巴巴2023年的ESG报告是否披露了如下信息：
                    1.注册公司必须披露任何已经或有合理可能性对其业务战略、经营业绩或财务状况产生重大影响的气候相关风险。
                    2.对于物理风险,注册公司必须披露风险类型(急性或慢性)、受影响资产的地理位置和性质。
                    3.对于转型风险,注册公司必须披露风险是否与监管、技术、市场(包括消费者、业务伙伴和投资者偏好变化)或其他转型相关因素有关,以及这些因素如何影响公司。
                    4.注册公司必须提供足够的信息以解释所面临风险的性质及其对公司的暴露程度。
                    """
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    A_2 = task_PyOpr(
        task_id="A_2",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "system input": agent_prompt,
                    "human input": """请问阿里巴巴2023年的ESG报告是否披露了如下关于时间跨度和重要性的信息：
                    1.在描述任何已经实质性影响或合理可能实质性影响的气候相关风险时,注册实体应当分别描述此类风险是否合理可能在短期(即未来12个月内)和长期(即超过未来12个月)发生。
                    2.对于合理可能超过未来12个月发生的风险描述,注册实体可以进一步细分为中期和较长期风险,但这须与注册实体对气候相关风险的评估和管理一致。
                    3.在评估任何气候相关风险是否已经或合理可能实质性影响注册实体时,包括对其业务策略、经营业绩或财务状况的影响,注册实体应当依赖传统的实质性概念。实质性是指如果一个合理的投资者在决定买卖证券或投票时会将其视为重要,或者如果遗漏该信息会显著改变所提供信息的总体情况,则该事项具有实质性。
                    4.实质性的确定需要结合定量和定性因素进行事实具体分析。
                    5."合理可能"的评估标准与MD&A中关于已知趋势、事件和不确定性的相同标准一致,需要管理层客观评估风险的后果,即使未来事件的结果未知。
                    """
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    A_task_summary = task_PyOpr(
        task_id="A_task_summary",
        callable_func=A_task_summary_agent,
    )

    [A_1, A_2] >> A_task_summary