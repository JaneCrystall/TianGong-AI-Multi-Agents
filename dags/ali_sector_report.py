import json
import os
from functools import partial
import time

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator
from httpx import RequestError
from tenacity import retry, stop_after_attempt, wait_fixed

BASE_URL = "http://192.168.65.254:7778"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"

session_id = int(time.time())


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def post_request(
    post_url: str,
    formatter: callable,
    ti: any,
    data_to_send: dict = None,
):
    if not data_to_send:
        data_to_send = formatter(ti)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=500) as client:
        try:
            response = client.post(post_url, json=data_to_send)

            response.raise_for_status()

            data = response.json()

            return data

        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")


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
            "input": {"input": prompt + "\n\n INPUT:" + content},
            "config": {"configurable": {"session_id": session_id}},
        }
        return formatted_data
    else:
        pass


bg1_agent_formatter = partial(
    agent_formatter,
    prompt="基于下面输入，编写云计算服务需求的全球趋势的中文段落。不要分点列出。要有逻辑、有层次，通顺连贯。",
    task_ids=["1.1云计算需求_素材检索", "1.2云计算需求_网络检索"],
    session_id=session_id,
)
bg2_agent_formatter = partial(
    agent_formatter,
    prompt="基于下面输入，编写介绍数据中心（Internet data center）如何支撑云计算的发展，以及数据中心的发展阶段的中文段落。不要分点列出。要有逻辑、有层次，通顺连贯。",
    task_ids=["2.1数据中心支撑_素材检索", "2.2数据中心支撑_网络检索"],
    session_id=session_id,
)
bg3_agent_formatter = partial(
    agent_formatter,
    prompt="基于下面输入，介绍云计算行业在环境可持续方面面临的挑战、压力和机遇。不要分点列出。要有逻辑、有层次，通顺连贯。",
    task_ids=["3.1绿色转型_素材检索", "3.2绿色转型_网络检索"],
    session_id=session_id,
)
bg4_agent_formatter = partial(
    agent_formatter,
    prompt="基于下面输入，介绍微软、谷歌、华为等公司的云计算服务和数据中心。不要分点列出。要有逻辑、有层次，通顺连贯。",
    task_ids=["4.1其他公司_素材检索", "4.2其他公司__网络检索"],
    session_id=session_id,
)
green5_agent_formatter = partial(
    agent_formatter,
    prompt="基于下面输入，详细介绍阿里云绿色计算技术栈（5 Greens），即绿色能源、绿色产品、绿色架构、绿色运营、绿色服务。不要分点列出。要有逻辑、有层次、有高度，通顺连贯。",
    task_ids=["5.1五绿_素材检索", "5.2五绿_网络检索"],
    session_id=session_id,
)

agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
bg1_agent = partial(
    post_request, post_url=agent_url, formatter=bg1_agent_formatter
)
bg2_agent = partial(
    post_request, post_url=agent_url, formatter=bg2_agent_formatter
)
bg3_agent = partial(
    post_request, post_url=agent_url, formatter=bg3_agent_formatter
)
bg4_agent = partial(
    post_request, post_url=agent_url, formatter=bg4_agent_formatter
)
bg5_agent = partial(
    post_request, post_url=agent_url, formatter=green5_agent_formatter
)


default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="ali_sector_report",
    default_args=default_args,
    description="Ali Report Agent DAG",
    schedule_interval=None,
    tags=["Ali_agent"],
    catchup=False,
) as dag:
    bg_cloud_demand_s = PythonOperator(
        task_id="1.1云计算需求_素材检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍云计算服务需求的全球趋势，检索specialied information. 编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_cloud_demand_i = PythonOperator(
        task_id="1.2云计算需求_网络检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍云计算服务需求的全球趋势，检索网络信息. 编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_cloud_demand = PythonOperator(
        task_id="1云计算需求",
        python_callable=bg1_agent,
    )

    bg_data_center_s = PythonOperator(
        task_id="2.1数据中心支撑_素材检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍数据中心（Internet data center）如何支撑云计算的发展，以及数据中心的发展阶段。检索specialied information. 编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_data_center_i = PythonOperator(
        task_id="2.2数据中心支撑_网络检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍数据中心（Internet data center）如何支撑云计算的发展，以及数据中心的发展阶段。检索网络信息。编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_data_center = PythonOperator(
        task_id="2数据中心支撑",
        python_callable=bg2_agent,
    )

    bg_green_tran_s = PythonOperator(
        task_id="3.1绿色转型_素材检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍云计算行业在环境可持续方面面临的挑战、压力和机遇。检索specialied information. 编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_green_tran_i = PythonOperator(
        task_id="3.2绿色转型_网络检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍云计算行业在环境可持续方面面临的挑战、压力和机遇。检索网络信息。编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_green_tran = PythonOperator(
        task_id="3绿色转型",
        python_callable=bg3_agent,
    )

    bg_other_s = PythonOperator(
        task_id="4.1其他公司_素材检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍微软、谷歌、华为等公司的云计算服务和数据中心。检索specialied information. 编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_other_i = PythonOperator(
        task_id="4.2其他公司_网络检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "介绍微软、谷歌、华为等公司的云计算服务和数据中心。检索网络信息。编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    bg_other = PythonOperator(
        task_id="4其他公司",
        python_callable=bg4_agent,
    )

    green5_s = PythonOperator(
        task_id="5.1五绿_素材检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "详细介绍阿里云绿色计算技术栈（5 Greens），即绿色能源、绿色产品、绿色架构、绿色运营、绿色服务。检索specialied information. 编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    green5_i = PythonOperator(
        task_id="5.2五绿_网络检索",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "详细介绍阿里云绿色计算技术栈（5 Greens），即绿色能源、绿色产品、绿色架构、绿色运营、绿色服务。检索网络信息。编写成完整、通顺的中文段落，不要分点列出。"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    green5 = PythonOperator(
        task_id="5五绿",
        python_callable=bg5_agent,
    )

    [green5_i, green5_s]>>green5
    [bg_other_s, bg_other_i]>>bg_other
    [bg_green_tran_s, bg_green_tran_i]>>bg_green_tran
    [bg_cloud_demand_s, bg_cloud_demand_i]>>bg_cloud_demand
    [bg_data_center_s, bg_data_center_i]>>bg_data_center
