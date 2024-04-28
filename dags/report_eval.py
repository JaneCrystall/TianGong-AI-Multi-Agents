import json
import os
from functools import partial
from datetime import datetime

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator
from httpx import RequestError
from tenacity import retry, stop_after_attempt, wait_fixed

BASE_URL = "http://host.docker.internal:7778"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_local_db_agent/invoke"
openai_url = "/openai_chain/invoke"


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
    with httpx.Client(base_url=BASE_URL, headers=headers, timeout=120) as client:
        try:
            response = client.post(post_url, json=data_to_send)

            response.raise_for_status()

            data = response.json()

            return data

        except RequestError as req_err:
            print(f"An error occurred during the request: {req_err}")
        except Exception as err:
            print(f"An unexpected error occurred: {err}")


def agent_formatter(ti, prompt: str = "", task_ids: list = None):
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
            "input": prompt + "\n\n INPUT:" + content,
        }
        return formatted_data
    else:
        pass


def merge(ti, task_ids: list = None, file_name: str = "output1.md"):
    concatenated_result = ""

    for task_id in task_ids:
        result = ti.xcom_pull(task_ids=task_id)["output"]["content"]
        concatenated_result += result + "\n\n"

    # Save the model's response to a Markdown file
    with open(file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result


planning_summary_formatter = partial(
    agent_formatter,
    prompt="""根据下面INPUT，批判性总结项目与经济社会发展规划、区域规划、专项规划、国土空间规划等重大规划的衔接性。针对每个规划，各分点总结一句话，要简洁明了，符合下面的markdown格式。
    #与重大规划的衔接性
    ##经济社会发展规划：<placeholder for 具体总结>
    ##区域规划：<placeholder for 具体总结>
    ##专项规划：<placeholder for 具体总结>
    ##国土空间规划：<placeholder for 具体总结
    """,
    task_ids=[
        "经济社会发展规划",
        "区域规划",
        "专项规划",
        "国土空间规划",
    ],
)
policy_summary_formatter = partial(
    agent_formatter,
    prompt="""根据下面INPUT，批判性总结项目与扩大内需、共同富裕、乡村振兴、科技创新、节能减排、碳达峰碳中和、国家安全和应急管理等重大政策目标的符合性。针对每个规划，各分点总结一句话，要简洁明了，符合下面的markdown格式。
    #与政策目标的符合性
    ##扩大内需：<placeholder for 具体总结>
    ##共同富裕：<placeholder for 具体总结>
    ##乡村振兴：<placeholder for 具体总结>
    ##科技创新：<placeholder for 具体总结>
    ##节能减排：<placeholder for 具体总结>
    ##碳达峰碳中和：<placeholder for 具体总结>
    ##国家安全和应急管理：<placeholder for 具体总结>""",
    task_ids=[
        "扩大内需",
        "共同富裕",
        "乡村振兴",
        "科技创新",
        "节能减排",
        "碳达峰碳中和",
        "国家安全和应急管理",
    ],
)


agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
planning_summary_chain = partial(
    post_request, post_url=openai_url, formatter=planning_summary_formatter
)
policy_summary_chain = partial(
    post_request, post_url=openai_url, formatter=policy_summary_formatter
)
planning_policy_merge = partial(
    merge, task_ids=["与重大规划的衔接性", "与政策目标的符合性"]
)


default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="report_eval",
    default_args=default_args,
    description="Report Evaluation DAG",
    schedule_interval=None,
    tags=["report_eval_agent"],
    catchup=False,
) as dag:
    socioeconomic = PythonOperator(
        task_id="经济社会发展规划",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与经济社会发展规划的衔接性。检索本地知识库"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    regional = PythonOperator(
        task_id="区域规划",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {"input": "批判性阐述项目与区域规划的衔接性。检索本地知识库"},
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    specific = PythonOperator(
        task_id="专项规划",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {"input": "批判性阐述项目与专项规划的衔接性。检索本地知识库"},
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    landuse = PythonOperator(
        task_id="国土空间规划",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与国土空间规划的衔接性。检索本地知识库"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    planning_summary = PythonOperator(
        task_id="与重大规划的衔接性",
        python_callable=planning_summary_chain,
    )

    internal_demand = PythonOperator(
        task_id="扩大内需",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与扩大内需政策目标的符合性。检索本地知识库。"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    prosperity = PythonOperator(
        task_id="共同富裕",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与共同富裕政策目标的符合性。检索本地知识库。"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    rural_revitalization = PythonOperator(
        task_id="乡村振兴",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与乡村振兴政策目标的符合性。检索本地知识库。"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    innovation = PythonOperator(
        task_id="科技创新",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与科技创新政策目标的符合性。检索本地知识库。"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    energy_saving = PythonOperator(
        task_id="节能减排",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与节能减排政策目标的符合性。检索本地知识库。"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    dual_carbon = PythonOperator(
        task_id="碳达峰碳中和",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与碳达峰碳中和政策目标的符合性。检索本地知识库。"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    emergency = PythonOperator(
        task_id="国家安全和应急管理",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "批判性阐述项目与国家安全和应急管理政策目标的符合性。检索本地知识库。"
                },
                "config": {
                    "configurable": {
                        "session_id": datetime.now().strftime("%Y%m%d%H%M%S")
                    }
                },
            }
        },
    )

    policy_summary = PythonOperator(
        task_id="与政策目标的符合性",
        python_callable=policy_summary_chain,
    )

    planning_policy_merge = PythonOperator(
        task_id="规划政策符合性评估结果",
        python_callable=planning_policy_merge,
    )

    [socioeconomic, regional, specific, landuse] >> planning_summary
    [
        internal_demand,
        prosperity,
        rural_revitalization,
        innovation,
        energy_saving,
        dual_carbon,
        emergency,
    ] >> policy_summary
    [planning_summary, policy_summary] >> planning_policy_merge
