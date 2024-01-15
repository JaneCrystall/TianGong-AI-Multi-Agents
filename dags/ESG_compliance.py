from functools import partial
import json
import os

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"
governance_url = "/esg_governance_agent/invoke"

session_id = "20240113"

all_overview_prompt = """Based on the input, provide ONE overview paragraph about information disclosure. You must follow the structure in Markdown format like below:

    # Governance 6
    A concise summary about the ESG report disclosure status."""

task_overview_prompt = """Based on the input, provide ONE overview paragraph about information disclosure. You must follow the structure in Markdown format like below:

    ## Governance 6(a) or 6(b)
    A concise summary about the ESG report disclosure status.
    """

task_summary_prompt = """Based on the input, provide a summarized paragraph for each task. You must follow the structure in Markdown format like below:

    ### Governance 6 (a) - 1
    A concise summary of the disclosure status in the task.
    ### Governance 6 (a) - 2
    A concise summary of the disclosure status in the task.
    ...
    (Continue with additional task IDs and their summaries)"""


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


def openai_formatter(ti, prompt: str = None, task_ids: list = None):
    results = []
    for task_id in task_ids:
        data = ti.xcom_pull(task_ids=task_id)
        task_output = data["output"]["output"]  # Output from agent
        task_output_with_id = (
            f"{task_id}:\n {task_output}"  # Concatenate task ID with its output
        )
        results.append(task_output_with_id)
    content = "\n\n".join(results)  # Join all task results into a single string
    if prompt:
        content = prompt + "\n\n" + content
    formatted_data = {"input": content}
    return formatted_data


def gov_6_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Governance_6_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Governance_6_a_overview")["output"]["output"]
    result_2 = ti.xcom_pull(task_ids="Governance_6_a_task_summary")["output"]["output"]
    result_3 = ti.xcom_pull(task_ids="Governance_6_b_overview")["output"]["output"]
    result_4 = ti.xcom_pull(task_ids="Governance_6_b_task_summary")["output"]["output"]

    results = [result_0, result_1, result_2, result_3, result_4]
    concatenated_result = "\n\n".join(results)
    # print(concatenated_result)
    
    markdown_file_name = 'summary_output.md'

    # Save the model's response to a Markdown file
    with open(markdown_file_name, 'w') as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result


gov_a_task_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=[
        "Governance_6_a_1",
        "Governance_6_a_2",
        "Governance_6_a_3",
        "Governance_6_a_4",
        "Governance_6_a_5",
    ],
    session_id=session_id,
)
gov_b_task_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=["Governance_6_b_1", "Governance_6_b_2"],
    session_id=session_id,
)
gov_a_overview_formatter = partial(
    agent_formatter,
    prompt=task_overview_prompt,
    task_ids=[
        "Governance_6_a_1",
        "Governance_6_a_2",
        "Governance_6_a_3",
        "Governance_6_a_4",
        "Governance_6_a_5",
    ],
    session_id=session_id,
)
gov_b_overview_formatter = partial(
    agent_formatter,
    prompt=task_overview_prompt,
    task_ids=["Governance_6_b_1", "Governance_6_b_2"],
    session_id=session_id,
)
gov_6_overview_formatter = partial(
    agent_formatter,
    prompt=all_overview_prompt,
    task_ids=[
        "Governance_6_a_task_summary",
        "Governance_6_b_task_summary",
    ],
    session_id=session_id,
)

governance_openai_formatter = partial(
    openai_formatter,
    prompt="""Based on the following upstream outputs, provide a summary for each task related to the disclosure of how governance bodies or individuals are responsible for the oversight of climate-related risks and opportunities. You must follow the structure below:
    # Governance
    ## Governance 6 (a) - 1
    A concise summary of the disclosure in the task.
    ## Governance 6 (a) - 2
    A concise summary of the disclosure in the task.
    ...
    (Continue with additional task IDs and their summaries)

    Upstream outputs:
    """,
    task_ids=["Governance_6_a_1", "Governance_6_a_2"],
)


agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
# openai = partial(post_request, post_url=openai_url, formatter=openai_formatter)
openai_governance = partial(
    post_request, post_url=openai_url, formatter=governance_openai_formatter
)
gov_a_task_summary_agent = partial(
    post_request, post_url=governance_url, formatter=gov_a_task_summary_formatter
)
gov_b_task_summary_agent = partial(
    post_request, post_url=governance_url, formatter=gov_b_task_summary_formatter
)
gov_a_overview_agent = partial(
    post_request, post_url=governance_url, formatter=gov_a_overview_formatter
)
gov_b_overview_agent = partial(
    post_request, post_url=governance_url, formatter=gov_b_overview_formatter
)
gov_6_overview_agent = partial(
    post_request, post_url=governance_url, formatter=gov_6_overview_formatter
)

default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="ESG_compliance",
    default_args=default_args,
    description="ESG compliance Agent DAG",
    schedule_interval=None,
    tags=["ESG_agent"],
    catchup=False,
) as dag:
    Governance_6_a_1 = PythonOperator(
        task_id="Governance_6_a_1",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report disclose information about: how responsibilities for climate-related risks and opportunities are reflected in the terms of reference, mandates, role descriptions and other related policies applicable to that body(s) or individual(s)?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_2 = PythonOperator(
        task_id="Governance_6_a_2",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how the body(s) or individual(s) determines whether appropriate skills and competencies are available or will be developed to oversee strategies designed to respond to climate-related risks and opportunities?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_3 = PythonOperator(
        task_id="Governance_6_a_3",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how and how often the body(s) or individual(s) is informed about climate-related risks and opportunities?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_4 = PythonOperator(
        task_id="Governance_6_a_4",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how the body(s) or individual(s) takes into account climate-related risks and opportunities when overseeing the entity’s strategy, its decisions on major transactions and its risk management processes and related policies, including whether the body(s) or individual(s) has considered trade-offs associated with those risks and opportunities?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_5 = PythonOperator(
        task_id="Governance_6_a_5",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how the body(s) or individual(s) oversees the setting of targets related to climate-related risks and opportunities, and monitors progress towards those targets, including whether and how related performance metrics are included in remuneration policies?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_overview = PythonOperator(
        task_id="Governance_6_a_overview",
        python_callable=gov_a_overview_agent,
    )

    Governance_6_a_task_summary = PythonOperator(
        task_id="Governance_6_a_task_summary",
        python_callable=gov_a_task_summary_agent,
    )

    Governance_6_b_1 = PythonOperator(
        task_id="Governance_6_b_1",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: management’s role in the governance processes, controls and procedures used to monitor, manage and oversee climate-related risks and opportunities, and whether the role is delegated to a specific management-level position or management-level committee and how oversight is exercised over that position or committee?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_b_2 = PythonOperator(
        task_id="Governance_6_b_2",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: management’s role in the governance processes, controls and procedures used to monitor, manage and oversee climate-related risks and opportunities, and whether management uses controls and procedures to support the oversight of climate-related risks and opportunities and, if so, how these controls and procedures are integrated  with other internal functions?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_b_overview = PythonOperator(
        task_id="Governance_6_b_overview",
        python_callable=gov_b_overview_agent,
    )

    Governance_6_b_task_summary = PythonOperator(
        task_id="Governance_6_b_task_summary",
        python_callable=gov_b_task_summary_agent,
    )

    Governance_6_merge = PythonOperator(
        task_id="Governance_6_merge",
        python_callable=gov_6_merge,
    )

    Governance_6_overview = PythonOperator(
        task_id="Governance_6_overview",
        python_callable=gov_6_overview_agent,
    )

    [
        Governance_6_a_1,
        Governance_6_a_2,
        Governance_6_a_3,
        Governance_6_a_4,
        Governance_6_a_5,
    ] >> Governance_6_a_overview
    [
        Governance_6_a_1,
        Governance_6_a_2,
        Governance_6_a_3,
        Governance_6_a_4,
        Governance_6_a_5,
    ] >> Governance_6_a_task_summary

    [
        Governance_6_b_1,
        Governance_6_b_2,
    ] >> Governance_6_b_overview

    [
        Governance_6_b_1,
        Governance_6_b_2,
    ] >> Governance_6_b_task_summary

    [Governance_6_a_task_summary, Governance_6_b_task_summary] >> Governance_6_overview

    [
        Governance_6_overview,
        Governance_6_a_overview,
        Governance_6_a_task_summary,
        Governance_6_b_overview,
        Governance_6_b_task_summary,
    ] >> Governance_6_merge
