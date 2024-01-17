import json
import os
from datetime import timedelta
from functools import partial

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"
summary_url = "/esg_governance_agent/invoke"

session_id = "20240113"


task_summary_prompt = """Based on the input, provide a summarized paragraph for each task. You must follow the structure in Markdown format like below (change the title to the task ID):

    ### Governance 6 (a) - 1 or Governance 6 (a)
    A summary of the information disclosure.
    ### Governance 6 (a) - 2 or Governance 6 (b)
    A summary of the information disclosure.
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


def merge(ti):
    result_0 = ti.xcom_pull(task_ids="Governance_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Governance_6_a_overview")["output"]["output"]
    result_2 = ti.xcom_pull(task_ids="Governance_6_a_task_summary")["output"]["output"]
    result_3 = ti.xcom_pull(task_ids="Governance_6_b_overview")["output"]["output"]
    result_4 = ti.xcom_pull(task_ids="Governance_6_b_task_summary")["output"]["output"]
    result_5 = ti.xcom_pull(task_ids="Strategy_overview")["output"]["output"]
    result_6 = ti.xcom_pull(task_ids="Strategy_10_overview")["output"]["output"]
    result_7 = ti.xcom_pull(task_ids="Strategy_10_task_summary")["output"]["output"]
    result_8 = ti.xcom_pull(task_ids="Strategy_11_overview")["output"]["output"]
    result_9 = ti.xcom_pull(task_ids="Strategy_13_overview")["output"]["output"]
    result_10 = ti.xcom_pull(task_ids="Strategy_13_task_summary")["output"]["output"]

    concatenated_result = (
        " # Governance\n"
        + result_0
        + "\n\n"
        + "## Governance 6 (a)\n"
        + result_1
        + "\n\n"
        + result_2
        + "\n\n"
        + "## Governance 6 (b)\n"
        + result_3
        + "\n\n"
        + result_4
        + "\n\n"
        + "# Strategy\n"
        + result_5
        + "\n\n"
        + "## Strategy 10\n"
        + result_6
        + "\n\n"
        + result_7
        + "\n\n"
        + "## Strategy 11\n"
        + result_8
        + "\n\n"
        + "## Strategy 13\n"
        + result_9
        + "\n\n"
        + result_10
    )

    markdown_file_name = "ESG_disclosure_report.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result


def task_PyOpr(
    task_id: str,
    callable_func,
    retries: int = 3,
    retry_delay=timedelta(seconds=3),
    execution_timeout=timedelta(minutes=10),
    op_kwargs: dict = None,
):
    return PythonOperator(
        task_id=task_id,
        python_callable=callable_func,
        retries=retries,
        retry_delay=retry_delay,
        execution_timeout=execution_timeout,
        op_kwargs=op_kwargs,
    )


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
    prompt="Based on the input, SUMMARIZE whether the report discloses information about the governance body(s) or individual(s) responsible for oversight of climate-related risks and opportunities, into ONE paragraph.",
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
    prompt="Based on the input, SUMMARIZE whether the report discloses information about management’s role in the governance processes, controls and procedures used to monitor, manage and oversee climate-related risks and opportunities, into ONE paragraph.",
    task_ids=["Governance_6_b_1", "Governance_6_b_2"],
    session_id=session_id,
)
gov_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE whether the report discloses information that enable users of general purpose financial reports to understand the governance processes, controls and procedures an entity uses to monitor, manage and oversee climate-related risks and opportunities, into ONE paragraph.",
    task_ids=[
        "Governance_6_a_overview",
        "Governance_6_b_overview",
    ],
    session_id=session_id,
)

strategy_10_task_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=["Strategy_10_a", "Strategy_10_b", "Strategy_10_c", "Strategy_10_d"],
    session_id=session_id,
)
strategy_10_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE whether the report discloses information that enables users of general purpose financial reports to understand the climate-related risks and opportunities that could reasonably be expected to affect the entity’s prospects, into ONE paragraph.",
    task_ids=["Strategy_10_a", "Strategy_10_b", "Strategy_10_c", "Strategy_10_d"],
    session_id=session_id,
)
strategy_11_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE whether the report use all reasonable and supportable information that is available to the entity at the reporting date without undue cost or effort, including information about past events, current conditions and forecasts of future conditions, into ONE paragraph.",
    task_ids=["Strategy_11"],
    session_id=session_id,
)
strategy_13_task_summary_formatter = partial(
    agent_formatter,
    prompt=task_summary_prompt,
    task_ids=["Strategy_13_a", "Strategy_13_b"],
    session_id=session_id,
)
strategy_13_overview_formatter = partial(
    agent_formatter,
    prompt="Based on the input, SUMMARIZE whether the report discloses information that enables users of general purpose financial reports to understand the current and anticipated effects of climate - related risks and opportunities on the entity’s business model and value chain, into ONE paragraph.",
    task_ids=["Strategy_13_a", "Strategy_13_b"],
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
govenance_a_task_summary_agent = partial(
    post_request, post_url=summary_url, formatter=gov_a_task_summary_formatter
)
govenance_b_task_summary_agent = partial(
    post_request, post_url=summary_url, formatter=gov_b_task_summary_formatter
)
govenance_a_overview_agent = partial(
    post_request, post_url=summary_url, formatter=gov_a_overview_formatter
)
govenance_b_overview_agent = partial(
    post_request, post_url=summary_url, formatter=gov_b_overview_formatter
)
govenance_overview_agent = partial(
    post_request, post_url=summary_url, formatter=gov_overview_formatter
)

strategy_10_task_summary_agent = partial(
    post_request, post_url=summary_url, formatter=strategy_10_task_summary_formatter
)
strategy_10_overview_agent = partial(
    post_request, post_url=summary_url, formatter=strategy_10_overview_formatter
)
strategy_11_overview_agent = partial(
    post_request, post_url=summary_url, formatter=strategy_11_overview_formatter
)
strategy_13_task_summary_agent = partial(
    post_request, post_url=summary_url, formatter=strategy_13_task_summary_formatter
)
strategy_13_overview_agent = partial(
    post_request, post_url=summary_url, formatter=strategy_13_overview_formatter
)


default_args = {
    "owner": "airflow",
}


with DAG(
    dag_id="ESG_new",
    default_args=default_args,
    description="ESG compliance Agent DAG",
    schedule_interval=None,
    tags=["ESG_agent"],
    catchup=False,
) as dag:
    # wait_for_10_seconds = TimeDeltaSensor(
    #     task_id='wait_for_10_seconds',
    #     delta=timedelta(seconds=10),
    # )

    Governance_6_a_1 = task_PyOpr(
        task_id="Governance_6_a_1",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report disclose information about: how responsibilities for climate-related risks and opportunities are reflected in the terms of reference, mandates, role descriptions and other related policies applicable to that body(s) or individual(s)?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_2 = task_PyOpr(
        task_id="Governance_6_a_2",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how the body(s) or individual(s) determines whether appropriate skills and competencies are available or will be developed to oversee strategies designed to respond to climate-related risks and opportunities?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_3 = task_PyOpr(
        task_id="Governance_6_a_3",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how and how often the body(s) or individual(s) is informed about climate-related risks and opportunities?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_4 = task_PyOpr(
        task_id="Governance_6_a_4",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how the body(s) or individual(s) takes into account climate-related risks and opportunities when overseeing the entity’s strategy, its decisions on major transactions and its risk management processes and related policies, including whether the body(s) or individual(s) has considered trade-offs associated with those risks and opportunities?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_5 = task_PyOpr(
        task_id="Governance_6_a_5",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: how the body(s) or individual(s) oversees the setting of targets related to climate-related risks and opportunities, and monitors progress towards those targets, including whether and how related performance metrics are included in remuneration policies?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_a_overview = task_PyOpr(
        task_id="Governance_6_a_overview",
        callable_func=govenance_a_overview_agent,
    )

    Governance_6_a_task_summary = task_PyOpr(
        task_id="Governance_6_a_task_summary",
        callable_func=govenance_a_task_summary_agent,
    )

    Governance_6_b_1 = task_PyOpr(
        task_id="Governance_6_b_1",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: management’s role in the governance processes, controls and procedures used to monitor, manage and oversee climate-related risks and opportunities, and whether the role is delegated to a specific management-level position or management-level committee and how oversight is exercised over that position or committee?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_b_2 = task_PyOpr(
        task_id="Governance_6_b_2",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: management’s role in the governance processes, controls and procedures used to monitor, manage and oversee climate-related risks and opportunities, and whether management uses controls and procedures to support the oversight of climate-related risks and opportunities and, if so, how these controls and procedures are integrated  with other internal functions?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Governance_6_b_overview = task_PyOpr(
        task_id="Governance_6_b_overview",
        callable_func=govenance_b_overview_agent,
    )

    Governance_6_b_task_summary = task_PyOpr(
        task_id="Governance_6_b_task_summary",
        callable_func=govenance_b_task_summary_agent,
    )

    Governance_overview = task_PyOpr(
        task_id="Governance_overview",
        callable_func=govenance_overview_agent,
    )

    # Governance_6_merge = PythonOperator(
    #     task_id="Governance_6_merge",
    #     python_callable=gov_6_merge,
    # )

    Strategy_10_a = task_PyOpr(
        task_id="Strategy_10_a",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report describe climate-related risks and opportunities that could reasonably be expected to affect the entity’s prospects?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Strategy_10_b = task_PyOpr(
        task_id="Strategy_10_b",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report explain, for each climate-related risk the entity has identified, whether the entity considers the risk to be a climate-related physical risk or climate-related transition risk?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Strategy_10_c = task_PyOpr(
        task_id="Strategy_10_c",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report specify, for each climate-related risk and opportunity the entity has identified, over which time horizons—short, medium or long term— the effects of each climate-related risk and opportunity could reasonably be expected to occur?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Strategy_10_d = task_PyOpr(
        task_id="Strategy_10_d",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report explain how the entity defines ‘short term’, ‘medium term’ and ‘long term’ and how these definitions are linked to the planning horizons used by the entity for strategic decision-making?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Strategy_10_task_summary = task_PyOpr(
        task_id="Strategy_10_task_summary",
        callable_func=strategy_10_task_summary_agent,
    )

    Strategy_10_overview = task_PyOpr(
        task_id="Strategy_10_overview",
        callable_func=strategy_10_overview_agent,
    )

    Strategy_11 = task_PyOpr(
        task_id="Strategy_11",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "In identifying the climate-related risks and opportunities that could reasonably be expected to affect an entity’s prospects, does the report use all reasonable and supportable information that is available to the entity at the reporting date without undue cost or effort, including information about past events, current conditions and forecasts of future conditions?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Strategy_11_overview = task_PyOpr(
        task_id="Strategy_11_overview",
        callable_func=strategy_11_overview_agent,
    )

    Strategy_13_a = task_PyOpr(
        task_id="Strategy_13_a",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report disclose a description of the current and anticipated effects of climate-related risks and opportunities on the entity’s business model and value chain?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Strategy_13_b = task_PyOpr(
        task_id="Strategy_13_b",
        callable_func=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Does the report disclose a description of where in the entity’s business model and value chain climate-related risks and opportunities are concentrated (for example, geographical areas, facilities and types of assets)?"
                },
                "config": {"configurable": {"session_id": session_id}},
            }
        },
    )

    Strategy_13_task_summary = task_PyOpr(
        task_id="Strategy_13_task_summary",
        callable_func=strategy_13_task_summary_agent,
    )

    Strategy_13_overview = task_PyOpr(
        task_id="Strategy_13_overview",
        callable_func=strategy_13_overview_agent,
    )

    Strategy_overview = task_PyOpr(
        task_id="Strategy_overview",
        callable_func=strategy_13_overview_agent,
    )

    results_merge = PythonOperator(
        task_id="merge",
        python_callable=merge,
    )

    (
        [
            Governance_6_a_1,
            Governance_6_a_2,
            Governance_6_a_3,
            Governance_6_a_4,
            Governance_6_a_5,
        ]
        >> Governance_6_a_task_summary
        >> Governance_6_a_overview
    )

    (
        [
            Governance_6_b_1,
            Governance_6_b_2,
        ]
        >> Governance_6_b_task_summary
        >> Governance_6_b_overview
    )

    [Governance_6_a_overview, Governance_6_b_overview] >> Governance_overview

    (
        [
            Strategy_10_a,
            Strategy_10_b,
            Strategy_10_c,
            Strategy_10_d,
        ]
        >> Strategy_10_task_summary
        >> Strategy_10_overview
    )

    Strategy_11 >> Strategy_11_overview

    [Strategy_13_a, Strategy_13_b] >> Strategy_13_task_summary >> Strategy_13_overview

    [
        Strategy_10_overview,
        Strategy_11_overview,
        Strategy_13_overview,
    ] >> Strategy_overview

    [
        Governance_overview,
        Governance_6_a_overview,
        Governance_6_a_task_summary,
        Governance_6_b_overview,
        Governance_6_b_task_summary,
        Strategy_overview,
        Strategy_10_overview,
        Strategy_10_task_summary,
        Strategy_11_overview,
        Strategy_13_overview,
        Strategy_13_task_summary,
    ] >> results_merge

