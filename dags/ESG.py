from functools import partial
import json
import os

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")

agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"
summary_url = "/esg_governance_agent/invoke"

session_id = "20240113"


task_summary_prompt = """Based on the input, provide a summarized paragraph for each task. You must follow the structure in Markdown format like below:

    ### Governance 6 (a) - 1(change the title to the task ID)
    A summary of the information disclosure.
    ### Governance 6 (a) - 2
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

    markdown_file_name = "governance_summary.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
        markdown_file.write(concatenated_result)
    return concatenated_result


def strategy_merge(ti):
    result_0 = ti.xcom_pull(task_ids="Strategy_10_overview")["output"]["output"]
    result_1 = ti.xcom_pull(task_ids="Strategy_10_task_summary")["output"]["output"]
    result_2 = ti.xcom_pull(task_ids="Strategy_11")["output"]["output"]
    result_3 = ti.xcom_pull(task_ids="Strategy_13_overview")["output"]["output"]
    result_4 = ti.xcom_pull(task_ids="Strategy_13_task_summary")["output"]["output"]

    results = [result_0, result_1, result_2, result_3, result_4]
    concatenated_result = "\n\n".join(results)
    markdown_file_name = "strategy_summary.md"

    # Save the model's response to a Markdown file
    with open(markdown_file_name, "w") as markdown_file:
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
gov_6_overview_formatter = partial(
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
    prompt="Based on the input, SUMMARIZE whether the report discloses information that enables users of general purpose financial reports to understand the current and anticipated effects of climate - related risks and opportunities on the entity’s business model and value chain.",
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
gov_a_task_summary_agent = partial(
    post_request, post_url=summary_url, formatter=gov_a_task_summary_formatter
)
gov_b_task_summary_agent = partial(
    post_request, post_url=summary_url, formatter=gov_b_task_summary_formatter
)
gov_a_overview_agent = partial(
    post_request, post_url=summary_url, formatter=gov_a_overview_formatter
)
gov_b_overview_agent = partial(
    post_request, post_url=summary_url, formatter=gov_b_overview_formatter
)
gov_6_overview_agent = partial(
    post_request, post_url=summary_url, formatter=gov_6_overview_formatter
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
    post_request, post_url=summary_url, formatter=strategy_13_task_summary_formatter
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
    # wait_for_10_seconds = TimeDeltaSensor(
    #     task_id='wait_for_10_seconds',
    #     delta=timedelta(seconds=10),
    # )

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

    # Governance_6_merge = PythonOperator(
    #     task_id="Governance_6_merge",
    #     python_callable=gov_6_merge,
    # )

    Governance_6_overview = PythonOperator(
        task_id="Governance_6_overview",
        python_callable=gov_6_overview_agent,
    )

    # Strategy_10_a = PythonOperator(
    #     task_id="Strategy_10_a",
    #     python_callable=agent,
    #     op_kwargs={
    #         "data_to_send": {
    #             "input": {
    #                 "input": "Does the report describe climate-related risks and opportunities that could reasonably be expected to affect the entity’s prospects?"
    #             },
    #             "config": {"configurable": {"session_id": session_id}},
    #         }
    #     },
    # )

    # Strategy_10_b = PythonOperator(
    #     task_id="Strategy_10_b",
    #     python_callable=agent,
    #     op_kwargs={
    #         "data_to_send": {
    #             "input": {
    #                 "input": "Does the report explain, for each climate-related risk the entity has identified, whether the entity considers the risk to be a climate-related physical risk or climate-related transition risk?"
    #             },
    #             "config": {"configurable": {"session_id": session_id}},
    #         }
    #     },
    # )

    # Strategy_10_c = PythonOperator(
    #     task_id="Strategy_10_c",
    #     python_callable=agent,
    #     op_kwargs={
    #         "data_to_send": {
    #             "input": {
    #                 "input": "Does the report specify, for each climate-related risk and opportunity the entity has identified, over which time horizons—short, medium or long term— the effects of each climate-related risk and opportunity could reasonably be expected to occur?"
    #             },
    #             "config": {"configurable": {"session_id": session_id}},
    #         }
    #     },
    # )

    # Strategy_10_d = PythonOperator(
    #     task_id="Strategy_10_d",
    #     python_callable=agent,
    #     op_kwargs={
    #         "data_to_send": {
    #             "input": {
    #                 "input": "Does the report explain how the entity defines ‘short term’, ‘medium term’ and ‘long term’ and how these definitions are linked to the planning horizons used by the entity for strategic decision-making?"
    #             },
    #             "config": {"configurable": {"session_id": session_id}},
    #         }
    #     },
    # )

    # Strategy_10_task_summary = PythonOperator(
    #     task_id="Strategy_10_task_summary",
    #     python_callable=strategy_10_task_summary_agent,
    # )

    # Strategy_10_overview = PythonOperator(
    #     task_id="Strategy_10_overview",
    #     python_callable=strategy_10_overview_agent,
    # )

    # Strategy_11 = PythonOperator(
    #     task_id="Strategy_11",
    #     python_callable=agent,
    #     op_kwargs={
    #         "data_to_send": {
    #             "input": {
    #                 "input": "In identifying the climate-related risks and opportunities that could reasonably be expected to affect an entity’s prospects, does the report use all reasonable and supportable information that is available to the entity at the reporting date without undue cost or effort, including information about past events, current conditions and forecasts of future conditions?"
    #             },
    #             "config": {"configurable": {"session_id": session_id}},
    #         }
    #     },
    # )

    # Strategy_11_overview = PythonOperator(
    #     task_id="Strategy_11_overview",
    #     python_callable=strategy_11_overview_agent,
    # )

    # Strategy_13_a = PythonOperator(
    #     task_id="Strategy_13_a",
    #     python_callable=agent,
    #     op_kwargs={
    #         "data_to_send": {
    #             "input": {
    #                 "input": "Does the report disclose a description of the current and anticipated effects of climate-related risks and opportunities on the entity’s business model and value chain?"
    #             },
    #             "config": {"configurable": {"session_id": session_id}},
    #         }
    #     },
    # )

    # Strategy_13_b = PythonOperator(
    #     task_id="Strategy_13_b",
    #     python_callable=agent,
    #     op_kwargs={
    #         "data_to_send": {
    #             "input": {
    #                 "input": "Does the report disclose a description of where in the entity’s business model and value chain climate-related risks and opportunities are concentrated (for example, geographical areas, facilities and types of assets)?"
    #             },
    #             "config": {"configurable": {"session_id": session_id}},
    #         }
    #     },
    # )

    # Strategy_13_task_summary = PythonOperator(
    #     task_id="Strategy_13_task_summary",
    #     python_callable=strategy_13_task_summary_agent,
    # )

    # Strategy_13_overview = PythonOperator(
    #     task_id="Strategy_13_overview",
    #     python_callable=strategy_13_overview_agent,
    # )

    # strategy_results_merge = PythonOperator(
    #     task_id="strategy_merge",
    #     python_callable=strategy_merge,
    # )

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

    [Governance_6_a_overview, Governance_6_b_overview] >> Governance_6_overview

    # (
    #     [
    #         Strategy_10_a,
    #         Strategy_10_b,
    #         Strategy_10_c,
    #         Strategy_10_d,
    #     ]
    #     >> Strategy_10_task_summary
    #     >> Strategy_10_overview
    # )

    # Strategy_11 >> Strategy_11_overview

    # [Strategy_13_a, Strategy_13_b] >> Strategy_13_task_summary >> Strategy_13_overview

    # [
    #     Strategy_10_overview,
    #     Strategy_10_task_summary,
    #     Strategy_11,
    #     Strategy_13_overview,
    #     Strategy_13_task_summary,
    # ] >> strategy_results_merge
    # wait_for_5_seconds>>Strategy_11
