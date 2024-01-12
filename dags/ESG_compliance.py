from functools import partial

import httpx
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from httpx import RequestError

BASE_URL = "http://host.docker.internal:8000"
TOKEN = Variable.get("FAST_API_TOKEN")


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


agent_url = "/openai_agent/invoke"
openai_url = "/openai/invoke"


def agent_formatter(ti):
    data = ti.xcom_pull(task_ids="openai")
    content = data["output"]["content"]
    formatted_data = {
        "input": {"input": content},
        "config": {"configurable": {"session_id": ""}},
    }
    return formatted_data


def openai_formatter(ti):
    results=ti.xcom_pull(task_ids="Governance_6_a_1")
    pass


agent = partial(post_request, post_url=agent_url, formatter=agent_formatter)
openai = partial(post_request, post_url=openai_url, formatter=openai_formatter)


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
                    "input": "Dose the report disclose information about: how responsibilities for climate-related risks and opportunities are reflected in the terms of reference, mandates, role descriptions and other related policies applicable to that body(s) or individual(s)?"
                },
                "config": {"configurable": {"session_id": "20240112"}},
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
                "config": {"configurable": {"session_id": "20240112"}},
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
                "config": {"configurable": {"session_id": "20240112"}},
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
                "config": {"configurable": {"session_id": "20240112"}},
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
                "config": {"configurable": {"session_id": "20240112"}},
            }
        },
    )

    Governance_6_b_1 = PythonOperator(
        task_id="Governance_6_b_1",
        python_callable=agent,
        op_kwargs={
            "data_to_send": {
                "input": {
                    "input": "Dose the report disclose information about: management’s role in the governance processes, controls and procedures used to monitor, manage and oversee climate-related risks and opportunities, and whether the role is delegated to a specific management-level position or management-level committee and how oversight is exercised over that position or committee?"
                },
                "config": {"configurable": {"session_id": "20240112"}},
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
                "config": {"configurable": {"session_id": "20240112"}},
            }
        },
    )

    Governance = PythonOperator(
        task_id="Governance",
        python_callable=openai,
        op_kwargs={
            "data_to_send": {
                "input": "Summarize whether the report discloses how governance body(s) or individual(s) responsible for oversight of climate-related risks and opportunities.(The objective of climate-related financial disclosures on governance is to enable users of general purpose financial reports to understand the governance processes, controls and procedures an entity uses to monitor, manage and oversee climate-related risks and opportunities.)"
            }
        },
    )


    [Governance_6_a_1, Governance_6_a_2, Governance_6_a_3, Governance_6_a_4, Governance_6_a_5, Governance_6_b_1, Governance_6_b_2] >> Governance
    # task_1 >> task_2 >> [task_3, task_4] >> task_5
