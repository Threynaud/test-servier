from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from marts_steps import fct_drugs_graph, int_drugs_clinical_trials_deps, int_drugs_pubmed_deps
from output_step import drugs_graph_to_json
from staging_steps import base_pubmed_csv, base_pubmed_json, stg_clinical_trials, stg_drugs, stg_pubmed

dag = DAG(
    dag_id="drugs_graph_building_pipeline",
    start_date=datetime(2022, 4, 7),
    schedule_interval="@daily",
    catchup=False,
)

# Staging
stg_drugs_step = PythonOperator(task_id="stg_drugs", python_callable=stg_drugs.main, dag=dag)

stg_clinical_trials_step = PythonOperator(
    task_id="stg_clinical_trials", python_callable=stg_clinical_trials.main, dag=dag
)

base_pubmed_csv_step = PythonOperator(task_id="base_pubmed_csv", python_callable=base_pubmed_csv.main, dag=dag)

base_pubmed_json_step = PythonOperator(task_id="base_pubmed_json", python_callable=base_pubmed_json.main, dag=dag)

stg_pubmed_step = PythonOperator(task_id="stg_pubmed", python_callable=stg_pubmed.main, dag=dag)


# Marts
int_drugs_clinical_trials_deps_step = PythonOperator(
    task_id="int_drugs_clinical_trials_deps",
    python_callable=int_drugs_clinical_trials_deps.main,
    dag=dag,
)

int_drugs_pubmed_deps_step = PythonOperator(
    task_id="int_drugs_pubmed_deps", python_callable=int_drugs_pubmed_deps.main, dag=dag
)

fct_drugs_graph_step = PythonOperator(task_id="fct_drugs_graph", python_callable=fct_drugs_graph.main, dag=dag)

# JSON writer
drugs_graph_to_json_step = PythonOperator(
    task_id="drugs_graph_to_json", python_callable=drugs_graph_to_json.main, dag=dag
)

# Dag ordering
base_pubmed_csv_step >> stg_pubmed_step
base_pubmed_json_step >> stg_pubmed_step
stg_drugs_step >> [int_drugs_clinical_trials_deps_step, int_drugs_pubmed_deps_step]
stg_clinical_trials_step >> int_drugs_clinical_trials_deps_step
stg_pubmed_step >> int_drugs_pubmed_deps_step
int_drugs_clinical_trials_deps_step >> fct_drugs_graph_step
int_drugs_pubmed_deps_step >> fct_drugs_graph_step
fct_drugs_graph_step >> drugs_graph_to_json_step
