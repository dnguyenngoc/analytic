from dags.data_warehouse_prod.functions.db_connect import EngineConnect as DatabaseConnect
from dags.data_warehouse_prod.general_job import ProjectExecutor
from dags.data_warehouse_prod.settings import config

# Will be split file when release
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG


db_connect = DatabaseConnect(uri = config.DWH_SQLALCHEMY_URI)
executor = ProjectExecutor(
    environment=config.ENVIRONMENT,
    project_id=config.GDA_PROJECT_ID,
    uri=config.ELROND_URI,
    database_name=config.ELROND_DATABASE,
    db = db_connect
)

dag_params = {
    'dag_id': "daily_dwh_gda_2020",
    'start_date': datetime.datetime(2021, 1, 1, tzinfo=config.LOCAL_TIME_ZONE),
    'schedule_interval': '0 4 * * *'
}

dag = DAG(**dag_params)

clean = PythonOperator(task_id='clean', python_callable=executor.clean, dag=dag)

check_connect = PythonOperator(task_id='check_connect', python_callable=executor.check_connect, dag=dag)

backup_docs = PythonOperator(task_id='backup_docs', python_callable=executor.backup_docs, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)
backup_trans = PythonOperator(task_id='backup_trans', python_callable=executor.backup_trans, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)
backup_performance = PythonOperator(task_id='backup_performance', python_callable=executor.backup_performance, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)
backup_fields = PythonOperator(task_id='backup_fields', python_callable=executor.backup_field, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)


dim_field = PythonOperator(task_id='dim_field', python_callable=executor.dim_field, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)
fact_document = PythonOperator(task_id='fact_document', python_callable=executor.fact_document, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)
fact_performance = PythonOperator(task_id='fact_performance', python_callable=executor.fact_performance, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)
fact_data_extract = PythonOperator(task_id='fact_data_extract', python_callable=executor.fact_data_extract, dag=dag, trigger_rule=TriggerRule.ALL_SUCCESS)

report = PythonOperator(task_id='report', python_callable=executor.report, dag=dag, trigger_rule=TriggerRule.ALL_DONE)



clean >> check_connect >> [backup_docs, backup_trans, backup_performance, backup_fields]

dim_field.set_upstream(backup_fields)
fact_document.set_upstream([backup_docs, backup_trans])
fact_performance.set_upstream([backup_performance, fact_document])
fact_data_extract.set_upstream([fact_document, fact_performance])

[dim_field, fact_document, fact_performance, fact_data_extract] >> report