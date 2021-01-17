import pickle
from dags.data_warehouse_prod.schema.fact_etl_report import FactEtlReportModel
import datetime
from dateutil import tz
import pendulum
from dags.data_warehouse_prod.functions import function as func
import time

def initial_report(name: str, project_id: str, schedule_type: str, schedule_date_key: int, schedule_time_key: int):
    to_zone = tz.gettz('Asia/Ho_Chi_Minh')
    from_zone = tz.tzutc()
    utc = datetime.datetime.utcnow()
    utc = utc.replace(tzinfo=from_zone)
    now = utc.astimezone(to_zone)
    report = FactEtlReportModel(
        project_id = project_id, 
        job_name = name, 
        executor_date_timestamp = now,
        executor_date_key = func.time_to_date_key(now),
        executor_time_key = func.time_to_time_key(now),
        schedule_type = schedule_type,
        schedule_date_key = schedule_date_key,
        schedule_time_key = schedule_time_key
    )
    return report