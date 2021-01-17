class FactEtlReportModel:
    def __init__(
        self,
        *kwargs,
        project_id: str,
        job_name: str,
        schedule_type: str = 'daily',
        schedule_time_key: str = None,
        schedule_date_key: str = None,
        executor_date_timestamp: str,
        executor_date_key: int,
        executor_time_key: int,
        status_code: str = None,
        description: str = None,
        total_time_run_second: float = None
    ):
        self.project_id = project_id
        self.job_name = job_name
        self.schedule_time_key = schedule_time_key
        self.schedule_date_key = schedule_date_key
        self.executor_date_timestamp = executor_date_timestamp
        self.executor_date_key = executor_date_key
        self.executor_time_key = executor_time_key
        self.status_code = status_code
        self.description = description        
        self.total_time_run_second = total_time_run_second