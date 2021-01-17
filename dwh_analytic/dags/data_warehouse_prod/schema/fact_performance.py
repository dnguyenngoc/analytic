class FactPerformanceModel:
    def __init__(
        self,
        *kwargs,
        performance_key: int,
        ori_performance_id: str,
        document_key: str = None,
        project_id: str,
        group_id: str = None,
        document_id: str,
        reworked: bool = False,
        work_type_key: int,
        process_key: int,
        number_of_record: int = None,
        number_of_item: int = None,
        number_of_field: int = None,
        number_of_character: int = None,
        user_name: str,
        ip: str = None,
        captured_date_timestamp: str,
        captured_date_key: int,
        captured_time_key: int,
        total_time_second: int
    ):
        self.performance_key = performance_key
        self.ori_performance_id = ori_performance_id
        self.document_key = document_key
        self.project_id = project_id
        self.group_id = group_id
        self.document_id = document_id
        self.reworked = reworked
        self.work_type_key = work_type_key        
        self.process_key = process_key
        self.number_of_item = number_of_item
        self.number_of_record = number_of_record
        self.number_of_field = number_of_field
        self.number_of_character = number_of_character
        self.user_name = user_name
        self.ip = ip
        self.captured_date_timestamp = captured_date_timestamp
        self.captured_date_key = captured_date_key
        self.captured_time_key = captured_time_key
        self.total_time_second = total_time_second