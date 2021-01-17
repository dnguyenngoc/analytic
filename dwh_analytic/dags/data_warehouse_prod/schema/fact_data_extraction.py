class FactDataExtractionModel:
    def __init__(
        self,
        *kwargs,
        document_key: int,
        performance_key: int = None,
        ori_document_id: int,
        project_id: str,
        document_id: str,
        doc_set_id: str,
        record_id: int = None,
        last_modified_time_key: int,
        last_modified_date_key: int,
        user_name: str = None,
        process_key: int,
        field_name: str,
        field_value: str = None,
        last_modified_timestamp: str
    ):
        self.document_key = document_key
        self.performance_key = performance_key
        self.ori_document_id = ori_document_id
        self.project_id = project_id
        self.document_id = document_id
        self.doc_set_id = doc_set_id
        self.record_id = record_id
        self.last_modified_time_key = last_modified_time_key
        self.last_modified_date_key = last_modified_date_key
        self.user_name = user_name
        self.process_key = process_key
        self.field_name = field_name
        self.field_value = field_value
        self.last_modified_timestamp = last_modified_timestamp