class DimFieldModel:
    def __init__(
        self,
        *kwargs,
        ori_id: str,
        project_id: str,
        name: str,
        control_type: str,
        default_value: str = None,
        counted_character: bool,

    ):
        self.project_id = project_id
        self.document_id = document_id
        self.doc_set_id = doc_set_id
        self.last_modified_time_key = last_modified_time_key
        self.last_modified_date_key = last_modified_date_key
        self.user_name = user_name
        self.process_key = process_key
        self.field_name = field_name
        self.field_value = field_value
        self.last_modified_timestamp = last_modified_timestamp