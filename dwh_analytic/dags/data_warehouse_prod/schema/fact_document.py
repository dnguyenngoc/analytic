class FactDocumentModel:
    def __init__(
        self,
        *kwargs,
        document_key: int,
        ori_document_id: int,
        project_id: int,
        document_id: int,
        doc_set_id: int,
        import_time_key: int,
        import_date_key: int,
        export_time_key: int,
        export_date_key: int,
        remark_code: str = None,
        remark_description: str = None,
        import_timestamp: str,
        export_timestamp: str,
    ):
        self.document_key = document_key
        self.ori_document_id = ori_document_id
        self.project_id = project_id
        self.document_id = document_id
        self.doc_set_id = doc_set_id
        self.import_time_key = import_time_key
        self.import_date_key = import_date_key
        self.export_time_key = export_time_key
        self.export_date_key = export_date_key
        self.import_timestamp = import_timestamp
        self.export_timestamp = export_timestamp
        self.remark_code = remark_code
        self.remark_description = remark_description