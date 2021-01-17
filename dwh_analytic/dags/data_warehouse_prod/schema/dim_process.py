resource ='human ad machime'
class DimProcess:
    def __init__(
        self,
        *kwargs,
        process_key: int,
        module: str,
        type: str, 
        step: str,
        sub_step: str,
        resource: str = 'human',
        
    ):
    def step(self):
        return ['qc', 'auto_qc', 'apr_qc', 'keyer_input']
    def example_data(self):
        data = {
            'process_key': 1,
            'resource': 'human',
            'module': 'keyed_data',
            'step': 'qc',
            'sub_step': None,
            
            'process_key': 2,
            'resource': 'machine',
            'module': 'keyed_data',
            'step': 'transform',
            'sub_step': None,
        }
        

class FactDataExtractionModel:
    def __init__(
        self,
        *kwargs,
        project_id: str,
        document_id: str,
        doc_set_id: str,
        last_modified_time_key: int,
        last_modified_date_key: int,
        user_name: str = None,
        process_key: int,
        field_name: str,
        field_value: str = None,
        last_modified_timestamp: str
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