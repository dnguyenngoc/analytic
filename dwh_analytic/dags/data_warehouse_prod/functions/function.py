import datetime

def is_list(data):
    if isinstance(data, list): 
        return True
    else: 
        return False
    
    
def int_to_bool(data: int):
    if data <= 0:
        return False
    else:
        return True

def bson_object_to_string(name):
    return str(name)


def time_to_date_key(data: datetime):
    day = data.day
    if day < 10:
        day = '0'+ str(day)
    return int(str(data.year)+str(data.month)+str(day))

def time_to_time_key(data: datetime):
    end = str(data.hour) + str(data.minute) + str(data.second)
    return int(end)

def get_process_id_performance(type):
    if type == 'keying':
        return 3
    elif type == 'qc':
        return 5
    elif type == 'approve_qc':
        return 6
     
def get_process_key_performance_gda(module, task):
    if module == 'keying':
        if task.startswith('Type'):
            return 3
        elif task.startswith('Proof'):
            return 5
    elif module == 'qc':
        return 8
    elif module == 'approve_qc':
        return 9
    elif task=='Verify_Hold_Type':
        return 12
        
def get_working_type_id_by_name(name: str):
    if name == 'In WorkShift':
        return 1
    else:
        return 2

def handle_date_to_date_and_time_id(date):
    date_id = int(date.strftime('%Y%m%d'))
    time_id = int(date.strftime('%H%M%S'))
    return date_id, time_id

def check_index_data_docs(meta_datas, list_created, request_id, case_id, case_number):
    for i in range(len(meta_datas)):
        item = meta_datas[i]
        if item['requestId'] == request_id and item['caseId'] == case_id and item['caseNumber'] == case_number:
            return list_created[i]


def lower_first_string(string):
    return string[0].lower() + string[1:]

def fix_field_name_keyed_data(name):
    if name.startswith('classify') or name.startswith('claim'): 
        return lower_first_string(name)
    elif name.startswith('cl'): 
        return lower_first_string(name[2:])
    elif name.startswith('ocr_'): 
        return lower_first_string(name[4:])
    else: 
        return lower_first_string(name)

def fix_process_type_keyed_data(key):
    if key == 'Auto_Extract': 
        return 'autoExtract'
    elif key == 'Classify': 
        return 'classify'
    elif key == 'Verify_Data': 
        return 'verify'

def fix_step_type_keyed_data(key):
    if key.startswith('cl'): 
        return 'confidenceLevel'
    elif key.startswith('ocr_'): 
        return 'opticalCharacterRecognition'


def get_process_key(module, process, step):
    if module == 'keyed_data':
        if process == 'verify':
            return 3
        elif process == 'autoExtract':
            if step == 'opticalCharacterRecognition':
                return 1
            elif step == 'confidenceLevel': 
                return 2
    elif module == 'system_data':
        return 4
    elif module == 'qc_ed_data':
        return 5
    elif module == 'apr_ed_data':
        return 6
    elif module == 'final_data':
        return 7
    elif module == 'transform_data':
        return 8

    
def created_date_of_docs_by_id(id: str, list_created: list):
    end = None
    for item in list_created:
        if item['doc_id'] == id:
            end = item['created_date']
            break
    return end