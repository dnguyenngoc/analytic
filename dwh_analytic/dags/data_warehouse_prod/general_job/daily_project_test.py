from pprint import pprint
import os
import time
import json
from datetime import date, timedelta
import datetime
from dateutil import tz
import pendulum
import pickle
from pymongo import MongoClient 
from dags.data_warehouse_prod.settings import config
from dags.data_warehouse_prod.functions import function as func
from dags.data_warehouse_prod.functions.db_connect import EngineConnect as DatabaseConnect
from dags.data_warehouse_prod.functions import report as report_func
from dags.data_warehouse_prod.schema.fact_document import FactDocumentModel
from dags.data_warehouse_prod.schema.dim_field import DimFieldModel
from dags.data_warehouse_prod.schema.fact_performance import FactPerformanceModel
from dags.data_warehouse_prod.schema.fact_data_extraction import FactDataExtractionModel


#==================================================================================================================================
#                                                                                                                  [EXECUTOR CLASS]
#==================================================================================================================================
class ProjectExecutor:
    def __init__(
        self,
        *kwargs,
        environment: str,
        project_id: str,
        uri: str,
        database_name: str = 'elrond',
        db: DatabaseConnect,
        start: datetime,
    ):
        self.backup_dir = config.BACKUP_DIR
        self.schema = 'dwh_production_pre_data' + '_' + str(start.strftime("%Y-%m-%d")).replace('-', '_')  # here change
        self.fact_document_table = config.DWH_FACT_DOCUMENT_TABLE
        self.fact_performance_table = config.DWH_FACT_PERFORMANCE_TABLE
        self.fact_data_extraction_table = config.DWH_FACT_DATA_EXTRACTION_TABLE
        self.fact_report_table = config.DWH_FACT_REPORT_ETL_TABLE
        self.dim_project_variable_table = config.DWH_DIM_PROJECT_VARIABLE_TABLE
        self.dim_field_table = config.DWH_DIM_FIELD_TABLE
        self.maxSevSelDelay = 20000
        self.start = start
        self.database_name  = config.ELROND_DATABASE
        
        self.db = db
        self.environment = environment
        self.project_id = project_id
        
        self.var_load = db.get_one_data_by_field_name_and_value('project_id',project_id,'dwh_development_analytic',self.dim_project_variable_table)
        self.project_name = config.GDA_PROJECT_NAME
        self.docs_collection_name = self.var_load['docs_collection_name']
        self.trans_collection_name = self.var_load['trans_collection_name']
        self.performance_collection_name = self.var_load['performance_collection_name']
        self.field_collection_name = self.var_load['field_collection_name']
        self.docs_query = func.handle_query_variable(self.var_load['docs_query'])
        self.trans_query = func.handle_query_variable(self.var_load['trans_query'])
        self.performance_query = func.handle_query_variable(self.var_load['performance_query'])
        self.field_query = self.var_load['field_query']
        self.project_backup_dir = self.var_load['project_backup_dir']
        self.project_docs_dir = self.var_load['project_docs_dir']
        self.project_trans_dir = self.var_load['project_trans_dir']
        self.project_performance_dir = self.var_load['project_performance_dir']
        self.project_field_dir = self.var_load['project_field_dir']
        self.backup_file_type = self.var_load['backup_file_type']
        self.schedule_type = self.var_load['schedule_type']
        self.schedule_date_key = self.var_load['schedule_date_key']
        self.schedule_time_key = self.var_load['schedule_time_key']
       
        self.reports = []
        self.document_key_checks = []
        self.performance_key_checks = []
        
#==================================================================================================================================
#                                                                                                      [LOANDING DOCUMENTS & TRANS]
#==================================================================================================================================
    def get_docs_and_trans(self): # here change
        if self.environment == 'development':
            obj_docs = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir + self.project_docs_dir \
                                    + self.project_id + '_' + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
            obj_trans = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir + self.project_trans_dir \
                                   + self.project_id + '_' +  str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        else: 
            obj_docs = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_docs_dir \
                                                            + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
            obj_trans = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_trans_dir \
                                                            + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        data_docs = [item for item in obj_docs]
        data_trans = [item for item in obj_trans]
        return data_docs, data_trans
    
#==================================================================================================================================
#                                                                                                            [LOANDING PERFORMANCE]
#================================================================================================================================== 
    def get_performance(self):
        if self.environment == 'development':
            obj_performance = pickle.load(open('dags/data_warehouse_prod/backup/' +self.project_backup_dir \
      + self.project_performance_dir + self.project_id + '_' + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        else:
            obj_performance = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_performance_dir \
                                                             + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        data_performance = [item for item in obj_performance]
        return data_performance
    
#==================================================================================================================================
#                                                                                                                 [LOANDING FIELDS]
#==================================================================================================================================     
    def get_field(self):
        if self.environment == 'development':
            obj_load = pickle.load(open('dags/data_warehouse_prod/backup/' +self.project_backup_dir + self.project_field_dir \
                                                                                 + self.project_id + self.backup_file_type, 'rb'))
        else:
            obj_load = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_field_dir \
                                                            + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        obj_list = [item for item in obj_load]
        return obj_list
    
#==================================================================================================================================
#                                                                                                                   [CHECK CONNECT]
#================================================================================================================================== 
    def check_connect(self):
        report = report_func.initial_report('check_connect', self.project_id, self.schedule_type, self.schedule_date_key, \
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                pass
            else:
                client = MongoClient(self.uri, serverSelectionTimeoutMS= self.maxSevSelDelay)
                client.server_info()
                client.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report

#==================================================================================================================================
#                                                                                                                      [CLEAN TASK]
#================================================================================================================================== 
    def clean(self):
        report = report_func.initial_report('clean', self.project_id, self.schedule_type, self.schedule_date_key, \
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development' or self.environment == 'production':
                now = self.start - timedelta(days=1)
                file_name = str(now.strftime("%Y-%m-%d"))
                list_path = [
                    self.backup_dir + self.project_backup_dir + self.project_docs_dir + file_name + self.backup_file_type,
                    self.backup_dir + self.project_backup_dir + self.project_trans_dir + file_name + self.backup_file_type,
                    self.backup_dir + self.project_backup_dir + self.project_performance_dir + file_name + self.backup_file_type,
                    self.backup_dir + self.project_backup_dir + self.project_field_dir + file_name + self.backup_file_type,
                ]
                description = ''
                for item in list_path:
                    if os.path.exists(item):
                        os.remove(item)
                    else:
                        description +=  ', ' + "The {path} does not exist".format(path = item)
                report.description = description
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report

#==================================================================================================================================
#                                                                                                                 [BACKUP DOCUMENT]
#================================================================================================================================== 
    def backup_docs(self):
        report = report_func.initial_report('backup_docs', self.project_id, self.schedule_type, self.schedule_date_key, \
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir + self.project_docs_dir \
                                                                                  + self.project_id + self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup/test_data/docs_' + str(self.start.strftime("%Y-%m-%d")) \
                                                                                                     + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.docs_collection_name].find(self.query_docs)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_docs_dir \
                                                              + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report
        
#==================================================================================================================================
#                                                                                                       [BACKUP DOCUMENT TRANSFORM]
#================================================================================================================================== 
    def backup_trans(self):
        report = report_func.initial_report('backup_trans', self.project_id, self.schedule_type, self.schedule_date_key, \
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir + self.project_trans_dir \
                                                                                    + self.project_id+self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup/test_data/trans_' + str(self.start.strftime("%Y-%m-%d")) \
                              + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.trans_collection_name].find(self.query_trans)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_trans_dir \
                                                             + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type , 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report
        
#==================================================================================================================================
#                                                                                                              [BACKUP PERFORMANCE]
#================================================================================================================================== 
    def backup_performance(self):
        report = report_func.initial_report('backup_performance', self.project_id, self.schedule_type, self.schedule_date_key, \
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir \
                                                   + self.project_performance_dir + self.project_id + self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup/test_data/performance_' + str(self.start.strftime("%Y-%m-%d")) \
                                                                                                     + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.performance_collection_name].find(self.query_performance)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_performance_dir \
                                                              + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report
        
#==================================================================================================================================
#                                                                                                                    [BACKUP FIELD]
#================================================================================================================================== 
    def backup_field(self):
        report = report_func.initial_report('backup_field', self.project_id, self.schedule_type, self.schedule_date_key, \
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir + self.project_field_dir \
                                                                                  + self.project_id + self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup/test_data/field_' + str(self.start.strftime("%Y-%m-%d")) \
                                                                                                     + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.performance_collection_name].find(self.query_field)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_performance_dir \
                                                              + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report
    
#==================================================================================================================================
#                                                                                                                   [UPLOAD_REPORT]
#==================================================================================================================================
    def report(self):
        all_report = [item.__dict__ for item in self.reports]
        report_func.upload_report(all_report, self.schema, self.fact_report_table, self.db)
        return all_report
        
#==================================================================================================================================
#                                                                                                                   [UPLOAD_REPORT]
#==================================================================================================================================
    def dim_field(self):
        report = report_func.initial_report('dim_field', self.project_id, self.schedule_type, self.schedule_date_key,
                                                                                                           self.schedule_time_key)
        start_run = time.time()
        try:
            check_date = self.start
            if check_date == func.check_dim_field_run(self.start) == True or self.environment == 'development':
                data_fields = self.get_field()
                results = []
                for data in data_fields:
                    _obj = DimFieldModel(
                            field_key = func.bson_object_to_string(data['_id']),
                            project_id = self.project_id,
                            name = data['name'],
                            control_type =  data['control_type'],
                            default_value = data['default_value'],
                            counted_character = data['counted_character'],
                            counted_character_date_from_key = 20210101, # hard code for test nedded change
                            counted_character_time_from_key = 0,  # hard code for test nedded change
                            counted_character_date_to_key =20210131,  # hard code for test nedded change
                            counted_character_time_to_key = 235959,  # hard code for test nedded change
                            counted_character_from_timestamp= '2021-01-01 00:00:00',  # hard code for test nedded change
                            counted_character_to_timestamp= '2021-01-31 23:59:59',  # hard code for test nedded change
                            is_sub_field = False,
                        )
                    results.append(_obj)
                report.status_code = 'PASSED'
                self.db.update([item.__dict__ for item in results], self.schema, self.dim_field_table)
            else:
                report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report
        
        
#==================================================================================================================================
#                                                                                                                   [UPLOAD_REPORT]
#==================================================================================================================================
    def fact_document(self):
        report = report_func.initial_report('fact_document', self.project_id, self.schedule_type, self.schedule_date_key,
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            datas = []
            data_docs, data_trans = self.get_docs_and_trans()
            list_created = [{'doc_id': func.bson_object_to_string(data['_id']), 'created_date': data['created_date']} \
                                                                                                             for data in data_docs]
            _id = 1
            for data in data_trans:
                if len(data['records']) == 0:
                    continue
                created_date_utc_7  = func.created_date_of_docs_by_id(func.bson_object_to_string(data['doc_id']), \
                                                                                      list_created) + datetime.timedelta(hours = 7)
                last_modified_utc_7 = data['last_modified'] + datetime.timedelta(hours = 7)
                import_date_key_utc_7, import_time_key_utc_7 = func.handle_date_to_date_and_time_id(created_date_utc_7)
                export_date_key_utc_7, export_time_key_utc_7 = func.handle_date_to_date_and_time_id(last_modified_utc_7)
                document_id = func.bson_object_to_string(data['doc_id'])
                doc_set_id = func.bson_object_to_string(data['doc_set_id']),
                _obj = FactDocumentModel(
                    document_key = _id,
                    ori_document_id = func.bson_object_to_string(data['_id']),
                    project_id = self.project_id,
                    document_id = document_id,
                    doc_set_id =  doc_set_id,
                    remark_code = None,
                    remark_description = None,
                    import_date_key = import_date_key_utc_7,
                    import_time_key = import_time_key_utc_7,
                    export_date_key = export_date_key_utc_7,
                    export_time_key = export_time_key_utc_7,
                    import_timestamp = created_date_utc_7,
                    export_timestamp = last_modified_utc_7,
                )
                self.document_key_checks.append({'document_key': _id, 'document_id': document_id, 'doc_set_id': doc_set_id})
                datas.append(_obj)
                _id+=1
            self.db.update([item.__dict__ for item in datas], self.schema, self.fact_document_table)
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report
             
#==================================================================================================================================
#                                                                                                                [FACT PERFORMANCE]
#==================================================================================================================================
    def fact_performance(self):
        report = report_func.initial_report('fact_performance', self.project_id, self.schedule_type, self.schedule_date_key, \
                                                                                                            self.schedule_time_key)
        start_run = time.time()
        try:
            datas = []
            data_performance = self.get_performance()
            _id = 1
            for performance in data_performance:
                process_key = func.get_process_key_performance_gda(performance['type'], performance['task_def_key'])
                captured_date_timestamp_utc_7 = performance['time'] + datetime.timedelta(hours = 7)
                document_key = None
                obj_ = FactPerformanceModel(
                        performance_key = _id,
                        ori_performance_id = func.bson_object_to_string(performance['_id']),
                        document_key = document_key,
                        project_id = self.project_id,  
                        group_id = performance['group_id'],  
                        document_id = performance['doc_id'],  
                        reworked = func.int_to_bool(performance['rework_count']),  
                        work_type_key = func.get_working_type_id_by_name(performance['work_type']),  
                        process_key = func.get_process_key_performance_gda(performance['type'], performance['task_def_key']),  
                        number_of_record = performance['records'],
                        number_of_item = performance['items'],  
                        number_of_field = performance['fields'],
                        number_of_character = performance['chars'],  
                        user_name = performance['username'], 
                        ip = performance['ip'], 
                        captured_date_timestamp = captured_date_timestamp_utc_7,  
                        captured_date_key = func.time_to_date_key(captured_date_timestamp_utc_7),  
                        captured_time_key = func.time_to_time_key(captured_date_timestamp_utc_7),  
                        total_time_second = performance['total_time']    
                )
                datas.append(obj_)
                _id+=1
            self.db.update([item.__dict__ for item in datas], self.schema, self.fact_performance_table)
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report
        
    def fact_document_status(self):
        data_docs, data_trans = self.get_docs_and_trans()
        
#==================================================================================================================================
#                                                                                                            [FACT DATA EXTRACTION]
#==================================================================================================================================
    def fact_data_extract(self):
        report = report_func.initial_report('fact_data_extraction', self.project_id, self.schedule_type, self.schedule_date_key, \
                                self.schedule_time_key)
        start_run = time.time()
        try:
            data_docs, data_trans = self.get_docs_and_trans()
            col_ignores = ['ImagePath']
            trans_ignore = ['doc_id', 'doc_uri', 'fileName', 'fileName_Bad', 'filter_control', 'getBatchName', 'keyer', \
                            'keyer_proof', 'keyer_type', 'FolderOutput', 'Image']
            results = []                
            for data in data_docs:
                records = data['records']
                document_id = func.bson_object_to_string(data['_id'])          
                document_key = None
                doc_set_id = func.bson_object_to_string(data['doc_set_id'])
                for i in range(len(records)):
                    record_id = i+1
                    record = records[i]
                    for key, value in record.items():
                        if key == 'keyed_data':
                            for keyed_data in value:
                                source = keyed_data['source']
                                task_def_key = keyed_data['task_def_key']
                                data_needed = keyed_data['data'][0].items()
                                last_modified_utc_7 = keyed_data['createdtime'] + datetime.timedelta(hours = 7)
                                user_name = keyed_data['keyer']
                                performance_key = None
                                if source != 'queue_transform' and task_def_key.startswith('Type'):
                                    process_key = 3 # human input keyed_data kpi
                                if source != 'queue_transform' and task_def_key == 'Verify_Hold_Type':
                                    process_key = 12 # human check bad_image keyed_data not kpi                               
                                elif source == 'queue_transform' and task_def_key.startswith('Type'):
                                    process_key = 4 # 'machine save input keyed_data'
                                elif source != 'queue_transform' and task_def_key.startswith('Proof'):
                                    process_key = 5 # human qc input keyed_data' kpi
                                elif source == 'queue_transform' and task_def_key.startswith('Proof'):
                                    process_key = 6 # 'machine save qc keyed_data'
                                for field_name, field_value_dict in data_needed:
                                    if field_name in col_ignores:
                                        continue
                                    _obj = FactDataExtractionModel(
                                        document_key = document_key,
                                        performance_key = performance_key,
                                        ori_document_id = document_id,
                                        project_id = self.project_id,
                                        document_id = document_id,
                                        doc_set_id =  doc_set_id,
                                        record_id = record_id,
                                        last_modified_date_key = func.time_to_date_key(last_modified_utc_7),
                                        last_modified_time_key = func.time_to_time_key(last_modified_utc_7),
                                        last_modified_timestamp = last_modified_utc_7,
                                        user_name = user_name,
                                        process_key = process_key,
                                        field_name = field_name,
                                        field_value = field_value_dict['text']
                                    )
                                    results.append(_obj)

                        elif key == 'final_data':
                            final_data = value[0]
                            data_needed = final_data['data'][0].items()
                            last_modified_utc_7 = final_data['createdtime'] + datetime.timedelta(hours = 7)
                            user_name = final_data['keyer']
                            process_key = 10
                            for field_name, field_value_dict in data_needed:
                                if field_name in col_ignores:
                                    continue
                                _obj = FactDataExtractionModel(
                                    document_key = document_key,
                                    performance_key = None,
                                    ori_document_id = document_id,
                                    project_id = self.project_id,
                                    document_id = document_id,
                                    doc_set_id =  doc_set_id,
                                    record_id = record_id,
                                    last_modified_date_key = func.time_to_date_key(last_modified_utc_7),
                                    last_modified_time_key = func.time_to_time_key(last_modified_utc_7),
                                    last_modified_timestamp = last_modified_utc_7,
                                    user_name = user_name,
                                    process_key = process_key,
                                    field_name = field_name,
                                    field_value = field_value_dict['text']
                                )
                                results.append(_obj)

                        elif key == 'qc_ed_data':
                            qc_ed_data = value[0][0]
                            if 'qc_fields_err' not in qc_ed_data.keys():
                                continue
                            qc_ed_data_err = qc_ed_data['qc_fields_err']
                            data_needed = qc_ed_data_err[0].items()
                            last_modified_utc_7 = qc_ed_data['createdtime'] + datetime.timedelta(hours = 7)
                            user_name = qc_ed_data['keyer']
                            process_key = 8
                            performance_key = None
                            for field_name, field_value_dict in data_needed:
                                if field_name in col_ignores:
                                    continue
                                _obj = FactDataExtractionModel(
                                    document_key = document_key,
                                    performance_key = performance_key,
                                    ori_document_id = document_id,
                                    project_id = self.project_id,
                                    document_id = document_id,
                                    doc_set_id =  doc_set_id,
                                    record_id = record_id,
                                    last_modified_date_key = func.time_to_date_key(last_modified_utc_7),
                                    last_modified_time_key = func.time_to_time_key(last_modified_utc_7),
                                    last_modified_timestamp = last_modified_utc_7,
                                    user_name = user_name,
                                    process_key = process_key,
                                    field_name = field_name,
                                    field_value = field_value_dict['text']
                                )
                                results.append(_obj)

                        elif key == 'apr_ed_data':
                            report.description = 'Not handle aprove qc data because not have sample data'
                            print('FUCK???????: ')
                            
            for data in data_trans:
                document_id = func.bson_object_to_string(data['doc_id'])
                document_key = None
                ori_document_id = func.bson_object_to_string(data['_id'])
                doc_set_id =  func.bson_object_to_string(data['doc_set_id'])
                performance_key = None
                records = data['records']
                last_modified_utc_7 = data['last_modified'] + datetime.timedelta(hours = 7)
                for i in range(len(records)):
                    record = records[i]
                    record_id = i + 1
                    data_needed = record.items()
                    for field_name, field_value in data_needed:
                        if field_name in trans_ignore:
                            continue
                        _obj = FactDataExtractionModel(
                            document_key = document_key,
                            performance_key = performance_key,
                            ori_document_id = ori_document_id,
                            project_id = self.project_id,
                            document_id = document_id,
                            doc_set_id =  doc_set_id,
                            record_id = record_id,
                            last_modified_date_key = func.time_to_date_key(last_modified_utc_7),
                            last_modified_time_key = func.time_to_time_key(last_modified_utc_7),
                            last_modified_timestamp = last_modified_utc_7,
                            user_name = None,
                            process_key = 11,
                            field_name = field_name,
                            field_value = field_value
                        )
                        results.append(_obj)
            self.db.update([item.__dict__ for item in results], self.schema, self.fact_data_extraction_table)
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            self.reports.append(report)
            return report