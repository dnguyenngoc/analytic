from dags.data_warehouse_prod.functions.report import initial_report
import time
from datetime import date, timedelta
import os
import pickle

class BackupData:

    def clean(self):
        report = initial_report('clean', self.project_id, self.schedule_type, self.schedule_date_key, self.schedule_time_key)
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
            return report

    def backup_performance(self):
        report = initial_report('backup_performance', self.project_id, self.schedule_type, self.schedule_date_key, self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/performance/' + self.project_id + self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup_test/performance_' + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.performance_collection_name].find(self.query)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_performance_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            return report

    def backup_docs(self):
        report = initial_report('backup_docs', self.project_id, self.schedule_type, self.schedule_date_key, self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/docs/' + self.project_id + self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup_test/docs_' + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.docs_collection_name].find(self.query)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_docs_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            return report

    def backup_trans(self):
        report = initial_report('backup_trans', self.project_id, self.schedule_type, self.schedule_date_key, self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/trans/' + self.project_id + self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup_test/trans_' + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.trans_collection_name].find(self.query)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_trans_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type , 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            return report



    def backup_field(self):
        report = initial_report('backup_field', self.project_id, self.schedule_type, self.schedule_date_key, self.schedule_time_key)
        start_run = time.time()
        try:
            if self.environment == 'development':
                objects = pickle.load(open('dags/data_warehouse_prod/backup/field/' + self.project_id + self.backup_file_type, 'rb'))
                data_objects = [item for item in objects]
                handle = open('dags/data_warehouse_prod/backup_test/performance_' + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            else:
                client = MongoClient(self.uri)
                data_query = client[self.database_name][self.performance_collection_name].find(self.query)
                data_objects = [item for item in data_query]
                client.close()
                handle = open(self.backup_dir + self.project_backup_dir + self.project_performance_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'wb')
                pickle.dump(data_objects, handle, protocol=pickle.HIGHEST_PROTOCOL)
                handle.close()
            report.status_code = 'PASSED'
        except Exception as e:
            report.status_code = 'FAILED'
            report.description = str(e)
        finally:
            report.total_time_run_second = time.time()-start_run
            return report
