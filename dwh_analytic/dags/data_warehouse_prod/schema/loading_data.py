import os
import pickle
from pymongo import MongoClient
from dags.data_warehouse_prod.settings import config

class LoadingData:

    def get_docs_and_trans(self):
        if self.environment == 'development':
            obj_docs = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir + self.project_docs_dir + self.project_id + self.backup_file_type, 'rb'))
            obj_trans = pickle.load(open('dags/data_warehouse_prod/backup/' + self.project_backup_dir + self.project_trans_dir + self.project_id + self.backup_file_type, 'rb'))
        else: 
            obj_docs = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_docs_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
            obj_trans = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_trans_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        data_docs = [item for item in obj_docs]
        data_trans = [item for item in obj_trans]
        return data_docs, data_trans
    
    def get_performance(self):
        if self.environment == 'development':
            obj_performance = pickle.load(open('dags/data_warehouse_prod/backup/' +self.project_backup_dir + self.project_performance_dir + self.project_id + self.backup_file_type, 'rb'))
        else:
            obj_performance = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_performance_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        data_performance = [item for item in obj_performance]
        return data_performance
    
    def get_field(self):
        if self.environment == 'development':
            obj_load = pickle.load(open('dags/data_warehouse_prod/backup/' +self.project_backup_dir + self.project_field_dir + self.project_id + self.backup_file_type, 'rb'))
        else:
            obj_load = pickle.load(open(self.backup_dir + self.project_backup_dir + self.project_field_dir + str(self.start.strftime("%Y-%m-%d")) + self.backup_file_type, 'rb'))
        obj_list = [item for item in obj_load]
        return obj_list
