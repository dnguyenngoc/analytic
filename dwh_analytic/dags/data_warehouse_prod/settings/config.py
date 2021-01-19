import os
import pendulum
from datetime import date, timedelta
import datetime
from dateutil import tz



#==================================================================================================================================
#                                                                                                            [ENVIRONMENT AND TIME]
#==================================================================================================================================
ENVIRONMENT = 'development'
LOCAL_TIME_ZONE = pendulum.timezone("Asia/Ho_Chi_Minh")
to_zone = tz.gettz('Asia/Ho_Chi_Minh')
from_zone = tz.tzutc()
utc = datetime.datetime.utcnow()
utc = utc.replace(tzinfo=from_zone)
now = utc.astimezone(to_zone)
end = datetime.datetime(now.year, now.month, now.day, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
start = end - timedelta(days = 1)


#==================================================================================================================================
#                                                                                                                 [ELROND DATABASE]
#==================================================================================================================================
ELROND_DATABASE = 'elrond'
ELROND_PROJECT_COLLECTION = 'projects'
ELROND_URI = ''

#==================================================================================================================================
#                                                                                                                      [DWH BACKUP]
#==================================================================================================================================
BACKUP_DIR = "/dags/data_warehouse_prod/backup/"
BACKUP_FILE_TYPE = '.pickle'


#==================================================================================================================================
#                                                                                                                      [DWH CONFIG]
#==================================================================================================================================
# DWH_USER = 'admin'
# DWH_PASSWORD = '1q2w3e4r'
# DWH_HOST = '127.0.0.1'
# DWH_PORT = '5432'
# DWH_DATABASE = 'dwhdb'
DWH_USER = 'duynn_1'
DWH_PASSWORD = '9ajMnBiU1;j'
DWH_HOST = 'dbdd-rd-dwh.digi-texx.vn'
DWH_PORT = '5432'
DWH_DATABASE = 'dwhdb'
EHR_URL_USER_INFOR = 'https://ehr.digi-texx.vn/api/interaction'
DWH_SQLALCHEMY_URI = "postgresql+psycopg2://{user}:{pw}@{host}:5432/{db}" \
    .format(user=DWH_USER, pw=DWH_PASSWORD, host=DWH_HOST, db=DWH_DATABASE)

# SCHEMA
DWH_ANALYTIC_SCHEMA = 'dwh_' + ENVIRONMENT + '_analytic'
DWH_MART_PNL_SCHEMA = 'dwh_' + ENVIRONMENT + '_mart_pnl'
DWH_MART_OPERATION_SCHEMA = 'dwh_' + ENVIRONMENT + '_mart_operation'
DWH_PRE_DATA_SCHEMA = 'dwh_' + ENVIRONMENT + '_pre_data'

# DIM TABLE
DWH_DIM_PROJECT_VARIABLE_TABLE = 'dim_project_variable'
DWH_DIM_TIME_TABLE = 'dim_time'
DWH_DIM_DATE_TABLE = 'dim_date'
DWH_DIM_USER_TABLE = 'dim_user'
DWH_DIM_PROJECT_TABLE = 'dim_project'
DWH_DIM_WORK_TYPE_TABLE = 'dim_work_type'
DWH_DIM_PROCESS_TABLE = 'dim_process'
DWH_DIM_WORK_TYPE_TABLE = 'dim_work_type'
DWH_DIM_PROJECT_MANAGER_TABLE = 'dim_project_manager'
DWH_DIM_FIELD_TABLE = 'dim_field'

# FACT TABLE
DWH_FACT_PERFORMANCE_TABLE = 'fact_performance'
DWH_FACT_DOCUMENT_TABLE = 'fact_document'
DWH_FACT_PROCESS_TABLE = 'fact_process'
DWH_FACT_DATA_EXTRACTION_TABLE = 'fact_data_extraction'
DWH_FACT_DOCUMENT_METADATA_TABLE = 'fact_document_metadata'
DWH_FACT_REPORT_ETL_TABLE = 'fact_etl_report'

# MART TABLE
DWH_MART_PNL_SUMMARY_TABLE = 'pnl_summary'
DWH_MART_PNL_TARGET_TABLE = 'pnl_target'


#==================================================================================================================================
#                                                                                                                     [DWH PROJECT]
#==================================================================================================================================
"""This is a all variable needed for one project on dgs3 make pipeline etl with airflow"""

# [ GDA VARIABLE ]
GDA_PROJECT_ID = '5e9e7ec598d753001b7efe6b'
GDA_PROJECT_NAME = '0473_200421_006_2020_10052_GDA_2020'

















# # [ ECLAIMS VARIABLE ]
# ECLAIMS_PROJECT_ID = '5db144de27f919001f5f25e5'
# ECLAIMS_PROJECT_NAME = '096_190619_124_MVL_eClaim'
# ECLAIMS_DOCS_DIR = 'docs/'
# ECLAIMS_TRANS_DIR = 'trans/'
# ECLAIMS_PERFORMANCE_DIR = 'performance/'
# ECLAIMS_BACKUP_DIR = '096_190619_124_MVL_eClaim/'
# ECLAIMS_QUERY = {"last_modified":{"$gte": start, "$lte": end}}
# ECLAIMS_PERFORMANCE_QUERY = {"captured_date":{"$gte": start, "$lte": end}}
# ECLAIMS_DOCS_COLLECTION = ECLAIMS_PROJECT_ID + '_document'
# ECLAIMS_TRANS_COLLECTION = ECLAIMS_PROJECT_ID + '_transform_document'
# ECLAIMS_PERFORMANCE_COLLECTION = ECLAIMS_PROJECT_ID + '_transform_document'

# GDA_DOCS_DIR = 'docs/'
# GDA_TRANS_DIR = 'trans/'
# GDA_PERFORMANCE_DIR = 'performance/'
# GDA_FIELD_DIR = 'field/'
# GDA_BACKUP_DIR = '0473_200421_006_2020_10052_GDA_2020/'
# GDA_QUERY = {"last_modified": {"$gte": start, "$lte": end}}
# GDA_PERFORMANCE_QUERY = {'time': {"$gte": start, "$lte": end}}
# GDA_DOCS_COLLECTION = GDA_PROJECT_ID + '_document'
# GDA_TRANS_COLLECTION = GDA_PROJECT_ID + '_transform_document'
# GDA_PERFORMANCE_COLLECTION = GDA_PROJECT_ID + '_performance_report'

# [ MVL_STP_OCR VARIABLE ]
# MVL_STP_OCR_PROJECT_ID = '5db5c87345052400142992e9'
# MVL_STP_OCR_PROJECT_NAME = '148_191004_124_MVL_STP_OCR'
# MVL_STP_OCR_PERFORMANCE_QUERY =  {"captured_date":{"$gte": start, "$lte": end}}
# MVL_STP_OCR_DOCS_COLLECTION = MVL_STP_OCR_PROJECT_ID + '_document'
# MVL_STP_OCR_PERFORMANCE_COLLECTION = MVL_STP_OCR_PROJECT_ID + '_performance_report'
# MVL_STP_OCR_TRANS_COLLECTION = MVL_STP_OCR_PROJECT_ID + '_transform_document'
# MVL_STP_OCR_DOCS_DIR = 'docs/'
# MVL_STP_OCR_TRANS_DIR = 'trans/'
# MVL_STP_OCR_PERFORMANCE_DIR = 'performance/'
# MVL_STP_OCR_BACKUP_DIR = MVL_STP_OCR_PROJECT_NAME + '/'






# # ECLAIM TABLE
# DWH_DIM_FIELD_ECLAIMS_TABLE = 'dim_field_' + ECLAIMS_PROJECT_ID
# DWH_DIM_REMARK_ECLAIMS_TABLE = 'dim_remark_' + ECLAIMS_PROJECT_ID
# DWH_DIM_WORKING_TIME_ECLAIMS_TABLE = 'dim_working_time_' + ECLAIMS_PROJECT_ID
# DWH_FACT_ACCURACY_ECLAIMS_TABLE = 'fact_accuracy_' + ECLAIMS_PROJECT_ID
# DWH_FACT_DOCUMENT_ECLAIMS_TABLE = 'fact_document_' + ECLAIMS_PROJECT_ID
# DWH_FACT_PROCESS_ECLAIMS_TABLE = 'fact_process_' + ECLAIMS_PROJECT_ID














# # ------------------------------------------------------------[ SECURITY VARIABLE ]-----------------------------------------------------------------
# ENCODE_TYPE = "utf-8"
# SALT_SIZE = 16
# # SALT = os.urandom(SALT_SIZE)
# SALT = b'5\xe0v?\x17s\xdd:`Z\xbc\xb5\x85\xb43;'
# ROUNDS = 1994
# DIGEST = 'sha256'
# SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"


# ELROND_DATABASE = ''
# ELROND_URI = ''
# ELROND_PROJECT_COLLECTION = 'projects'
# ELROND_ECLAIMS_PERFORMANCE_REPORT_COLLECTION = ECLAIMS_PROJECT_ID + '_performance_report'
