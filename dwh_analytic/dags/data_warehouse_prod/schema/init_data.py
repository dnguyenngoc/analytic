from dags.data_warehouse_prod.functions.db_connect import EngineConnect as DatabaseConnect
from dags.data_warehouse_prod.settings import config
import pandas as pd


class InitData:
    def __init__(
        self,
        *kwagrs,
    ):
        self.dim_project_variable_table = config.DWH_DIM_PROJECT_VARIABLE_TABLE
        self.schema = config.DWH_ANALYTIC_SCHEMA     
   
    def dim_project_variable(self, db: DatabaseConnect):
        df = pd.read_csv('dags/data_warehouse_prod/backup/init_data/dim_project_variable.csv')
        df.set_index(['project_id'] , inplace=True)
        df.to_sql(self.dim_project_variable_table, schema = self.schema, con=db.engine, index = True, if_exists = 'append')
            