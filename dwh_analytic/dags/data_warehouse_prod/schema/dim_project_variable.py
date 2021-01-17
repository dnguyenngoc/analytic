from dags.data_warehouse_prod.functions.db_connect import EngineConnect as DatabaseConnect
from dags.data_warehouse_prod.settings import config
import pandas as pd


class DimProjectVariableModel:
    def __init__(
        self,
        *kwagrs,
        project_id: str,
    ):
        self.project_id = project_id
        self.dim_project_variable_table = config.DWH_DIM_PROJECT_VARIABLE_TABLE
        self.schema = config.DWH_ANALYTIC_SCHEMA
        
    def get_variable(self, field: str, value: str,  db: DatabaseConnect):
        result = db.raw_sql("""select * from {schema}."{table}" where {field_name} = 'value}';""" \
                                 .format(schema=self.schema, table=self.dim_project_variable_table, field=field, value=value))
        rows = result.fetchone()
        if len(rows) == 0:
            return None
        else:
            return rows[0]
        
    def init_schema(self, db: DatabaseConnect):
        db.raw_sql("""
            DROP TABLE {schema}."{table}";
            CREATE TABLE {schema}."{table}"
            (
                project_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
                project_name character varying(500) COLLATE pg_catalog."default" NOT NULL,
                docs_collection_name character varying(255) COLLATE pg_catalog."default",
                trans_collection_name character varying(255) COLLATE pg_catalog."default",
                performance_collection_name character varying(255) COLLATE pg_catalog."default",
                field_collection_name character varying(255) COLLATE pg_catalog."default",
                docs_query character varying(500) COLLATE pg_catalog."default",
                trans_query character varying(500) COLLATE pg_catalog."default",
                performance_query character varying(500) COLLATE pg_catalog."default",
                field_query character varying(500) COLLATE pg_catalog."default",
                project_backup_dir character varying(255) COLLATE pg_catalog."default",
                project_docs_dir character varying(255) COLLATE pg_catalog."default",
                project_trans_dir character varying(255) COLLATE pg_catalog."default",
                project_performance_dir character varying(255) COLLATE pg_catalog."default",
                project_field_dir character varying(255) COLLATE pg_catalog."default",
                backup_file_type character varying(255) COLLATE pg_catalog."default",
                schedule character varying(255) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
                schedule_date_key integer,
                sechedule_time_key integer,
                CONSTRAINT dim_project_variable_pkey PRIMARY KEY (project_id)
            )

            TABLESPACE pg_default;

            ALTER TABLE {schema}."{table}" OWNER to admin;
        """.format(schema=self.schema, table=self.dim_project_variable_table))
    
    def init_data(self, db: DatabaseConnect):
        df = pd.read_csv('dags/data_warehouse_prod/backup/init_data/dim_project_variable.csv')
        df.set_index(['project_id'] , inplace=True)
        df.to_sql(self.dim_project_variable_table, schema = self.schema, con=db.engine, index = True, if_exists = 'append')
            