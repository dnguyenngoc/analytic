
import sys

sys.path = ['', '..'] + sys.path[1:]

from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# DB models
from db_models import dim_project_variable, dim_field, dim_process, fact_data_extraction, fact_document, fact_etl_report
from db_models.base_class import Base
from dags.data_warehouse_prod.settings import config as environments


config = context.config
config.set_main_option("sqlalchemy.url", environments.DWH_SQLALCHEMY_URI)
print(environments.DWH_SQLALCHEMY_URI)

fileConfig(config.config_file_name)

# Exceute db models
target_metadata = Base.metadata


def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        # version_table_schema='dwh_development_analytic',
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata,
            include_schemas=True,
            # version_table_schema='dwh_development_analytic',
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
