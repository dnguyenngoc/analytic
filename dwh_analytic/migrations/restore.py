# Session connect to database
from sqlalchemy import create_engine
from dags.data_warehouse_prod.settings import config as environments
from sqlalchemy.sql import text
import logging

data = environments.DATABASE_URL
engine = create_engine(data, pool_pre_ping=True, echo=True)
connection = engine.connect()
try:
    rs = connection.execute('DELETE FROM alembic_version')
except Exception as e:
     logging.warning("[Alembic] - %s", str(e))
finally:
    connection.close()
    engine.dispose()

