from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, BigInteger
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class FactEtlReport(Base):
    __tablename__ = "fact_etl_report"
    report_key = Column(Integer,  primary_key=True)
    project_id = Column(String(255), nullable=False)
    job_name = Column(String(255), nullable=False)
    schedule_type = Column(String(255), nullable=False)
    schedule_time_key = Column(String(255), nullable=True, default=None)
    schedule_date_key = Column(Integer, nullable=True, default=None)
    executor_date_timestamp = Column(DateTime, nullable=True, default=None)
    executor_date_key = Column(Integer, nullable=True, default=None)
    executor_time_key = Column(Integer, nullable=True, default=None)
    status_code = Column(String(255), nullable=True, default=None)
    description = Column(String(500), nullable=True, default=None)
    total_time_run_second = Column(Integer, nullable=False)

    __table_args__ = {"schema": "dwh_development_analytic"}