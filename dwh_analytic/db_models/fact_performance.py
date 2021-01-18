from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, BigInteger
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class FactPerformance(Base):
    __tablename__ = "fact_performance"
    performance_key = Column(BigInteger,  primary_key=True, index=True)
    document_key = Column(BigInteger, nullable=True, default=None)
    ori_performance_id = Column(String(255), nullable=False)
    group_id = Column(String(255), nullable=True, default=None)
    project_id = Column(String(255), nullable=True)
    document_id = Column(String(255), nullable=True)
    reworked = Column(Boolean, nullable=False, default=False)
    work_type_key = Column(Integer, nullable=False)
    process_key = Column(Integer, nullable=True, default=1)
    user_name = Column(String(255), nullable=False)
    ip = Column(String(255), nullable=True, default=None)
    captured_date_timestamp = Column(DateTime, nullable=False)
    captured_date_key = Column(Integer, nullable=False)
    captured_time_key = Column(Integer, nullable=False)
    total_time_second = Column(Integer, nullable=False)
    number_of_record = Column(Integer, nullable=True, default=None)
    number_of_item = Column(Integer, nullable=True, default=None)
    number_of_field = Column(Integer, nullable=True, default=None)
    number_of_character = Column(Integer, nullable=True, default=None)

    __table_args__ = {"schema": "dwh_development_analytic"}