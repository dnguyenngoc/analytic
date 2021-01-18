from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class FactPerformance(Base):
    __tablename__ = "fact_performance"
    performance_key = Column(Integer,  primary_key=True, index=True)
    ori_id = Column(String(255), nullable=False)
    group_id = Column(String(255), nullable=True, default=None)
    project_id = Column(String(255), nullable=False)
    document_id = Column(String(255), nullable=False)
    reworked = Column(Boolean, nullable=False, default=False)
    work_type_key = Column(Interger, nullable=False, default=1)
    process_key = Column(Interger, nullable=False, default=1)
    user_name = Column(String(255), nullable=False)
    ip = Column(String(255), nullable=True, default=None)
    captured_date_timestamp = Column(DateTime, nullable=False)
    captured_date_key = Column(Interger, nullable=False)
    captured_time_key = Column(DateTime, nullable=False, default=0)
    total_time_second = Column(Interger, nullable=False)
    number_of_record = Column(Interger, nullable=True, default=None)
    number_of_item = Column(Interger, nullable=True, default=None)
    number_of_field = Column(Interger, nullable=True, default=None)
    number_of_character = Column(Interger, nullable=True, default=None)