from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class DimProjectVariable(Base):
    __tablename__ = "dim_project_variable"
    project_id = Column(String(255),  primary_key=True, index=True)
    project_name = Column(String(255), nullable=False)
    docs_collection_name = Column(String(255), nullable=True, default=None)
    trans_collection_name = Column(String(255), nullable=True, default=None)
    performance_collection_name = Column(String(255), nullable=True, default=None)
    field_collection_name = Column(String(255), nullable=True, default=None)
    docs_query = Column(String(500), nullable=True, default=None)
    trans_query = Column(String(500), nullable=True, default=None)
    performance_query = Column(String(500), nullable=True, default=None)
    field_query = Column(String(500), nullable=True, default=None)
    project_backup_dir = Column(String(255), nullable=True, default=None)
    project_docs_dir = Column(String(255), nullable=True, default=None)
    project_trans_dir = Column(String(255), nullable=True, default=None)
    project_performance_dir = Column(String(255), nullable=False)
    backup_file_type = Column(String(255), nullable=True, default='.pickle')
    schedule = Column(String(255), nullable=True, default=None)
    schedule_date_key = Column(Integer, nullable=True, default=None)
    sechedule_time_key = Column(Integer, nullable=True, default=None)
    
    __table_args__ = {"schema": "dwh_development_analytic"}