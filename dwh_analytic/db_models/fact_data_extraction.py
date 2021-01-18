from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, BigInteger
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class FactDataExtraction(Base):

    __tablename__ = "fact_data_extraction"
    data_extraction_key = Column(BigInteger,  primary_key=True, index=True)
    performance_key = Column(BigInteger, nullable=True)
    document_key = Column(BigInteger, index=True)
    ori_document_id = Column(String(255), nullable=True, default=None)
    project_id = Column(String(255), nullable=False)
    document_id = Column(String(255), nullable=True, default=None)
    doc_set_id = Column(String(255), nullable=True, default=None)
    record_id = Column(Integer, nullable=True, default=None)
    last_modified_time_key = Column(Integer, nullable=True, default=None)
    last_modified_date_key = Column(Integer, nullable=True, default=None)
    user_name = Column(String(255), nullable=True, default=None)
    process_key = Column(Integer, nullable=True, default=None)
    field_name = Column(String(255), nullable=False)
    field_value = Column(String(255), nullable=True, default=None)
    last_modified_timestamp = Column(DateTime, nullable=False)

    __table_args__ = {"schema": "dwh_development_analytic"}