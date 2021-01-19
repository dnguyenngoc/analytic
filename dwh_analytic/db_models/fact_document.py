from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime, BigInteger
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class FactDocument(Base):
    __tablename__ = "fact_document"
    document_key = Column(BigInteger,  primary_key=True)
    ori_document_id = Column(String(255), nullable=False)
    project_id = Column(String(255), nullable=False)
    document_id = Column(String(255), nullable=False)
    doc_set_id = Column(String(255), nullable=True, default=None)
    import_time_key = Column(Integer, nullable=True, default=None)
    import_date_key = Column(Integer, nullable=True, default=None)
    export_time_key = Column(Integer, nullable=True, default=None)
    export_date_key = Column(Integer, nullable=True, default=None)
    import_timestamp = Column(DateTime, nullable=True, default=None)
    export_timestamp = Column(DateTime, nullable=False)
    remark_code = Column(String(255), nullable=True, default=None)
    remark_description = Column(String(500), nullable=True, default=None)

    __table_args__ = {"schema": "dwh_development_analytic"}