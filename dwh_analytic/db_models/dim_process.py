from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class DimProcess(Base):
    __tablename__ = "dim_process"
    process_key = Column(Integer,  primary_key=True)
    module = Column(String(255), nullable=True, default=None)
    type = Column(String(255), nullable=True, default=None)
    step = Column(String(255), nullable=True, default=None)
    sub_step = Column(String(255), nullable=True, default=None)
    resource = Column(String(255), nullable=True, default=True)
    description = Column(String(500), nullable=True, default=False)

    __table_args__ = {"schema": "dwh_development_analytic"}