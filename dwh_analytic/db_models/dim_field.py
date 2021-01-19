from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
from db_models.base_class import Base

class DimField(Base):
    __tablename__ = "dim_field"
    field_key = Column(Integer, nullable=False, primary_key=True)
    project_id = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    control_type = Column(String(255), nullable=True, default=None)
    default_value = Column(String(255), nullable=True, default=None)
    counted_character = Column(Boolean, nullable=False, default=True)
    counted_character_date_from_key = Column(Integer, nullable=False)
    counted_character_time_from_key = Column(Integer, nullable=False)
    counted_character_date_to_key = Column(Integer, nullable=False)
    counted_character_time_to_key = Column(Integer, nullable=False)
    is_sub_field = Column(Boolean, nullable=False, default=False)
    __table_args__ = {"schema": "dwh_development_analytic"}