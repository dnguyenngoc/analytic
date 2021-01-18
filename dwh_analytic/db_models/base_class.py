from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, ForeignKey, Integer, String, Boolean, DateTime, DECIMAL
from sqlalchemy.orm import relationship

class CustomBase(object):
    __mapper_args__= {'always_refresh': True}
    
Base = declarative_base(cls=CustomBase)
