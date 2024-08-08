from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from src.config.db_config import esquema

Base = declarative_base()


class Log(Base):
    __tablename__ = 'log'
    __table_args__ = {'schema': esquema}
    id = Column(Integer, primary_key=True)
    fecha_hora = Column(Date)
    observacion = Column(String)
