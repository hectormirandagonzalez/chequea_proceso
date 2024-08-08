from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from src.config.db_config import esquema

Base = declarative_base()

class Formularios(Base):
    __tablename__ = 'formularios'
    __table_args__ = {'schema': esquema}
    id=Column(Integer, primary_key=True)
    titulo = Column(String)
    estado=Column(String)
    fecha_creacion= Column(Date)
    fecha_update = Column(Date)
    fecha_ultimo_reg = Column(Date)
    url = Column(String)
