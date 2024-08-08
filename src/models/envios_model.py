from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from src.config.db_config import esquema

Base = declarative_base()

class Envios(Base):
    __tablename__ = 'envios'
    __table_args__ = {'schema': esquema}
    id=Column(Integer, primary_key=True)
    form_id = Column(Integer)
    fecha_creacion= Column(Date)
    fecha_update = Column(Date)
    estado=Column(String)
    nuevo = Column(String)
    ip_origen = Column(String)