from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from src.config.db_config import esquema

Base = declarative_base()


class EToOpen(Base):
    __tablename__ = 'e_to_open'
    __table_args__ = {'schema': esquema}
    id_envio = Column(Integer, primary_key=True)
    fecha_creacion = Column(Date)
    fecha_hora_local = Column(Date)
    formulario = Column(String)
    id_transaction = Column(Integer)
    driver = Column(String)
    delivery_order = Column(String)
    longitud = Column(String)
    latitud = Column(String)
    estado = Column(String)
