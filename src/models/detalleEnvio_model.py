from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from src.config.db_config import esquema


Base = declarative_base()


class DetalleEnvio(Base):
    __tablename__ = 'detalle_envio'
    __table_args__ = {'schema': esquema}
    id=Column(Integer, primary_key=True)
    id_envio = Column(Integer)
    key_campo= Column(Integer)
    nombre_campo = Column(String)
    etiqueta_campo = Column(String)
    respuesta = Column(String)
    tipo = Column(String)