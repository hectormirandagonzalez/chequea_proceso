from sqlalchemy import URL
from dotenv import load_dotenv
import os

#Configuracón base de datos

load_dotenv()

dbase = {}
dbase['HOST'] = os.getenv('DATABASE_HOST')
dbase['PORT'] = os.getenv('DATABASE_PORT')
dbase['DATABASE'] = os.getenv('DATABASE_NAME')
dbase['USER'] = os.getenv('DATABASE_USER')
dbase['PASSWORD'] = os.getenv('DATABASE_PASSWORD')

url_object = URL.create(
    "postgresql+psycopg2",
    username=dbase['USER'],
    password=dbase['PASSWORD'],  # plain (unescaped) text
    host=dbase['HOST'],
    database=dbase['DATABASE'],
    port=dbase['PORT'],
)

esquema = 'jotform'
conexion = url_object
proceso = 'extraer'
