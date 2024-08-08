import datetime
import time

from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
import requests
import logging
from src.config.db_config import conexion
from src.config.db_config import esquema
from src.models.formularios_model import Formularios
from src.models.envios_model import Envios
from src.models.detalleEnvio_model import DetalleEnvio
from src.models.log_model import Log
from src.models.eToOpen_model import EToOpen
from src.config.jotform_config import apikey


def revisa_orden_viajes():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT ordenes FROM (\
    SELECT rtrim(ltrim((array_agg(delivery_order))::text,'{'),'}') \
    as ordenes FROM (SELECT * FROM (SELECT delivery_order,position(formularios IN form_orden) \
    as posicion, formularios, form_orden FROM (SELECT delivery_order, \
    rtrim(ltrim((array_agg(eto.formulario))::text,'{'),'}') as formularios,	\
    (SELECT rtrim(ltrim((array_agg(formulario))::text,'{'),'}') as formularios FROM \
    (SELECT upper(trim(f.titulo)) formulario FROM _chequeo.orden_viajes ov JOIN jotform.formularios f \
    ON ov.form_id = f.id	ORDER BY orden) s) as form_orden \
    FROM \
    (SELECT eto.delivery_order, eto.fecha_hora_local, upper(trim(f.titulo)) as formulario FROM \
    (SELECT eto.* FROM jotform.e_to_open eto JOIN (SELECT DISTINCT delivery_order FROM jotform.e_to_open eto \
    WHERE eto.fecha_hora_local > substring((now()::timestamp at time zone 'utc' at time zone \
    'America/Los_Angeles')::text,1,19)::timestamp - '1 day'::interval) eto_1 USING (delivery_order) \
    WHERE UPPER(eto.estado) <> 'ANULADO') eto JOIN jotform.envios e ON eto.id_envio = e.id JOIN jotform.formularios \
    f ON e.form_id = f.id JOIN _chequeo.orden_viajes ov ON f.id = ov.form_id ORDER BY delivery_order, \
    fecha_hora_local) as eto GROUP BY delivery_order) b) c WHERE posicion <> 1) z) y WHERE ordenes IS NOT NULL")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            hay = True
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'ORD'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('ORD', 'ERROR', 'Problema con el orden de los viajes, delivery_orders: " + str(row[0]) + "', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
            print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'ORD'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('ORD', 'OK', 'ORDEN DE VIAJES OK', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)


def revisa_error_e2o():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT ordenes FROM (SELECT rtrim(ltrim((array_agg(delivery_order))::text,'{'),'}') \
    as ordenes FROM (SELECT DISTINCT delivery_order	FROM jotform.e_to_open WHERE estado = 'ERROR') z) y \
    WHERE ordenes IS NOT NULL")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            hay = True
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'E2OE'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('E2OE', 'ERROR', 'Errores en tabla e_to_open, delivery_orders: " + str(row[0]) + "', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
            print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'E2OE'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('E2OE', 'OK', 'E_TO_OPEN OK', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)

def estado_proceso():
    # obtener la cantidad de registros con estado NUEVO en la tabla EToOpen
    engine2 = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine2)
    session = sessionMaker()
    registros_nuevos = session.query(EToOpen).filter(EToOpen.estado == 'NUEVO').count()
    if registros_nuevos is None:
        registros_nuevos = 0

    # Aqui debo obtener la máxima fecha_hora de la tabla log
    maximaFechaLog = session.query(func.max(Log.fecha_hora))
    if maximaFechaLog[0][0] is None:
        maximaFechaLog = None
    else:
        maximaFechaLog = maximaFechaLog[0][0].strftime("%d-%m-%Y %H:%M:%S")

    maximaFechaE2O = session.query(func.max(EToOpen.fecha_hora_local))
    if maximaFechaE2O[0][0] is None:
        maximaFechaE2O = None
    else:
        maximaFechaE2O = maximaFechaE2O[0][0].strftime("%d-%m-%Y %H:%M:%S")
    session.close()

    salida = {"fecha_ultima_extraccion": maximaFechaLog,
              "fecha_ultimo_envio": maximaFechaE2O,
              "registros_nuevos": registros_nuevos}
    return salida


def enviar_mensajes():
    print("Ejecutando tarea enviando mensajeria 1: ")
    engine = create_engine(conexion).execution_options(autocommit=True)
    print("conectado")
    sessionMaker = sessionmaker(bind=engine)
    print("creando session")
    session = sessionMaker()
    print("session creada")
    query = text("SELECT b.mensajes, (SELECT array_agg(phone) FROM _chequeo.phones WHERE NOT disabled) \
    as phones FROM (SELECT replace(rtrim(ltrim((array_agg(mensaje))::text,'{'),'}'), '\",\"', \
    chr(13) || chr(10) || chr(13) || chr(10)) mensajes FROM (SELECT mensaje FROM _chequeo.status_apps sa \
    JOIN _chequeo.apps a ON sa.tag = a.tag WHERE NOT a.disabled ORDER BY a.id) a) b LEFT JOIN \
    (SELECT mensaje as mensajes FROM _chequeo.mensajes_enviado ORDER BY fecha_hora DESC LIMIT 1) c \
    ON b.mensajes = c.mensajes WHERE c.mensajes IS NULL;")
    print(query)
    try:
        resultado = session.execute(query).fetchall()
        print("registros a enviar: " + str(len(resultado)))
        for row in resultado:
            print("debe enviar")
            msg = row[0]
            for fono in row[1]:
                print("va a enviar" + fono)
                enviar_whatsapp(fono, msg)
            print(row)
            # insertar el mensaje que se envió en la tabla mensajes_enviado
            query_insert = text("INSERT INTO _chequeo.mensajes_enviado(mensaje, fecha_hora, estado) \
            VALUES ('" + msg + "', \
            substring((now()::timestamp at time zone 'utc' \
            at time zone 'America/Los_Angeles')::text,1,19)::timestamp, 'ENVIADO')")
            session.execute(query_insert)
            session.commit()
            print(row)
        session.close()
    except Exception as e:
        print(e)


def revisa_atraso():
    engine2 = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine2)
    session = sessionMaker()
    query_inserta_etopen = text( "SELECT a.tag,	\
    CASE WHEN sa.tag IS NULL THEN 'ATRASO' ELSE	\
    CASE WHEN fecha_hora IS NULL THEN 'ATRASO' \
    ELSE CASE WHEN (EXTRACT(EPOCH FROM \
    substring((now()::timestamp at time zone 'utc' \
    at time zone 'America/Los_Angeles')::text,1,19)::timestamp \
    - '2024-06-29 16:00:00'::timestamp ) / 60)::integer > a.intervalo_min \
    THEN 'ATRASO' ELSE sa.estado END END END AS estado,	\
    sa.mensaje FROM _chequeo.apps a LEFT JOIN _chequeo.status_apps sa \
    USING (tag) WHERE NOT a.disabled ORDER BY a.tag;")
    print (query_inserta_etopen)
    resultado = session.execute(query_inserta_etopen)
    resultado = resultado.fetchall()
    for row in resultado:
        print(row)
    session.commit()
    session.close()


def enviar_whatsapp(fono, msg):
    url = "http://localhost:3002/send-message"
    message = msg + " \n\n --Este mensaje fue enviado automáticamente por el sistema de chequeo de procesos--"
    data = {"fono": fono, "msg": message}
    headers = {"Content-Type": "application/json"}
    print(data)
    response = requests.post(url, json=data, headers=headers)
    time.sleep(5)
    print(response.text)
    return response


def realiza_chequeos():
    revisa_orden_viajes()
    revisa_error_e2o()
    enviar_mensajes()


