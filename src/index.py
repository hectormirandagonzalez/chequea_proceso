import datetime
import time
from dotenv import load_dotenv
import os

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


load_dotenv()

url_wsapi = os.getenv('WSAPI_URL')
port_wsapi = os.getenv('WSAPI_PORT')


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
            VALUES ('ORD', 'ERROR', '*Problema con el orden de los viajes, delivery_orders:* " + str(row[0]) + "', \
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
            VALUES ('E2OE', 'ERROR', '*Errores en tabla e_to_open, delivery_orders:* " + str(row[0]) + "', \
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

def revisa_error_drayage():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT ordenes FROM (SELECT rtrim(ltrim((array_agg(driver))::text,'{'),'}') as ordenes \
    FROM (SELECT DISTINCT driver	FROM jotform.drayage WHERE container_code_validado = 'ERROR' \
    AND fecha_hora_local > '2025-04-07 00:00:00') z) y WHERE ordenes IS NOT NULL")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            hay = True
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'CONT'; \
                INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                VALUES ('CONT', 'ERROR', '*Códigos contendor Drayage con error:* " + str(row[0]) + "', \
                substring((now()::timestamp at time zone 'utc' \
                 at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
            print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'CONT';")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)


def revisa_nuevos_atrasados_e2o():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT id_envio FROM jotform.e_to_open WHERE estado = 'NUEVO' \
    AND fecha_hora_local < (substring((now()::timestamp at time zone 'utc' \
    at time zone 'America/Los_Angeles')::text,1,19)::timestamp - '1 hour'::interval) LIMIT 1")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            hay = True
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'E2ON'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('E2ON', 'ERROR', '*Hay estados NUEVO en e_to_open sin procesar* " + "', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
            print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'E2ON'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('E2ON', 'OK', 'ESTADOS E2OPEN OK', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)


def revisa_week_number_null():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT replace(envios, ',', ', ') as envios FROM \
    (SELECT array_agg(id_envio)::text as envios \
    FROM jotform.drayage WHERE week_number IS NULL) a WHERE envios is not null")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            hay = True
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'WKNL'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('WKNL', 'ERROR', '*Hay week_number nulos en tabla drayage: " + str(row[0]) + "* " + "', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
            print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'WKNL'; \
            INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
            VALUES ('WKNL', 'OK', 'WEEK_NUMBER DRAYAGE OK', \
            substring((now()::timestamp at time zone 'utc' \
             at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)


def revisa_seguimiento_eto():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT replace(devices, ',', ', ') as devices FROM (SELECT array_agg(id_device)::text as \
    devices FROM (SELECT id_device, count(id_device) as cuenta FROM jotform.seguimiento_eto \
    WHERE estado = 'ACTIVO'GROUP BY id_device HAVING count(id_device) > 1) a ) a WHERE devices is not null")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            hay = True
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'SEGI'; \
                INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                VALUES ('SEGI', 'ERROR', '*Hay id_device con más de 1 transaccion activa en vista \
                jot_form.seguimiento_eto: " + str(row[0]) + "* " + "', \
                substring((now()::timestamp at time zone 'utc' \
                 at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
            print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'SEGI';")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)


def revisa_planificacion():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT \
    CASE \
        WHEN \
            substring((now()::timestamp at time zone 'utc' \
            at time zone 'America/Los_Angeles')::text,1,19)::time < '16:00:00'::time \
        THEN true \
        ELSE \
            CASE \
            WHEN cuenta > 0 THEN true \
            ELSE false \
        END \
    END as valor \
    FROM \
    (SELECT COUNT(id) AS cuenta	FROM jotform.planificacion \
    WHERE delivery_date = (SELECT substring(((now()::timestamp at time zone 'utc' \
    at time zone 'America/Los_Angeles') + '1 day'::interval)::text,1,19)::date)) b")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            # si row[0] es True es correcto
            if row[0] == True:
                hay = True
            else:
                hay = False
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'PLAN'; \
                INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                VALUES ('PLAN', 'ERROR', '*Falta planificación*', \
                substring((now()::timestamp at time zone 'utc' \
                 at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
        else:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'PLAN'; \
                INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                VALUES ('PLAN', 'OK', 'PLANIFICACION OK', \
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


def revisa_itinerario():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT replace(rtrim(ltrim((array_agg(mensaje))::text,'{'),'}'), '\",\"', \
    chr(13) || chr(10) || chr(13) || chr(10)) mensajes \
    FROM (SELECT f.*, eto.formulario, ('Driver: ' || f.driver || chr(13) || chr(10) || 'Formulario: ' \
    || f.formulario	|| chr(13) || chr(10) || 'ID_Transaction: ' || f.id_transaction || chr(13) || chr(10)) \
    as mensaje FROM (SELECT * FROM (SELECT driver, id_transaction, s.formulario, delivery_date,	\
    (((delivery_date + s.sumar)::date)::text || ' ' || s.hora::text)::timestamp as fecha_hora_local \
    FROM jotform.planificacion,	(SELECT upper(f.titulo) as formulario, ci.hora, ci.sumar \
    FROM _chequeo.chequea_itinerario ci join jotform.formularios f on ci.formulario_id = f.id ) s \
    WHERE delivery_date between (substring((now()::timestamp at time zone 'utc' at time zone \
    'America/Los_Angeles')::text,1,19)::date + '-10 day'::interval) and substring((now()::timestamp at time zone 'utc' \
    at time zone 'America/Los_Angeles')::text,1,19)::date + '1 day'::interval) z \
    WHERE z.fecha_hora_local < substring((now()::timestamp at time zone 'utc' at time zone \
    'America/Los_Angeles')::text,1,19)::timestamp) f LEFT JOIN jotform.e_to_open eto \
    ON f.id_transaction = eto.id_transaction AND f.formulario = eto.formulario\
    WHERE eto.formulario IS NULL) as c")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            if row[0] is not None:
                hay = True
                query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'ITI'; \
                    INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                    VALUES ('ITI', 'ERROR', '*ITINERARIO, faltan submissions:* " + str(row[0]) + "', \
                    substring((now()::timestamp at time zone 'utc' \
                     at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
                session.execute(query_insert)
                print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'ITI'; \
                INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                VALUES ('ITI', 'OK', 'ITINERARIO OK', \
                substring((now()::timestamp at time zone 'utc' \
                 at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)


def revisa_itinerario_v2():
    engine = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine)
    session = sessionMaker()
    query_consulta = text("SELECT replace(rtrim(ltrim((array_agg(mensaje))::text,'{'),'}'), '\",\"', \
    chr(13) || chr(10) || chr(13) || chr(10)) mensajes FROM (SELECT f.*, eto.formulario, ('Driver: ' || f.driver || \
    chr(13) || chr(10) || 'Formulario: ' || f.formulario	|| chr(13) || chr(10) || 'ID_Transaction: ' || \
    f.id_transaction || chr(13) || chr(10)) as mensaje FROM (SELECT * FROM (SELECT * FROM (SELECT driver, \
    id_transaction, 'PICKUP_EMPTY_CHIQUITA-OXNARD' as formulario, fecha_hora_pickup, (((fecha_hora_pickup::date - \
    '1 day'::interval)::date)::text || ' ' || '23:00:00')::timestamp as fecha FROM jotform.planificacion \
    WHERE movement_type = 'PICKUP' UNION SELECT driver, id_transaction, 'CHECK-IN_CHIQUITA-OXNARD' as formulario, \
    fecha_hora_pickup, fecha_hora_pickup as fecha FROM jotform.planificacion WHERE movement_type = 'PICKUP'	UNION \
    SELECT driver, id_transaction, 'CHECK-OUT_CHIQUITA-OXNARD' as formulario, fecha_hora_pickup, (fecha_hora_pickup + \
    '4 hour'::interval)::timestamp as fecha	FROM jotform.planificacion WHERE movement_type = 'PICKUP' UNION	\
    SELECT driver, id_transaction, 'CHECK-IN_WALMART/KROGER' as formulario, fecha_hora_pickup, fecha_hora_etadrop \
    as fecha FROM jotform.planificacion WHERE movement_type = 'PICKUP' UNION SELECT driver, id_transaction, \
    'CHECK-OUT_WALMART/KROGER' as formulario, fecha_hora_pickup, (fecha_hora_etadrop + '1 hour'::interval)::timestamp \
    as fecha FROM jotform.planificacion WHERE movement_type = 'PICKUP' UNION SELECT driver, id_transaction, \
    'CHECK-IN_CHIQUITA-OXNARD' as formulario, fecha_hora_pickup, fecha_hora_pickup as fecha	FROM jotform.planificacion \
    WHERE movement_type = 'DELIVERY' UNION SELECT driver, id_transaction, 'CHECK-OUT_CHIQUITA-OXNARD' as formulario, \
    fecha_hora_pickup, (fecha_hora_pickup + '1 hour'::interval)::timestamp as fecha FROM jotform.planificacion \
    WHERE movement_type = 'DELIVERY' UNION SELECT driver, id_transaction, 'CHECK-IN_WALMART/KROGER' as formulario, \
    fecha_hora_pickup, fecha_hora_etadrop as fecha FROM jotform.planificacion WHERE movement_type = 'DELIVERY' \
    UNION SELECT driver, id_transaction, 'CHECK-OUT_WALMART/KROGER' as formulario, fecha_hora_pickup, \
    (fecha_hora_etadrop + '4 hour'::interval)::timestamp as fecha FROM jotform.planificacion WHERE \
    movement_type = 'DELIVERY' UNION SELECT driver, id_transaction, 'RETURN_EMPTY_CHIQUITA-OXNARD' as formulario, \
    fecha_hora_pickup, (((fecha_hora_etadrop)::date)::text || ' ' || '22:00:00')::timestamp as fecha FROM \
    jotform.planificacion WHERE movement_type = 'DELIVERY') s WHERE s.fecha_hora_pickup between \
    (substring((now()::timestamp at time zone 'utc' at time zone 'America/Los_Angeles')::text,1,19)::date + '-7 \
    day'::interval) and substring((now()::timestamp at time zone 'utc' at time zone \
    'America/Los_Angeles')::text,1,19)::date + '1 day'::interval) z WHERE z.fecha <= (now()::timestamp at time zone \
    'utc' at time zone 'America/Los_Angeles')::timestamp::timestamp) f \
    LEFT JOIN jotform.e_to_open eto ON f.id_transaction = eto.id_transaction AND f.formulario = eto.formulario \
    WHERE eto.formulario IS NULL) as c")
    try:
        resultado = session.execute(query_consulta)
        resultado = resultado.fetchall()
        hay = False
        for row in resultado:
            if row[0] is not None:
                hay = True
                query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'ITI'; \
                    INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                    VALUES ('ITI', 'ERROR', '*ITINERARIO, faltan submissions:* " + str(row[0]) + "', \
                    substring((now()::timestamp at time zone 'utc' \
                     at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
                session.execute(query_insert)
                print(row)
        if not hay:
            query_insert = text("DELETE FROM _chequeo.status_apps WHERE tag = 'ITI'; \
                INSERT INTO _chequeo.status_apps(tag, estado, mensaje, fecha_hora) \
                VALUES ('ITI', 'OK', 'ITINERARIO OK', \
                substring((now()::timestamp at time zone 'utc' \
                 at time zone 'America/Los_Angeles')::text,1,19)::timestamp)")
            session.execute(query_insert)
        session.commit()
        session.close()
    except Exception as e:
        print(e)


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
    # url = "http://localhost:3002/send-message"
    # url = "http://localhost:3001/lead"
    url = "http://" + url_wsapi + ":" + port_wsapi + "/v1/messages"

    message = msg + " \n\n --Este mensaje fue enviado automáticamente por el sistema de chequeo de procesos--"
    # data = {"fono": fono, "msg": message}
    # data = {"phone": fono, "message": message}
    data = {"number": fono, "message": message}
    headers = {"Content-Type": "application/json"}
    print(data)
    response = requests.post(url, json=data, headers=headers)
    time.sleep(5)
    print(response.text)
    return response


def revisa_conexion_a_base_de_datos():
    engine2 = create_engine(conexion).execution_options(autocommit=True)
    sessionMaker = sessionmaker(bind=engine2)
    session = sessionMaker()
    query_inserta_etopen = text("SELECT 1")
    print(query_inserta_etopen)
    resultado = True
    try:
        session.execute(query_inserta_etopen)
        session.commit()
        session.close()
    except Exception as e:
        resultado = False
        print(e)
    return resultado


def realiza_chequeos():
    if revisa_conexion_a_base_de_datos():
        revisa_orden_viajes()
        revisa_error_e2o()
        revisa_error_drayage()
        revisa_nuevos_atrasados_e2o()
        revisa_week_number_null()
        revisa_planificacion()
        ## revisa_itinerario()
        revisa_itinerario_v2()
        enviar_mensajes()
    else:
        print("No hay acceso a la base de datos")
        fonos = ["13109511864", "56993269900", "18057655148"]
        for fono in fonos:
            enviar_whatsapp(fono, "*No hay acceso a la base de datos*")
