import os
import json
from datetime import datetime
from geo_db_lotes_manager import GeoDBLotesManager
from werkzeug.exceptions import BadRequest
import pandas as pd
from flask_cors import CORS
from flask import Flask, g, request, jsonify
import pyodbc
from ai_manager import AIAnalysisManager

ai_manager = AIAnalysisManager(
    api_key="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"
)
app = Flask(__name__)
CORS(app, methods=["GET", "POST", "PUT", "DELETE"])

# Ruta de la base de datos
DATABASE_PATH = os.path.abspath("./IAD.mdb")
DRIVER = "Microsoft Access Driver (*.mdb, *.accdb)"

# Instancia global del gestor
db_manager = GeoDBLotesManager(DATABASE_PATH)

ai_manager = AIAnalysisManager(
    api_key="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"
)


# Serializador de fechas para JSON
def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


@app.before_request
def before_request():
    connection_string = f"DRIVER={{{DRIVER}}};DBQ={DATABASE_PATH};"
    g.db_conn = pyodbc.connect(connection_string)


@app.teardown_request
def teardown_request(exception):
    db_conn = g.pop("db_conn", None)
    if db_conn is not None:
        db_conn.close()


@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response


@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify(
        {
            "status": "ok",
            "message": "API está funcionando correctamente",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


@app.route("/api/analisis/resumen", methods=["POST"])
def analisis_resumen():
    data = request.get_json()
    resultado = ai_manager.analyze("resumen_lote", data)
    return jsonify({"analisis": resultado})


@app.route("/api/analisis/recomendaciones", methods=["POST"])
def analisis_recomendaciones():
    data = request.get_json()
    resultado = ai_manager.analyze("recomendaciones_lote", data)
    return jsonify({"analisis": resultado})


@app.route("/api/analisis/anomalias", methods=["POST"])
def analisis_anomalias():
    data = request.get_json()
    resultado = ai_manager.analyze("detectar_anomalias", data)
    return jsonify({"analisis": resultado})


@app.route("/api/analisis/score", methods=["POST"])
def analisis_score():
    data = request.get_json()
    resultado = ai_manager.analyze("score_salud_lote", data)
    return jsonify({"analisis": resultado})


@app.route("/api/analisis/proxima_accion", methods=["POST"])
def analisis_proxima_accion():
    data = request.get_json()
    resultado = ai_manager.analyze("proxima_accion_lote", data)
    return jsonify({"analisis": resultado})


@app.route("/api/tablas", methods=["GET"])
def listar_tablas():
    tablas = db_manager.listar_tablas(g.db_conn)
    return jsonify({"tablas": tablas})


@app.route("/api/lotes", methods=["GET"])
def get_lotes_geoespaciales():
    try:
        filtro = request.args.get("filtro")
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 10))

        page = max(1, page)
        page_size = min(max(1, page_size), 100)

        lotes_df = db_manager.obtener_lotes(g.db_conn, where=filtro)
        if lotes_df is None:
            return jsonify({"error": "Error al obtener lotes geoespaciales"}), 500

        total_registros = len(lotes_df)
        total_paginas = (total_registros + page_size - 1) // page_size
        inicio = (page - 1) * page_size
        fin = inicio + page_size

        lotes_pagina = lotes_df.iloc[inicio:fin].to_dict("records")

        for lote in lotes_pagina:
            for key, value in lote.items():
                if isinstance(value, bytes):
                    lote[key] = value.decode("utf-8", errors="replace")
                elif isinstance(value, datetime):
                    lote[key] = json_serial(value) if not pd.isnull(value) else None

        return jsonify(
            {
                "lotes": lotes_pagina,
                "page": page,
                "page_size": page_size,
                "total_registros": total_registros,
                "total_paginas": total_paginas,
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lotes/<int:objectid>", methods=["GET"])
def get_lote_por_id(objectid):
    try:
        filtro = f"OBJECTID = {objectid}"
        lote_df = db_manager.obtener_lotes(g.db_conn, where=filtro)

        if lote_df is None or lote_df.empty:
            return (
                jsonify({"error": f"No se encontró el lote con OBJECTID {objectid}"}),
                404,
            )

        lote = lote_df.iloc[0].to_dict()
        for key, value in lote.items():
            if isinstance(value, bytes):
                lote[key] = value.decode("utf-8", errors="replace")
            elif isinstance(value, datetime):
                lote[key] = json_serial(value) if not pd.isnull(value) else None

        return jsonify({"lote": lote})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/titular", methods=["GET"])
def get_titulares():
    objectid = request.args.get("objectid")
    filtro = f"OBJECTID_REF = {objectid}" if objectid else None
    titulares = db_manager.obtener_titulares(g.db_conn, filtro=filtro)

    if titulares is None:
        return jsonify({"error": "Error al obtener titulares"}), 500

    titulares_list = titulares.to_dict("records")
    for titular in titulares_list:
        for key, value in titular.items():
            if isinstance(value, datetime):
                titular[key] = json_serial(value)

    return jsonify({"titulares": titulares_list})


@app.route("/api/ai/servicio_tecnico", methods=["POST"])
def ai_servicio_tecnico():
    try:
        data = request.get_json()
        if data.get("lote") and "Shape" in data.get("lote"):
            del data.get("lote")["Shape"]
        data_defined = {
            "servicios_tecnicos": data,
            "lote": data.get("lote"),
        }
        # Use the global ai_manager instance instead of creating a new one
        result = ai_manager.analyze("servicio_tecnico", data_defined)
        return jsonify({"result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/titular", methods=["POST"])
def crear_titular():
    try:
        data = request.get_json()
        if "objectid" not in data:
            return jsonify({"error": "Se requiere el campo objectid"}), 400

        objectid = data["objectid"]
        datos_titular = {
            "CODIGO_LOTE": data.get("CODIGO_LOTE", ""),
            "AC": data.get("AC", ""),
            "TITULAR": data.get("TITULAR", ""),
            "CEDULA": data.get("CEDULA", ""),
            "Usuario": data.get("Usuario", "api_user"),
        }

        if "FECHA_ASIGNACION" in data and data["FECHA_ASIGNACION"]:
            try:
                datos_titular["FECHA_ASIGNACION"] = datetime.strptime(
                    data["FECHA_ASIGNACION"], "%Y-%m-%d"
                )
            except ValueError:
                return (
                    jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD"}),
                    400,
                )

        resultado = db_manager.agregar_titular(g.db_conn, objectid, datos_titular)
        if resultado:
            return jsonify(
                {"success": True, "message": "Titular agregado correctamente"}
            )
        else:
            return jsonify({"error": "No se pudo agregar el titular"}), 500

    except BadRequest:
        return jsonify({"error": "JSON inválido"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/seguimiento", methods=["GET"])
def get_seguimientos():
    objectid = request.args.get("objectid")
    filtro = f"OBJECTID_REF = {objectid}" if objectid else None
    seguimientos = db_manager.obtener_seguimientos(g.db_conn, filtro=filtro)

    if seguimientos is None:
        return jsonify({"error": "Error al obtener seguimientos"}), 500

    seguimientos_list = seguimientos.to_dict("records")
    for seguimiento in seguimientos_list:
        for key, value in seguimiento.items():
            if isinstance(value, datetime):
                seguimiento[key] = json_serial(value)

    return jsonify({"seguimientos": seguimientos_list})


@app.route("/api/seguimiento", methods=["POST"])
def crear_seguimiento():
    try:
        data = request.get_json()
        if "objectid" not in data:
            return jsonify({"error": "Se requiere el campo objectid"}), 400

        estado = data.get("ESTADO", "").upper()
        if estado not in ["ACTIVO", "SUSPENDIDO", "PROCESO"]:
            return jsonify({"error": "Valor inválido para ESTADO"}), 400

        accion = data.get("ACCION", "").upper()
        if accion not in ["MEDICION", "DESARROLLO", "ASIGNACION"]:
            return jsonify({"error": "Valor inválido para ACCION"}), 400

        datos_seguimiento = {
            "ESTADO": estado,
            "ACCION": accion,
            "DEPARTAMENTO": data.get("DEPARTAMENTO", ""),
            "GERENCIA": data.get("GERENCIA", ""),
            "Usuario": data.get("Usuario", "api_user"),
        }

        resultado = db_manager.agregar_seguimiento(
            g.db_conn, data["objectid"], datos_seguimiento
        )
        if resultado:
            return jsonify(
                {"success": True, "message": "Seguimiento agregado correctamente"}
            )
        else:
            return jsonify({"error": "No se pudo agregar el seguimiento"}), 500

    except BadRequest:
        return jsonify({"error": "JSON inválido"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/servicio_tecnico", methods=["GET"])
def get_servicios_tecnicos():
    objectid = request.args.get("objectid")
    filtro = f"OBJECTID_REF = {objectid}" if objectid else None
    servicios = db_manager.obtener_servicios_tecnicos(g.db_conn, filtro=filtro)

    if servicios is None:
        return jsonify({"error": "Error al obtener servicios técnicos"}), 500

    servicios_list = servicios.to_dict("records")
    for servicio in servicios_list:
        for key, value in servicio.items():
            if isinstance(value, datetime):
                servicio[key] = json_serial(value)

    return jsonify({"servicios_tecnicos": servicios_list})


@app.route("/api/servicio_tecnico", methods=["POST"])
def crear_servicio_tecnico():
    try:
        data = request.get_json()
        if "objectid" not in data:
            return jsonify({"error": "Se requiere el campo objectid"}), 400

        required_fields = [
            "TECNICO_RESPONSABLE",
            "CULTIVO",
            "PREPARACION_TIERRA",
            "RIESGO",
        ]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Se requiere el campo {field}"}), 400

        prep_tierra = data["PREPARACION_TIERRA"].upper()
        if prep_tierra not in ["CORTE", "CRUCE", "RASTRA"]:
            return jsonify({"error": "Valor inválido para PREPARACION_TIERRA"}), 400

        riesgo = data["RIESGO"].upper()
        if riesgo not in ["SECANO", "GRAVEDAD", "ASPERSION", "GOTEO"]:
            return jsonify({"error": "Valor inválido para RIESGO"}), 400

        datos_servicio = {
            "TECNICO_RESPONSABLE": data.get("TECNICO_RESPONSABLE", ""),
            "CULTIVO": data.get("CULTIVO", ""),
            "PREPARACION_TIERRA": prep_tierra,
            "RIESGO": riesgo,
            "CONTROL_PLAGAS": data.get("CONTROL_PLAGAS", False),
            "FERTILIZACION": data.get("FERTILIZACION", False),
            "COSECHA": data.get("COSECHA", False),
            "Usuario": data.get("Usuario", "api_user"),
        }

        if "FECHA_SIEMBRA" in data and data["FECHA_SIEMBRA"]:
            try:
                datos_servicio["FECHA_SIEMBRA"] = datetime.strptime(
                    data["FECHA_SIEMBRA"], "%Y-%m-%d"
                )
            except ValueError:
                return (
                    jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD"}),
                    400,
                )

        resultado = db_manager.agregar_servicio_tecnico(
            g.db_conn, data["objectid"], datos_servicio
        )
        if resultado:
            return jsonify(
                {"success": True, "message": "Servicio técnico agregado correctamente"}
            )
        else:
            return jsonify({"error": "No se pudo agregar el servicio técnico"}), 500

    except BadRequest:
        return jsonify({"error": "JSON inválido"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
