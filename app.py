import os
import json
from flask import Flask, request, jsonify
from datetime import datetime
from geo_db_lotes_manager import GeoDBLotesManager
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

# Obtener la ruta del archivo de base de datos
script_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(script_dir, "./IAD.mdb")

# Instancia global del gestor de BD
db_manager = GeoDBLotesManager(db_path)


# Función auxiliar para manejar fechas en JSON
def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


# Middleware para establecer conexión a la BD
@app.before_request
def before_request():
    db_manager.conectar()


# Middleware para cerrar conexión a la BD
@app.teardown_request
def teardown_request(exception):
    db_manager.desconectar()


# Endpoint para verificar que la API está funcionando
@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify(
        {
            "status": "ok",
            "message": "API está funcionando correctamente",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


# Endpoint para listar las tablas disponibles
@app.route("/api/tablas", methods=["GET"])
def listar_tablas():
    tablas = db_manager.listar_tablas()
    return jsonify({"tablas": tablas})


#########################################
# Endpoints para la tabla TITULAR
#########################################


# Obtener todos los titulares
@app.route("/api/titular", methods=["GET"])
def get_titulares():
    # Opcionalmente, se puede filtrar por OBJECTID
    objectid = request.args.get("objectid")

    if objectid:
        filtro = f"OBJECTID_REF = {objectid}"
        titulares = db_manager.obtener_titulares(filtro=filtro)
    else:
        titulares = db_manager.obtener_titulares()

    if titulares is None:
        return jsonify({"error": "Error al obtener titulares"}), 500

    # Convertir DataFrame a lista de diccionarios
    titulares_list = titulares.to_dict("records")

    # Serializar fechas
    for titular in titulares_list:
        for key, value in titular.items():
            if isinstance(value, datetime):
                titular[key] = json_serial(value)

    return jsonify({"titulares": titulares_list})


# Crear un nuevo titular
@app.route("/api/titular", methods=["POST"])
def crear_titular():
    try:
        data = request.get_json()

        # Validar que se proporcione un OBJECTID
        if "objectid" not in data:
            return jsonify({"error": "Se requiere el campo objectid"}), 400

        objectid = data["objectid"]

        # Preparar datos para la inserción
        datos_titular = {
            "CODIGO_LOTE": data.get("CODIGO_LOTE", ""),
            "AC": data.get("AC", ""),
            "TITULAR": data.get("TITULAR", ""),
            "CEDULA": data.get("CEDULA", ""),
            "Usuario": data.get("Usuario", "api_user"),
        }

        # Manejar la fecha si se proporciona
        if "FECHA_ASIGNACION" in data and data["FECHA_ASIGNACION"]:
            try:
                fecha = datetime.strptime(data["FECHA_ASIGNACION"], "%Y-%m-%d")
                datos_titular["FECHA_ASIGNACION"] = fecha
            except ValueError:
                return (
                    jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD"}),
                    400,
                )

        # Intentar agregar el registro
        resultado = db_manager.agregar_titular(objectid, datos_titular)

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


#########################################
# Endpoints para la tabla SEGUIMIENTO
#########################################


# Obtener todos los seguimientos
@app.route("/api/seguimiento", methods=["GET"])
def get_seguimientos():
    # Opcionalmente, se puede filtrar por OBJECTID
    objectid = request.args.get("objectid")

    if objectid:
        filtro = f"OBJECTID_REF = {objectid}"
        seguimientos = db_manager.obtener_seguimientos(filtro=filtro)
    else:
        seguimientos = db_manager.obtener_seguimientos()

    if seguimientos is None:
        return jsonify({"error": "Error al obtener seguimientos"}), 500

    # Convertir DataFrame a lista de diccionarios
    seguimientos_list = seguimientos.to_dict("records")

    # Serializar fechas
    for seguimiento in seguimientos_list:
        for key, value in seguimiento.items():
            if isinstance(value, datetime):
                seguimiento[key] = json_serial(value)

    return jsonify({"seguimientos": seguimientos_list})


# Crear un nuevo seguimiento
@app.route("/api/seguimiento", methods=["POST"])
def crear_seguimiento():
    try:
        data = request.get_json()

        # Validar que se proporcione un OBJECTID
        if "objectid" not in data:
            return jsonify({"error": "Se requiere el campo objectid"}), 400

        objectid = data["objectid"]

        # Validar los campos obligatorios
        if "ESTADO" not in data:
            return jsonify({"error": "Se requiere el campo ESTADO"}), 400

        if "ACCION" not in data:
            return jsonify({"error": "Se requiere el campo ACCION"}), 400

        # Validar valores permitidos
        estado = data["ESTADO"].upper()
        if estado not in ["ACTIVO", "SUSPENDIDO", "PROCESO"]:
            return (
                jsonify(
                    {
                        "error": "Valor inválido para ESTADO. Valores permitidos: ACTIVO, SUSPENDIDO, PROCESO"
                    }
                ),
                400,
            )

        accion = data["ACCION"].upper()
        if accion not in ["MEDICION", "DESARROLLO", "ASIGNACION"]:
            return (
                jsonify(
                    {
                        "error": "Valor inválido para ACCION. Valores permitidos: MEDICION, DESARROLLO, ASIGNACION"
                    }
                ),
                400,
            )

        # Preparar datos para la inserción
        datos_seguimiento = {
            "ESTADO": estado,
            "ACCION": accion,
            "DEPARTAMENTO": data.get("DEPARTAMENTO", ""),
            "GERENCIA": data.get("GERENCIA", ""),
            "Usuario": data.get("Usuario", "api_user"),
        }

        # Intentar agregar el registro
        resultado = db_manager.agregar_seguimiento(objectid, datos_seguimiento)

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


#########################################
# Endpoints para la tabla SERVICIO_TECNICO
#########################################


# Obtener todos los servicios técnicos
@app.route("/api/servicio_tecnico", methods=["GET"])
def get_servicios_tecnicos():
    # Opcionalmente, se puede filtrar por OBJECTID
    objectid = request.args.get("objectid")

    if objectid:
        filtro = f"OBJECTID_REF = {objectid}"
        servicios = db_manager.obtener_servicios_tecnicos(filtro=filtro)
    else:
        servicios = db_manager.obtener_servicios_tecnicos()

    if servicios is None:
        return jsonify({"error": "Error al obtener servicios técnicos"}), 500

    # Convertir DataFrame a lista de diccionarios
    servicios_list = servicios.to_dict("records")

    # Serializar fechas
    for servicio in servicios_list:
        for key, value in servicio.items():
            if isinstance(value, datetime):
                servicio[key] = json_serial(value)

    return jsonify({"servicios_tecnicos": servicios_list})


# Crear un nuevo servicio técnico
@app.route("/api/servicio_tecnico", methods=["POST"])
def crear_servicio_tecnico():
    try:
        data = request.get_json()

        # Validar que se proporcione un OBJECTID
        if "objectid" not in data:
            return jsonify({"error": "Se requiere el campo objectid"}), 400

        objectid = data["objectid"]

        # Validar campos obligatorios
        required_fields = [
            "TECNICO_RESPONSABLE",
            "CULTIVO",
            "PREPARACION_TIERRA",
            "RIESGO",
        ]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Se requiere el campo {field}"}), 400

        # Validar valores permitidos
        prep_tierra = data["PREPARACION_TIERRA"].upper()
        if prep_tierra not in ["CORTE", "CRUCE", "RASTRA"]:
            return (
                jsonify(
                    {
                        "error": "Valor inválido para PREPARACION_TIERRA. Valores permitidos: CORTE, CRUCE, RASTRA"
                    }
                ),
                400,
            )

        riesgo = data["RIESGO"].upper()
        if riesgo not in ["SECANO", "GRAVEDAD", "ASPERSION", "GOTEO"]:
            return (
                jsonify(
                    {
                        "error": "Valor inválido para RIESGO. Valores permitidos: SECANO, GRAVEDAD, ASPERSION, GOTEO"
                    }
                ),
                400,
            )

        # Preparar datos para la inserción
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

        # Manejar la fecha si se proporciona
        if "FECHA_SIEMBRA" in data and data["FECHA_SIEMBRA"]:
            try:
                fecha = datetime.strptime(data["FECHA_SIEMBRA"], "%Y-%m-%d")
                datos_servicio["FECHA_SIEMBRA"] = fecha
            except ValueError:
                return (
                    jsonify({"error": "Formato de fecha inválido. Use YYYY-MM-DD"}),
                    400,
                )

        # Intentar agregar el registro
        resultado = db_manager.agregar_servicio_tecnico(objectid, datos_servicio)

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


# Punto de entrada principal
if __name__ == "__main__":
    # Inicializar tablas si no existen (opcional)
    db_manager.conectar()
    db_manager.crear_tabla_titular()
    db_manager.crear_tabla_seguimiento()
    db_manager.crear_tabla_servicio_tecnico()
    db_manager.desconectar()

    # Iniciar servidor web
    app.run(debug=True, port=5000)
