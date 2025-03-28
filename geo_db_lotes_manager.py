# geo_db_lotes_manager.py
import pyodbc
import pandas as pd


class GeoDBLotesManager:
    def __init__(self, db_path):
        self.db_path = db_path

    def conectar(self):
        connection_string = (
            f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.db_path};"
        )
        self.conn = pyodbc.connect(connection_string)

    def desconectar(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            self.conn = None

    def listar_tablas(self, conn):
        try:
            cursor = conn.cursor()
            tablas = [row.table_name for row in cursor.tables(tableType="TABLE")]
            cursor.close()
            return tablas
        except Exception as e:
            print(f"Error al listar tablas: {e}")
            return []

    def obtener_columnas_tabla(self, conn, tabla):
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {tabla} WHERE 1=0")
            columnas = [column[0] for column in cursor.description]
            cursor.close()
            return columnas
        except Exception as e:
            print(f"Error al obtener columnas de {tabla}: {e}")
            return []

    def obtener_lotes(self, conn, where=None):
        try:
            query = "SELECT * FROM lotes_muestra"
            if where:
                query += f" WHERE {where}"
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Error al obtener lotes: {e}")
            return None

    def obtener_titulares(self, conn, filtro=None):
        try:
            query = "SELECT * FROM titular"
            if filtro:
                query += f" WHERE {filtro}"
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Error al obtener titulares: {e}")
            return None

    # def agregar_titular(self, conn, objectid, datos):
    #     try:
    #         cursor = conn.cursor()
    #         columns = ", ".join(datos.keys())
    #         placeholders = ", ".join(["?"] * len(datos))
    #         values = list(datos.values())
    #         query = f"INSERT INTO titular (OBJECTID_REF, {columns}) VALUES (?, {placeholders})"
    #         cursor.execute(query, [objectid] + values)
    #         conn.commit()
    #         cursor.close()
    #         return True
    #     except Exception as e:
    #         print(f"Error al agregar titular: {e}")
    #         return False

    def obtener_seguimientos(self, conn, filtro=None):
        try:
            query = "SELECT * FROM seguimiento"
            if filtro:
                query += f" WHERE {filtro}"
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Error al obtener seguimientos: {e}")
            return None

    # def agregar_seguimiento(self, conn, objectid, datos):
    #     try:
    #         cursor = conn.cursor()
    #         columns = ", ".join(datos.keys())
    #         placeholders = ", ".join(["?"] * len(datos))
    #         values = list(datos.values())
    #         query = f"INSERT INTO seguimiento (OBJECTID_REF, {columns}) VALUES (?, {placeholders})"
    #         cursor.execute(query, [objectid] + values)
    #         conn.commit()
    #         cursor.close()
    #         return True
    #     except Exception as e:
    #         print(f"Error al agregar seguimiento: {e}")
    #         return False

    def obtener_servicios_tecnicos(self, conn, filtro=None):
        try:
            query = "SELECT * FROM servicio_tecnico"
            if filtro:
                query += f" WHERE {filtro}"
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Error al obtener servicios técnicos: {e}")
            return None

    # def agregar_servicio_tecnico(self, conn, objectid, datos):
    #     try:
    #         cursor = conn.cursor()
    #         columns = ", ".join(datos.keys())
    #         placeholders = ", ".join(["?"] * len(datos))
    #         values = list(datos.values())
    #         query = f"INSERT INTO servicio_tecnico (OBJECTID_REF, {columns}) VALUES (?, {placeholders})"
    #         cursor.execute(query, [objectid] + values)
    #         conn.commit()
    #         cursor.close()
    #         return True
    #     except Exception as e:
    #         print(f"Error al agregar servicio técnico: {e}")
    #         return False
    def agregar_titular(self, conn, objectid, datos):
        try:
            columnas_validas = self.obtener_columnas_tabla(conn, "titular")
            datos_filtrados = {k: v for k, v in datos.items() if k in columnas_validas}

            if not datos_filtrados:
                print("No hay columnas válidas para insertar en 'titular'")
                return False

            columns = ", ".join(datos_filtrados.keys())
            placeholders = ", ".join(["?"] * len(datos_filtrados))
            values = list(datos_filtrados.values())

            query = f"INSERT INTO titular (OBJECTID_REF, {columns}) VALUES (?, {placeholders})"
            cursor = conn.cursor()
            cursor.execute(query, [objectid] + values)
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"Error al agregar titular: {e}")
            return False

    def agregar_seguimiento(self, conn, objectid, datos):
        try:
            columnas_validas = self.obtener_columnas_tabla(conn, "seguimiento")
            datos_filtrados = {k: v for k, v in datos.items() if k in columnas_validas}

            if not datos_filtrados:
                print("No hay columnas válidas para insertar en 'seguimiento'")
                return False

            columns = ", ".join(datos_filtrados.keys())
            placeholders = ", ".join(["?"] * len(datos_filtrados))
            values = list(datos_filtrados.values())

            query = f"INSERT INTO seguimiento (OBJECTID_REF, {columns}) VALUES (?, {placeholders})"
            cursor = conn.cursor()
            cursor.execute(query, [objectid] + values)
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"Error al agregar seguimiento: {e}")
            return False

    def agregar_servicio_tecnico(self, conn, objectid, datos):
        try:
            columnas_validas = self.obtener_columnas_tabla(conn, "servicio_tecnico")
            datos_filtrados = {k: v for k, v in datos.items() if k in columnas_validas}

            if not datos_filtrados:
                print("No hay columnas válidas para insertar en 'servicio_tecnico'")
                return False

            columns = ", ".join(datos_filtrados.keys())
            placeholders = ", ".join(["?"] * len(datos_filtrados))
            values = list(datos_filtrados.values())

            query = f"INSERT INTO servicio_tecnico (OBJECTID_REF, {columns}) VALUES (?, {placeholders})"
            cursor = conn.cursor()
            cursor.execute(query, [objectid] + values)
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"Error al agregar servicio técnico: {e}")
            return False

    def verificar_y_crear_tablas(self, conn):
        # """
        # Verifica si las tablas necesarias existen, y las crea si no se encuentran.
        # """
        try:
            tablas_existentes = self.listar_tablas(conn)
            tablas_requeridas = ["titular", "seguimiento", "servicio_tecnico"]
            tablas_por_crear = [
                tabla for tabla in tablas_requeridas if tabla not in tablas_existentes
            ]

            if not tablas_por_crear:
                print("Todas las tablas requeridas ya existen")
                return True

            cursor = conn.cursor()

            # Crear tabla titular si no existe
            if "titular" in tablas_por_crear:
                print("Creando tabla 'titular'...")
                cursor.execute(
                    """
                    CREATE TABLE titular (
                        ID COUNTER PRIMARY KEY,
                        OBJECTID_REF INTEGER NOT NULL,
                        CODIGO_LOTE TEXT(50),
                        AC TEXT(50),
                        TITULAR TEXT(100),
                        CEDULA TEXT(20),
                        FECHA_ASIGNACION DATETIME,
                        Usuario TEXT(50),
                        Fecha_Registro DATETIME DEFAULT Now()
                    )
                """
                )

            # Crear tabla seguimiento si no existe
            if "seguimiento" in tablas_por_crear:
                print("Creando tabla 'seguimiento'...")
                cursor.execute(
                    """
                    CREATE TABLE seguimiento (
                        ID COUNTER PRIMARY KEY,
                        OBJECTID_REF INTEGER NOT NULL,
                        ESTADO TEXT(20),
                        ACCION TEXT(20),
                        DEPARTAMENTO TEXT(50),
                        GERENCIA TEXT(50),
                        Usuario TEXT(50),
                        Fecha_Registro DATETIME DEFAULT Now()
                    )
                """
                )

            # Crear tabla servicio_tecnico si no existe
            if "servicio_tecnico" in tablas_por_crear:
                print("Creando tabla 'servicio_tecnico'...")
                cursor.execute(
                    """
                    CREATE TABLE servicio_tecnico (
                        ID COUNTER PRIMARY KEY,
                        OBJECTID_REF INTEGER NOT NULL,
                        TECNICO_RESPONSABLE TEXT(100),
                        CULTIVO TEXT(50),
                        PREPARACION_TIERRA TEXT(20),
                        RIESGO TEXT(20),
                        CONTROL_PLAGAS BIT DEFAULT 0,
                        FERTILIZACION BIT DEFAULT 0,
                        COSECHA BIT DEFAULT 0,
                        FECHA_SIEMBRA DATETIME,
                        Usuario TEXT(50),
                        Fecha_Registro DATETIME DEFAULT Now()
                    )
                """
                )

            conn.commit()
            cursor.close()
            print(f"Se crearon {len(tablas_por_crear)} tablas")
            return True
        except Exception as e:
            print(f"Error al crear tablas: {e}")
            return False
