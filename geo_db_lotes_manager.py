import os
import sys
import pyodbc
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple


class GeoDBLotesManager:
    """
    Gestor de bases de datos geoespaciales para archivos Access (.mdb)
    """

    def __init__(self, db_path):
        """
        Inicializa el gestor de base de datos geoespaciales

        Args:
            db_path (str): Ruta completa al archivo .mdb
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def _get_available_drivers(self):
        """
        Detecta los drivers de Access disponibles en el sistema

        Returns:
            list: Lista de drivers disponibles
        """
        drivers = []
        try:
            for driver in pyodbc.drivers():
                if "Access" in driver:
                    drivers.append(driver)
        except Exception as e:
            print(f"Error al detectar drivers: {e}")
        return drivers

    def conectar(self):
        """
        Establece la conexión con la base de datos

        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario
        """
        try:
            # Verificar que el archivo exista
            if not os.path.exists(self.db_path):
                print(f"Error: La base de datos no existe en la ruta: {self.db_path}")
                return False

            # Detectar drivers disponibles
            drivers = self._get_available_drivers()
            if not drivers:
                print("Error: No se encontraron drivers de Access instalados.")
                print("Instala el driver ODBC para Microsoft Access.")
                return False

            print(f"Drivers detectados: {drivers}")

            # Intentar conectar con cada driver disponible
            for driver in drivers:
                try:
                    conn_str = f"DRIVER={{{driver}}};DBQ={self.db_path};"
                    print(f"Intentando conectar con: {conn_str}")

                    self.conn = pyodbc.connect(conn_str)
                    self.cursor = self.conn.cursor()
                    print(f"Conexión exitosa usando driver: {driver}")
                    return True
                except pyodbc.Error as e:
                    print(f"Error al conectar usando {driver}: {e}")

            # Si llegamos aquí, ningún driver funcionó
            print("No se pudo conectar con ningún driver disponible")

            # Información sobre arquitectura para ayudar a depurar
            print(f"Versión de Python: {sys.version}")
            print(
                f"Arquitectura de Python: {'64 bits' if sys.maxsize > 2**32 else '32 bits'}"
            )
            print(
                "Asegúrate de que la arquitectura de Python (32 o 64 bits) coincida con el driver de Access instalado"
            )

            return False
        except Exception as e:
            print(f"Error inesperado al conectar: {e}")
            return False

    def desconectar(self):
        """Cierra la conexión con la base de datos"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            self.cursor = None
            self.conn = None
            print("Conexión cerrada")
        except Exception as e:
            print(f"Error al desconectar: {e}")

    def listar_tablas(self):
        """
        Lista todas las tablas en la base de datos

        Returns:
            list: Lista de nombres de tablas
        """
        if not self.conn:
            if not self.conectar():
                return []

        tablas = []
        try:
            for tabla in self.cursor.tables(tableType="TABLE"):
                # Filtrar tablas del sistema (comienzan con "MSys")
                if not tabla.table_name.startswith("MSys"):
                    tablas.append(tabla.table_name)
            return tablas
        except pyodbc.Error as e:
            print(f"Error al listar tablas: {e}")
            return []

    def describir_tabla(self, nombre_tabla):
        """
        Describe la estructura de una tabla

        Args:
            nombre_tabla (str): Nombre de la tabla

        Returns:
            list: Información sobre las columnas de la tabla
        """
        if not self.conn:
            if not self.conectar():
                return []

        try:
            columnas = []
            for column in self.cursor.columns(table=nombre_tabla):
                columnas.append(
                    {
                        "nombre": column.column_name,
                        "tipo": column.type_name,
                        "nullable": column.nullable,
                        "tamaño": column.column_size,
                    }
                )
            return columnas
        except pyodbc.Error as e:
            print(f"Error al describir tabla '{nombre_tabla}': {e}")
            return []

    def obtener_lotes(self, limit=None, where=None):
        """
        Lee los datos de la tabla lotes_muestra

        Args:
            limit (int, optional): Limitar número de registros
            where (str, optional): Condición WHERE para filtrar datos

        Returns:
            pandas.DataFrame: DataFrame con los datos
        """
        if not self.conn:
            if not self.conectar():
                return None

        try:
            sql = "SELECT "
            if limit:
                sql += f"TOP {limit} "

            sql += "* FROM [lotes_muestra]"

            if where:
                sql += f" WHERE {where}"

            print(f"Ejecutando consulta: {sql}")
            return pd.read_sql(sql, self.conn)
        except pyodbc.Error as e:
            print(f"Error al leer tabla 'lotes_muestra': {e}")
            return None

    def crear_tabla_muestra_registro(self):
        """
        Crea una tabla muestra_registro para almacenar información adicional

        Returns:
            bool: True si se creó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Verificar si la tabla ya existe
            tablas = self.listar_tablas()
            if "muestra_registro" in tablas:
                print("La tabla 'muestra_registro' ya existe.")
                return False

            # Crear la tabla de registro
            sql = """
            CREATE TABLE [muestra_registro] (
                [ID_Registro] COUNTER PRIMARY KEY,
                [OBJECTID_REF] INTEGER,
                [RINCON] TEXT(50),
                [RC] TEXT(10),
                [Fecha_Registro] DATETIME,
                [Tipo_Muestra] TEXT(50),
                [Observaciones] MEMO,
                [Tecnico] TEXT(50),
                [Profundidad] DOUBLE,
                [Valor_Medicion] DOUBLE,
                [Unidades] TEXT(20),
                [Estado_Muestra] TEXT(20),
                [Validado] BIT,
                [Coord_X] DOUBLE,
                [Coord_Y] DOUBLE,
                [Altitud] DOUBLE,
                [Metodo_Muestreo] TEXT(100),
                [Equipo_Usado] TEXT(100),
                [Usuario_Sistema] TEXT(50),
                [Fecha_Procesamiento] DATETIME
            )
            """

            self.cursor.execute(sql)

            # Crear índice para la referencia
            self.cursor.execute(
                "CREATE INDEX [idx_OBJECTID_REF] ON [muestra_registro] ([OBJECTID_REF])"
            )

            self.conn.commit()
            print("Tabla 'muestra_registro' creada correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al crear tabla 'muestra_registro': {e}")
            self.conn.rollback()
            return False

    def agregar_registro_muestra(self, objectid_ref, datos):
        """
        Agrega un nuevo registro de muestra a la tabla muestra_registro

        Args:
            objectid_ref (int): OBJECTID de referencia en lotes_muestra
            datos (dict): Datos del registro

        Returns:
            bool: True si se agregó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Convertir a entero Python estándar si es necesario
            objectid_ref = int(objectid_ref)

            # Obtener información del lote referenciado
            query = f"SELECT ASOC, CODE FROM [lotes_muestra] WHERE [OBJECTID] = ?"
            self.cursor.execute(query, (objectid_ref,))
            lote = self.cursor.fetchone()

            if not lote:
                print(f"No se encontró lote con OBJECTID {objectid_ref}")
                return False

            # Extraer RINCON y RC (o los campos que correspondan en tu tabla)
            rincon = lote[0] if lote[0] else ""
            rc = lote[1] if lote[1] else ""

            # Preparar datos completos
            datos_completos = {
                "OBJECTID_REF": objectid_ref,
                "RINCON": rincon,
                "RC": rc,
                "Fecha_Registro": datetime.now(),
                "Usuario_Sistema": datos.get("Usuario", "Sistema"),
            }

            # Agregar el resto de los datos
            for campo, valor in datos.items():
                if campo != "Usuario":  # Ya se procesó
                    datos_completos[campo] = valor

            # Construir la consulta SQL
            campos = ", ".join([f"[{campo}]" for campo in datos_completos.keys()])
            placeholders = ", ".join(["?" for _ in datos_completos.keys()])
            valores = list(datos_completos.values())

            sql = f"INSERT INTO [muestra_registro] ({campos}) VALUES ({placeholders})"

            self.cursor.execute(sql, valores)
            self.conn.commit()
            print("Registro de muestra agregado correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al agregar registro de muestra: {e}")
            self.conn.rollback()
            return False

    def obtener_muestras(self, filtro=None):
        """
        Obtiene registros de muestras con opción de filtrado

        Args:
            filtro (str, optional): Condición WHERE para filtrar

        Returns:
            pandas.DataFrame: Muestras registradas
        """
        if not self.conn:
            if not self.conectar():
                return None

        try:
            sql = "SELECT * FROM [muestra_registro]"
            if filtro:
                sql += f" WHERE {filtro}"

            return pd.read_sql(sql, self.conn)
        except pyodbc.Error as e:
            print(f"Error al obtener muestras: {e}")
            return None

    # NUEVOS MÉTODOS PARA LAS TABLAS ADICIONALES
    def crear_tabla_titular(self):
        """
        Crea una tabla titular para almacenar información de los titulares de lotes

        Returns:
            bool: True si se creó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Verificar si la tabla ya existe
            tablas = self.listar_tablas()
            if "titular" in tablas:
                print("La tabla 'titular' ya existe.")
                return False

            # Crear la tabla de titular
            sql = """
            CREATE TABLE [titular] (
                [ID_Titular] COUNTER PRIMARY KEY,
                [CODIGO_LOTE] TEXT(50),
                [AC] TEXT(50),
                [TITULAR] TEXT(100),
                [CEDULA] TEXT(20),
                [FECHA_ASIGNACION] DATETIME,
                [OBJECTID_REF] INTEGER,
                [Usuario_Sistema] TEXT(50),
                [Fecha_Registro] DATETIME
            )
            """

            self.cursor.execute(sql)

            # Crear índice para la referencia
            self.cursor.execute(
                "CREATE INDEX [idx_OBJECTID_REF_titular] ON [titular] ([OBJECTID_REF])"
            )

            self.conn.commit()
            print("Tabla 'titular' creada correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al crear tabla 'titular': {e}")
            self.conn.rollback()
            return False

    def crear_tabla_seguimiento(self):
        """
        Crea una tabla seguimiento para almacenar información de seguimiento de lotes

        Returns:
            bool: True si se creó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Verificar si la tabla ya existe
            tablas = self.listar_tablas()
            if "seguimiento" in tablas:
                print("La tabla 'seguimiento' ya existe.")
                return False

            # Crear la tabla de seguimiento
            sql = """
            CREATE TABLE [seguimiento] (
                [ID_Seguimiento] COUNTER PRIMARY KEY,
                [OBJECTID_REF] INTEGER,
                [ESTADO] TEXT(50),
                [ACCION] TEXT(50),
                [DEPARTAMENTO] TEXT(100),
                [GERENCIA] TEXT(100),
                [Usuario_Sistema] TEXT(50),
                [Fecha_Registro] DATETIME
            )
            """

            self.cursor.execute(sql)

            # Crear índice para la referencia
            self.cursor.execute(
                "CREATE INDEX [idx_OBJECTID_REF_seg] ON [seguimiento] ([OBJECTID_REF])"
            )

            self.conn.commit()
            print("Tabla 'seguimiento' creada correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al crear tabla 'seguimiento': {e}")
            self.conn.rollback()
            return False

    def crear_tabla_servicio_tecnico(self):
        """
        Crea una tabla servicio_tecnico para almacenar información técnica de los cultivos

        Returns:
            bool: True si se creó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Verificar si la tabla ya existe
            tablas = self.listar_tablas()
            if "servicio_tecnico" in tablas:
                print("La tabla 'servicio_tecnico' ya existe.")
                return False

            # Crear la tabla de servicio técnico
            sql = """
            CREATE TABLE [servicio_tecnico] (
                [ID_Servicio] COUNTER PRIMARY KEY,
                [OBJECTID_REF] INTEGER,
                [TECNICO_RESPONSABLE] TEXT(100),
                [CULTIVO] TEXT(50),
                [FECHA_SIEMBRA] DATETIME,
                [PREPARACION_TIERRA] TEXT(50),
                [RIESGO] TEXT(50),
                [CONTROL_PLAGAS] BIT,
                [FERTILIZACION] BIT,
                [COSECHA] BIT,
                [Usuario_Sistema] TEXT(50),
                [Fecha_Registro] DATETIME
            )
            """

            self.cursor.execute(sql)

            # Crear índice para la referencia
            self.cursor.execute(
                "CREATE INDEX [idx_OBJECTID_REF_serv] ON [servicio_tecnico] ([OBJECTID_REF])"
            )

            self.conn.commit()
            print("Tabla 'servicio_tecnico' creada correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al crear tabla 'servicio_tecnico': {e}")
            self.conn.rollback()
            return False

    def agregar_titular(self, objectid_ref, datos):
        """
        Agrega un nuevo registro a la tabla titular

        Args:
            objectid_ref (int): OBJECTID de referencia en lotes_muestra
            datos (dict): Datos del titular con las claves:
                        CODIGO_LOTE, AC, TITULAR, CEDULA, FECHA_ASIGNACION

        Returns:
            bool: True si se agregó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Convertir a entero Python estándar si es necesario
            objectid_ref = int(objectid_ref)

            # Verificar que la tabla existe
            tablas = self.listar_tablas()
            if "titular" not in tablas:
                self.crear_tabla_titular()

            # Preparar datos completos
            datos_completos = {
                "OBJECTID_REF": objectid_ref,
                "CODIGO_LOTE": datos.get("CODIGO_LOTE", ""),
                "AC": datos.get("AC", ""),
                "TITULAR": datos.get("TITULAR", ""),
                "CEDULA": datos.get("CEDULA", ""),
                "FECHA_ASIGNACION": datos.get("FECHA_ASIGNACION", datetime.now()),
                "Usuario_Sistema": datos.get("Usuario", "Sistema"),
                "Fecha_Registro": datetime.now(),
            }

            # Construir la consulta SQL
            campos = ", ".join([f"[{campo}]" for campo in datos_completos.keys()])
            placeholders = ", ".join(["?" for _ in datos_completos.keys()])
            valores = list(datos_completos.values())

            sql = f"INSERT INTO [titular] ({campos}) VALUES ({placeholders})"

            self.cursor.execute(sql, valores)
            self.conn.commit()
            print("Registro de titular agregado correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al agregar registro de titular: {e}")
            self.conn.rollback()
            return False

    def agregar_seguimiento(self, objectid_ref, datos):
        """
        Agrega un nuevo registro a la tabla seguimiento

        Args:
            objectid_ref (int): OBJECTID de referencia en lotes_muestra
            datos (dict): Datos del seguimiento con las claves:
                        ESTADO, ACCION, DEPARTAMENTO, GERENCIA

        Returns:
            bool: True si se agregó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Convertir a entero Python estándar si es necesario
            objectid_ref = int(objectid_ref)

            # Verificar que la tabla existe
            tablas = self.listar_tablas()
            if "seguimiento" not in tablas:
                self.crear_tabla_seguimiento()

            # Validar el estado
            estado = datos.get("ESTADO", "").upper()
            if estado not in ["ACTIVO", "SUSPENDIDO", "PROCESO"]:
                print(
                    f"Error: ESTADO '{estado}' no válido. Debe ser ACTIVO, SUSPENDIDO o PROCESO"
                )
                return False

            # Validar la acción
            accion = datos.get("ACCION", "").upper()
            if accion not in ["MEDICION", "DESARROLLO", "ASIGNACION"]:
                print(
                    f"Error: ACCION '{accion}' no válida. Debe ser MEDICION, DESARROLLO o ASIGNACION"
                )
                return False

            # Preparar datos completos
            datos_completos = {
                "OBJECTID_REF": objectid_ref,
                "ESTADO": estado,
                "ACCION": accion,
                "DEPARTAMENTO": datos.get("DEPARTAMENTO", ""),
                "GERENCIA": datos.get("GERENCIA", ""),
                "Usuario_Sistema": datos.get("Usuario", "Sistema"),
                "Fecha_Registro": datetime.now(),
            }

            # Construir la consulta SQL
            campos = ", ".join([f"[{campo}]" for campo in datos_completos.keys()])
            placeholders = ", ".join(["?" for _ in datos_completos.keys()])
            valores = list(datos_completos.values())

            sql = f"INSERT INTO [seguimiento] ({campos}) VALUES ({placeholders})"

            self.cursor.execute(sql, valores)
            self.conn.commit()
            print("Registro de seguimiento agregado correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al agregar registro de seguimiento: {e}")
            self.conn.rollback()
            return False

    def agregar_servicio_tecnico(self, objectid_ref, datos):
        """
        Agrega un nuevo registro a la tabla servicio_tecnico

        Args:
            objectid_ref (int): OBJECTID de referencia en lotes_muestra
            datos (dict): Datos del servicio técnico con las claves:
                        TECNICO_RESPONSABLE, CULTIVO, FECHA_SIEMBRA,
                        PREPARACION_TIERRA, RIESGO, CONTROL_PLAGAS,
                        FERTILIZACION, COSECHA

        Returns:
            bool: True si se agregó correctamente
        """
        if not self.conn:
            if not self.conectar():
                return False

        try:
            # Convertir a entero Python estándar si es necesario
            objectid_ref = int(objectid_ref)

            # Verificar que la tabla existe
            tablas = self.listar_tablas()
            if "servicio_tecnico" not in tablas:
                self.crear_tabla_servicio_tecnico()

            # Validar preparación de tierra
            prep_tierra = datos.get("PREPARACION_TIERRA", "").upper()
            if prep_tierra not in ["CORTE", "CRUCE", "RASTRA"]:
                print(
                    f"Error: PREPARACION_TIERRA '{prep_tierra}' no válida. Debe ser CORTE, CRUCE o RASTRA"
                )
                return False

            # Validar riesgo
            riesgo = datos.get("RIESGO", "").upper()
            if riesgo not in ["SECANO", "GRAVEDAD", "ASPERSION", "GOTEO"]:
                print(
                    f"Error: RIESGO '{riesgo}' no válido. Debe ser SECANO, GRAVEDAD, ASPERSION o GOTEO"
                )
                return False

            # Preparar datos completos
            datos_completos = {
                "OBJECTID_REF": objectid_ref,
                "TECNICO_RESPONSABLE": datos.get("TECNICO_RESPONSABLE", ""),
                "CULTIVO": datos.get("CULTIVO", ""),
                "FECHA_SIEMBRA": datos.get("FECHA_SIEMBRA", datetime.now()),
                "PREPARACION_TIERRA": prep_tierra,
                "RIESGO": riesgo,
                "CONTROL_PLAGAS": bool(datos.get("CONTROL_PLAGAS", False)),
                "FERTILIZACION": bool(datos.get("FERTILIZACION", False)),
                "COSECHA": bool(datos.get("COSECHA", False)),
                "Usuario_Sistema": datos.get("Usuario", "Sistema"),
                "Fecha_Registro": datetime.now(),
            }

            # Construir la consulta SQL
            campos = ", ".join([f"[{campo}]" for campo in datos_completos.keys()])
            placeholders = ", ".join(["?" for _ in datos_completos.keys()])
            valores = list(datos_completos.values())

            sql = f"INSERT INTO [servicio_tecnico] ({campos}) VALUES ({placeholders})"

            self.cursor.execute(sql, valores)
            self.conn.commit()
            print("Registro de servicio técnico agregado correctamente.")
            return True

        except pyodbc.Error as e:
            print(f"Error al agregar registro de servicio técnico: {e}")
            self.conn.rollback()
            return False

    def obtener_titulares(self, filtro=None):
        """
        Obtiene registros de titulares con opción de filtrado

        Args:
            filtro (str, optional): Condición WHERE para filtrar

        Returns:
            pandas.DataFrame: Titulares registrados
        """
        if not self.conn:
            if not self.conectar():
                return None

        try:
            sql = "SELECT * FROM [titular]"
            if filtro:
                sql += f" WHERE {filtro}"

            return pd.read_sql(sql, self.conn)
        except pyodbc.Error as e:
            print(f"Error al obtener titulares: {e}")
            return None

    def obtener_seguimientos(self, filtro=None):
        """
        Obtiene registros de seguimientos con opción de filtrado

        Args:
            filtro (str, optional): Condición WHERE para filtrar

        Returns:
            pandas.DataFrame: Seguimientos registrados
        """
        if not self.conn:
            if not self.conectar():
                return None

        try:
            sql = "SELECT * FROM [seguimiento]"
            if filtro:
                sql += f" WHERE {filtro}"

            return pd.read_sql(sql, self.conn)
        except pyodbc.Error as e:
            print(f"Error al obtener seguimientos: {e}")
            return None

    def obtener_servicios_tecnicos(self, filtro=None):
        """
        Obtiene registros de servicios técnicos con opción de filtrado

        Args:
            filtro (str, optional): Condición WHERE para filtrar

        Returns:
            pandas.DataFrame: Servicios técnicos registrados
        """
        if not self.conn:
            if not self.conectar():
                return None

        try:
            sql = "SELECT * FROM [servicio_tecnico]"
            if filtro:
                sql += f" WHERE {filtro}"

            return pd.read_sql(sql, self.conn)
        except pyodbc.Error as e:
            print(f"Error al obtener servicios técnicos: {e}")
            return None
