import os
import pyodbc
from geo_db_lotes_manager import GeoDBLotesManager

print("waracha", pyodbc.drivers())


def main():
    # Obtener la ruta del archivo de base de datos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(
        script_dir, "./IAD.mdb"
    )  # Cambia "IAD.mdb" por el nombre de tu archivo

    # Crear instancia del gestor
    db = GeoDBLotesManager(db_path)

    # Conectar a la base de datos
    if not db.conectar():
        print("No se pudo establecer la conexión con la base de datos.")
        return

    # Listar tablas disponibles
    tablas = db.listar_tablas()
    print("\nTablas disponibles:")
    for tabla in tablas:
        print(f"  - {tabla}")

    # Verificar si existe la tabla lotes_muestra
    if "lotes_muestra" in tablas:
        # Describir estructura de la tabla
        estructura = db.describir_tabla("lotes_muestra")
        print("\nEstructura de la tabla 'lotes_muestra':")
        for columna in estructura:
            print(f"  - {columna['nombre']} ({columna['tipo']})")

        # Obtener algunos registros de muestra
        lotes = db.obtener_lotes(limit=5)
        if lotes is not None and not lotes.empty:
            print("\nEjemplo de lotes (primeros 5):")
            print(lotes)

            # Crear tabla muestra_registro si no existe
            print("\nCreando tabla 'muestra_registro'...")
            db.crear_tabla_muestra_registro()

            # Si hay al menos un lote, agregar un registro de muestra de ejemplo
            if len(lotes) > 0:
                # Convertir de numpy.int64 a int Python estándar
                objectid = int(lotes.iloc[0]["OBJECTID"])

                print(
                    f"\nAgregando registro de muestra para el lote con OBJECTID {objectid}..."
                )
                muestra = {
                    "Tipo_Muestra": "Suelo",
                    "Profundidad": 30.5,
                    "Valor_Medicion": 45.7,
                    "Unidades": "ppm",
                    "Observaciones": "Muestra de prueba",
                    "Tecnico": "Juan Pérez",
                    "Estado_Muestra": "Procesado",
                    "Validado": True,
                    "Coord_X": -76.5234,
                    "Coord_Y": 3.4567,
                    "Altitud": 950.5,
                    "Usuario": "usuario_test",
                }

                db.agregar_registro_muestra(objectid, muestra)

                # Verificar que se haya agregado correctamente
                print("\nConsultando registros de muestra:")
                muestras = db.obtener_muestras()
                if muestras is not None and not muestras.empty:
                    print(muestras)
                else:
                    print("No se encontraron registros de muestra.")
        else:
            print("No se encontraron registros en la tabla 'lotes_muestra'.")
    else:
        print("La tabla 'lotes_muestra' no existe en la base de datos.")

    # Desconectar
    db.desconectar()


if __name__ == "__main__":
    main()
