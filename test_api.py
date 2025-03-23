import requests
import json
from datetime import datetime

# URL base (asegúrate de que coincida con tu servidor Flask)
BASE_URL = "http://localhost:5000/api"


# Función para verificar que la API está funcionando
def test_health():
    response = requests.get(f"{BASE_URL}/health")
    print("Comprobación de salud:")
    print(response.json())
    print()


# Función para obtener las tablas disponibles
def test_tablas():
    response = requests.get(f"{BASE_URL}/tablas")
    print("Tablas disponibles:")
    print(response.json())
    print()


# Función para probar el endpoint de titular
def test_titular():
    # Obtener titulares
    response = requests.get(f"{BASE_URL}/titular")
    print("Titulares existentes:")
    print(json.dumps(response.json(), indent=2))
    print()

    # Crear un nuevo titular
    new_titular = {
        "objectid": 1,  # Asegúrate de que este OBJECTID exista en lotes_muestra
        "CODIGO_LOTE": "LOT-2023-002",
        "AC": "AC-789",
        "TITULAR": "María González López",
        "CEDULA": "V-87654321",
        "FECHA_ASIGNACION": datetime.now().strftime("%Y-%m-%d"),
        "Usuario": "usuario_test_api",
    }

    response = requests.post(
        f"{BASE_URL}/titular",
        json=new_titular,
        headers={"Content-Type": "application/json"},
    )

    print("Resultado de crear titular:")
    print(response.json())
    print()

    # Verificar que se haya creado correctamente
    response = requests.get(f"{BASE_URL}/titular")
    print("Titulares después de la creación:")
    print(json.dumps(response.json(), indent=2))
    print()


# Función para probar el endpoint de seguimiento
def test_seguimiento():
    # Obtener seguimientos
    response = requests.get(f"{BASE_URL}/seguimiento")
    print("Seguimientos existentes:")
    print(json.dumps(response.json(), indent=2))
    print()

    # Crear un nuevo seguimiento
    new_seguimiento = {
        "objectid": 1,  # Asegúrate de que este OBJECTID exista en lotes_muestra
        "ESTADO": "ACTIVO",
        "ACCION": "MEDICION",
        "DEPARTAMENTO": "Departamento de Desarrollo",
        "GERENCIA": "Gerencia de Proyectos",
        "Usuario": "usuario_test_api",
    }

    response = requests.post(
        f"{BASE_URL}/seguimiento",
        json=new_seguimiento,
        headers={"Content-Type": "application/json"},
    )

    print("Resultado de crear seguimiento:")
    print(response.json())
    print()

    # Verificar que se haya creado correctamente
    response = requests.get(f"{BASE_URL}/seguimiento")
    print("Seguimientos después de la creación:")
    print(json.dumps(response.json(), indent=2))
    print()


# Función para probar el endpoint de servicio_tecnico
def test_servicio_tecnico():
    # Obtener servicios técnicos
    response = requests.get(f"{BASE_URL}/servicio_tecnico")
    print("Servicios técnicos existentes:")
    print(json.dumps(response.json(), indent=2))
    print()

    # Crear un nuevo servicio técnico
    new_servicio = {
        "objectid": 1,  # Asegúrate de que este OBJECTID exista en lotes_muestra
        "TECNICO_RESPONSABLE": "Carlos Ramírez",
        "CULTIVO": "Trigo",
        "FECHA_SIEMBRA": datetime.now().strftime("%Y-%m-%d"),
        "PREPARACION_TIERRA": "CORTE",
        "RIESGO": "GOTEO",
        "CONTROL_PLAGAS": True,
        "FERTILIZACION": True,
        "COSECHA": False,
        "Usuario": "usuario_test_api",
    }

    response = requests.post(
        f"{BASE_URL}/servicio_tecnico",
        json=new_servicio,
        headers={"Content-Type": "application/json"},
    )

    print("Resultado de crear servicio técnico:")
    print(response.json())
    print()

    # Verificar que se haya creado correctamente
    response = requests.get(f"{BASE_URL}/servicio_tecnico")
    print("Servicios técnicos después de la creación:")
    print(json.dumps(response.json(), indent=2))
    print()


# Programa principal
if __name__ == "__main__":
    print("Iniciando pruebas de la API...\n")

    # Ejecutar pruebas
    test_health()
    test_tablas()
    test_titular()
    test_seguimiento()
    test_servicio_tecnico()

    print("Pruebas completadas.")
