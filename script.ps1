
$baseUrl = "http://localhost:5000/api"
$today = Get-Date

# Caso 1 - ObjectID 1: Cultivo exitoso de Maíz (histórico)
for ($i = 1; $i -le 5; $i++) {
    $fecha = ($today.AddDays(-200 + $i * 5)).ToString("yyyy-MM-dd")
    $titular = @{objectid = 1; CODIGO_LOTE = "LOT-1-A"; AC = "AC-1$i"; TITULAR = "Agricultor 1-$i"; CEDULA = "001-100000$i-0"; FECHA_ASIGNACION = $fecha; Usuario = "script_case1" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/titular" -Method POST -Body $titular -ContentType "application/json"

    $seguimiento = @{objectid = 1; ESTADO = "ACTIVO"; ACCION = "DESARROLLO"; DEPARTAMENTO = "Producción Maíz"; GERENCIA = "Zona Norte"; Usuario = "script_case1" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/seguimiento" -Method POST -Body $seguimiento -ContentType "application/json"

    $servicio = @{objectid = 1; TECNICO_RESPONSABLE = "Tec Maíz-$i"; CULTIVO = "Maíz"; FECHA_SIEMBRA = $fecha; PREPARACION_TIERRA = "RASTRA"; RIESGO = "ASPERSION"; CONTROL_PLAGAS = $true; FERTILIZACION = $true; COSECHA = $true; Usuario = "script_case1" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/servicio_tecnico" -Method POST -Body $servicio -ContentType "application/json"
}

# Caso 2 - ObjectID 2: Problemas en Arroz (últimos 3 meses)
for ($i = 1; $i -le 5; $i++) {
    $fecha = ($today.AddDays(-90 + $i * 3)).ToString("yyyy-MM-dd")
    $titular = @{objectid = 2; CODIGO_LOTE = "LOT-2-B"; AC = "AC-2$i"; TITULAR = "Productor 2-$i"; CEDULA = "002-200000$i-0"; FECHA_ASIGNACION = $fecha; Usuario = "script_case2" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/titular" -Method POST -Body $titular -ContentType "application/json"

    $seguimiento = @{objectid = 2; ESTADO = "SUSPENDIDO"; ACCION = "ASIGNACION"; DEPARTAMENTO = "Depto. Arroz"; GERENCIA = "Zona Este"; Usuario = "script_case2" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/seguimiento" -Method POST -Body $seguimiento -ContentType "application/json"

    $fert = if ($i -le 2) { $false } else { $true }
    $servicio = @{objectid = 2; TECNICO_RESPONSABLE = "Tec Arroz-$i"; CULTIVO = "Arroz"; FECHA_SIEMBRA = $fecha; PREPARACION_TIERRA = "CRUCE"; RIESGO = "GOTEO"; CONTROL_PLAGAS = $true; FERTILIZACION = $fert; COSECHA = $false; Usuario = "script_case2" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/servicio_tecnico" -Method POST -Body $servicio -ContentType "application/json"
}

# Caso 3 - ObjectID 3: Diversos cultivos recientes sin cosecha
$cultivos = @("Yuca", "Habichuela", "Sorgo", "Maíz", "Arroz")
for ($i = 1; $i -le 5; $i++) {
    $fecha = ($today.AddDays(-30 + $i)).ToString("yyyy-MM-dd")
    $titular = @{objectid = 3; CODIGO_LOTE = "LOT-3-C"; AC = "AC-3$i"; TITULAR = "Lote Diverso 3-$i"; CEDULA = "003-300000$i-0"; FECHA_ASIGNACION = $fecha; Usuario = "script_case3" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/titular" -Method POST -Body $titular -ContentType "application/json"

    $seguimiento = @{objectid = 3; ESTADO = "PROCESO"; ACCION = "MEDICION"; DEPARTAMENTO = "Mixto"; GERENCIA = "Zona Sur"; Usuario = "script_case3" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/seguimiento" -Method POST -Body $seguimiento -ContentType "application/json"

    $servicio = @{objectid = 3; TECNICO_RESPONSABLE = "Tec Diverso-$i"; CULTIVO = $cultivos[$i - 1]; FECHA_SIEMBRA = $fecha; PREPARACION_TIERRA = "CORTE"; RIESGO = "SECANO"; CONTROL_PLAGAS = $false; FERTILIZACION = $true; COSECHA = $false; Usuario = "script_case3" } | ConvertTo-Json -Depth 3
    Invoke-RestMethod "$baseUrl/servicio_tecnico" -Method POST -Body $servicio -ContentType "application/json"
}

Write-Host "✅ Casos 1, 2 y 3 registrados con fechas realistas para análisis IA."
