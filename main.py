import functions_framework
import logging
import requests
import pymysql
import pymysql.cursors
import json
import os
from datetime import datetime

# --- CONFIGURACIÓN DE APIS EXTERNAS ---
API_TOKEN_VERIFY = "https://api-verificacion-token-2946605267.us-central1.run.app"

def get_connection():
    try:
        conn = pymysql.connect(
            user="zeussafety-2024",
            password="ZeusSafety2025",
            db="Zeus_Safety_Data_Integration",
            unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute("SET time_zone = '-05:00'")
        return conn
    except Exception as e:
        logging.error(f"Error DB: {str(e)}")
        raise e

# --- LÓGICA DE ANALÍTICA (ACTUALIZADA: GENERAL 1 Y GENERAL 2 REACTIVO) ---

def obtener_metricas_dashboard(request, headers):
    conn = get_connection()
    f_inicio = request.args.get("inicio")
    f_fin = request.args.get("fin")
    tipo = request.args.get("tipo") 

    # Si no vienen fechas, pasamos None para que el procedimiento use sus valores por defecto
    f_inicio = f_inicio if f_inicio and f_inicio != "" else None
    f_fin = f_fin if f_fin and f_fin != "" else None

    try:
        with conn.cursor() as cursor:
            # --- REPORTE GENERAL 1 (Se mantiene igual) ---
            if tipo == "kpis":
                cursor.callproc('sp_Dashboard_KpisPrincipales', (f_inicio or "2025-01-01", f_fin or datetime.now().strftime('%Y-%m-%d')))
                result = cursor.fetchone()
            
            elif tipo == "productos":
                cursor.callproc('sp_Dashboard_TopProductos', (f_inicio or "2025-01-01", f_fin or datetime.now().strftime('%Y-%m-%d')))
                result = cursor.fetchall()
            
            elif tipo == "mensual":
                cursor.callproc('sp_Dashboard_VentasMensuales', (f_inicio or "2025-01-01", f_fin or datetime.now().strftime('%Y-%m-%d')))
                result = cursor.fetchall()
            
            elif tipo == "marketing":
                cursor.callproc('sp_Dashboard_MarketingAnalytics', (f_inicio or "2025-01-01", f_fin or datetime.now().strftime('%Y-%m-%d')))
                canales = cursor.fetchall()
                cursor.nextset()
                clasificacion = cursor.fetchall()
                cursor.nextset()
                lineas = cursor.fetchall()
                result = {"canales": canales, "clasificaciones": clasificacion, "lineas": lineas}

            # --- REPORTE GENERAL 2 (MOTOR REACTIVO ÚNICO) ---
            elif tipo == "full_reporte_2":
                nombre_cliente = request.args.get("cliente")
                nombre_region = request.args.get("region")  ### NUEVO: Recibe la región del front/postman

                # Limpieza de nulos para cliente
                if nombre_cliente in ["null", "", "undefined", None]:
                    nombre_cliente = None
                
                # Limpieza de nulos para región ### NUEVO ###
                if nombre_region in ["null", "", "undefined", None]:
                    nombre_region = None

                # Ejecutamos el procedimiento enviando CUATRO parámetros: cliente, region, inicio, fin
                # Asegúrate de que tu SP en MySQL ahora reciba estos 4 parámetros
                cursor.callproc('sp_Dashboard_ReporteGeneral2_Filtrado', (nombre_cliente, nombre_region, f_inicio, f_fin))
                
                # Definimos una función interna para capturar sets de forma segura
                def fetch_all_safe(c):
                    return c.fetchall() if c.description else []

                # 1. Ranking de Clientes (Se actualiza si hay filtro de región)
                ranking = fetch_all_safe(cursor)
                
                # 2. Productos
                productos = []
                if cursor.nextset(): productos = fetch_all_safe(cursor)
                
                # 3. Pagos
                pagos = []
                if cursor.nextset(): pagos = fetch_all_safe(cursor)
                
                # 4. Comprobantes
                comprobantes = []
                if cursor.nextset(): comprobantes = fetch_all_safe(cursor)
                
                # 5. Almacenes
                almacenes = []
                if cursor.nextset(): almacenes = fetch_all_safe(cursor)
                
                # 6. Regiones (Este se mantiene para mostrar el gráfico circular)
                regiones = []
                if cursor.nextset(): regiones = fetch_all_safe(cursor)
                
                # 7. Distritos
                distritos = []
                if cursor.nextset(): distritos = fetch_all_safe(cursor)

                result = {
                    "ranking": ranking,
                    "productos": productos,
                    "pagos": pagos,
                    "comprobantes": comprobantes,
                    "almacenes": almacenes,
                    "geografia": {"regiones": regiones, "distritos": distritos}
                }

            # Mantengo estos por compatibilidad si los necesitas por separado
            elif tipo == "clientes_compras":
                cursor.callproc('sp_Dashboard_ComprasPorCliente', (f_inicio or "2025-01-01", f_fin or datetime.now().strftime('%Y-%m-%d')))
                result = cursor.fetchall()

            else:
                return (json.dumps({"error": "Tipo no válido"}), 400, headers)

        return (json.dumps(result, default=str), 200, headers)
    finally:
        conn.close()

# --- LÓGICA DE VENTAS Y CLIENTES (SIN CAMBIOS) ---
# ... (Aquí va tu función gestionar_venta_completa igual que antes)

# --- PUNTO DE ENTRADA ---
@functions_framework.http
def reporte_ventas_online(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, PUT, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    }
    if request.method == "OPTIONS": return ("", 204, headers)
    
    auth = request.headers.get("Authorization")
    if not auth: return (json.dumps({"error": "No token"}), 401, headers)

    if request.method == "GET":
        if request.args.get("modo") == "dashboard":
            return obtener_metricas_dashboard(request, headers)
        
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM clientes_ventas ORDER BY ID_CLIENTE DESC")
                return (json.dumps(cursor.fetchall(), default=str), 200, headers)
        finally:
            conn.close()

    elif request.method == "POST":
        data = request.get_json()
        return gestionar_venta_completa(data, headers) if "cabecera" in data else insertar_cliente(data, headers)

    return (json.dumps({"error": "Método no permitido"}), 405, headers)