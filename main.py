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
    # Filtros Comunes
    f_inicio = request.args.get("inicio")
    f_fin = request.args.get("fin")
    tipo = request.args.get("tipo") 

    # Función interna de limpieza rápida
    def n(val):
        return None if val in ["null", "", "undefined", "None", None] else val

    try:
        with conn.cursor() as cursor:
            # --- REPORTE GENERAL 1: GESTIÓN Y VENTAS (NUEVO MOTOR) ---
            if tipo == "full_reporte_1":
                p_prod = request.args.get("producto")
                p_mes = request.args.get("mes")
                p_canal = request.args.get("canal")
                p_clasi = request.args.get("clasificacion")
                p_linea = request.args.get("linea")

                cursor.callproc('sp_Dashboard_General1', (
                    n(p_prod), n(p_mes), n(p_canal), n(p_clasi), n(p_linea), f_inicio, f_fin
                ))

                def fetch_all_safe(c):
                    return c.fetchall() if c.description else []

                resumen = fetch_all_safe(cursor)
                ventas_mes = []
                if cursor.nextset(): ventas_mes = fetch_all_safe(cursor)
                prod_top = []
                if cursor.nextset(): prod_top = fetch_all_safe(cursor)
                canales = []
                if cursor.nextset(): canales = fetch_all_safe(cursor)
                clasificaciones = []
                if cursor.nextset(): clasificaciones = fetch_all_safe(cursor)
                lineas = []
                if cursor.nextset(): lineas = fetch_all_safe(cursor)

                result = {
                    "kpis": resumen[0] if resumen else {"total_generado": 0, "cantidad_ventas": 0},
                    "ventas_por_mes": ventas_mes,
                    "productos_top": prod_top,
                    "canales_venta": canales,
                    "clasificacion_pedidos": clasificaciones,
                    "lineas": lineas
                }

            # --- REPORTE GENERAL 2: OPERATIVO Y CLIENTES ---
            elif tipo == "full_reporte_2":
                c = request.args.get("cliente")
                r = request.args.get("region")
                p = request.args.get("pago")
                comp = request.args.get("comprobante")
                alm = request.args.get("almacen")

                cursor.callproc('sp_Dashboard_ReporteGeneral2_Filtrado', (
                    n(c), n(r), n(p), n(comp), n(alm), f_inicio, f_fin
                ))

                def fetch_all_safe(c):
                    return c.fetchall() if c.description else []

                ranking = fetch_all_safe(cursor)
                productos = []
                if cursor.nextset(): productos = fetch_all_safe(cursor)
                pagos = []
                if cursor.nextset(): pagos = fetch_all_safe(cursor)
                comprobantes = []
                if cursor.nextset(): comprobantes = fetch_all_safe(cursor)
                almacenes = []
                if cursor.nextset(): almacenes = fetch_all_safe(cursor)
                regiones = []
                if cursor.nextset(): regiones = fetch_all_safe(cursor)
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