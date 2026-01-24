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

# --- LÓGICA DE ANALÍTICA (ACTUALIZADA CON REPORTES 2) ---

def obtener_metricas_dashboard(request, headers):
    conn = get_connection()
    f_inicio = request.args.get("inicio", "2025-01-01")
    f_fin = request.args.get("fin", datetime.now().strftime('%Y-%m-%d'))
    tipo = request.args.get("tipo") 

    try:
        with conn.cursor() as cursor:
            # --- REPORTE GENERAL 1 ---
            if tipo == "kpis":
                cursor.callproc('sp_Dashboard_KpisPrincipales', (f_inicio, f_fin))
                result = cursor.fetchone()
            
            elif tipo == "productos":
                cursor.callproc('sp_Dashboard_TopProductos', (f_inicio, f_fin))
                result = cursor.fetchall()
            
            elif tipo == "mensual":
                cursor.callproc('sp_Dashboard_VentasMensuales', (f_inicio, f_fin))
                result = cursor.fetchall()
            
            elif tipo == "marketing":
                cursor.callproc('sp_Dashboard_MarketingAnalytics', (f_inicio, f_fin))
                canales = cursor.fetchall()
                cursor.nextset()
                clasificacion = cursor.fetchall()
                cursor.nextset()
                lineas = cursor.fetchall()
                result = {"canales": canales, "clasificaciones": clasificacion, "lineas": lineas}

            # --- REPORTE GENERAL 2 (NUEVOS) ---
            elif tipo == "clientes_compras":
                cursor.callproc('sp_Dashboard_ComprasPorCliente', (f_inicio, f_fin))
                result = cursor.fetchall()

            elif tipo == "detalle_producto_cliente":
                nombre_cliente = request.args.get("cliente")
                if not nombre_cliente: return (json.dumps({"error": "Falta cliente"}), 400, headers)
                cursor.callproc('sp_Dashboard_ProductosPorCliente', (nombre_cliente, f_inicio, f_fin))
                result = cursor.fetchall()

            elif tipo == "pagos":
                cursor.callproc('sp_Dashboard_MetodosPago', (f_inicio, f_fin))
                result = cursor.fetchall()

            elif tipo == "geografia":
                cursor.callproc('sp_Dashboard_VentasGeograficas', (f_inicio, f_fin))
                regiones = cursor.fetchall()
                cursor.nextset()
                distritos = cursor.fetchall()
                result = {"regiones": regiones, "distritos": distritos}

            elif tipo == "comprobantes_almacen":
                cursor.callproc('sp_Dashboard_ComprobantesYAlmacen', (f_inicio, f_fin))
                comprobantes = cursor.fetchall()
                cursor.nextset()
                almacenes = cursor.fetchall()
                result = {"comprobantes": comprobantes, "almacenes": almacenes}
            
            else:
                return (json.dumps({"error": "Tipo no válido"}), 400, headers)

        return (json.dumps(result, default=str), 200, headers)
    finally:
        conn.close()

# --- LÓGICA DE VENTAS Y CLIENTES ---

def gestionar_venta_completa(data, headers):
    conn = get_connection()
    try:
        cab = data.get('cabecera')
        detalles = data.get('detalles')
        fecha_auto = datetime.now().strftime('%Y-%m-%d')
        with conn.cursor() as cursor:
            id_cliente = cab.get("id_cliente")
            if not id_cliente:
                cursor.execute("SELECT ID_CLIENTE FROM clientes_ventas WHERE CLIENTE = %s", (cab.get("cliente"),))
                res = cursor.fetchone()
                id_cliente = res['ID_CLIENTE'] if res else None

            sql_cab = "INSERT INTO ventas_online (ASESOR, ID_CLIENTE, CLIENTE, TIPO_COMPROBANTE, `N°_COMPR`, FECHA, REGION, DISTRITO, FORMA_DE_PAGO, SALIDA_DE_PEDIDO, LINEA, CANAL_VENTA, CLASIFICACION) VALUES (%(asesor)s, %(id_cliente)s, %(cliente)s, %(tipo_comprobante)s, %(comprobante)s, %(fecha)s, %(region)s, %(distrito)s, %(forma_pago)s, %(salida)s, %(linea)s, %(canal)s, %(clasificacion)s)"
            
            primer = detalles[0] if detalles else {}
            cursor.execute(sql_cab, {**cab, "id_cliente": id_cliente, "fecha": fecha_auto, "linea": primer.get("linea"), "canal": primer.get("canal"), "clasificacion": primer.get("clasificacion")})
            id_gen = cursor.lastrowid

            sql_det = "INSERT INTO detalle_ventas (LINEA, CANAL_VENTA, `N°_COMPR`, CODIGO_PRODUCTO, PRODUCTO, CANTIDAD, UNIDAD_MEDIDA, PRECIO_VENTA, DELIVERY, TOTAL, ID_VENTA, CLASIFICACION, FECHA) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            for it in detalles:
                cursor.execute(sql_det, (it.get("linea"), it.get("canal"), cab.get("comprobante"), it.get("codigo"), it.get("producto"), it.get("cantidad"), it.get("unidad"), it.get("precio"), it.get("delivery"), it.get("total"), id_gen, it.get("clasificacion"), fecha_auto))
        conn.commit()
        return (json.dumps({"success": "Venta registrada", "id": id_gen}), 200, headers)
    finally:
        conn.close()

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