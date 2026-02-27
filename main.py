import functions_framework
import logging
import requests
import pymysql
import pymysql.cursors
import json
import os
from datetime import datetime
import pandas as pd
import io

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
    
    f_inicio = request.args.get("inicio") or "2024-01-01"
    f_fin = request.args.get("fin") or datetime.now().strftime('%Y-%m-%d')
    tipo = request.args.get("tipo") 

    def n(val):
        return None if val in ["null", "", "undefined", "None", None] else val

    try:
        with conn.cursor() as cursor:
            def fetch_all_safe(c):
                return c.fetchall() if c.description else []

            # --- REPORTE GENERAL 1: GESTIÓN Y VENTAS ---
            if tipo == "full_reporte_1":
                # 1. Captura adaptativa: busca 'producto' (front) o 'p_prod' (variable interna)
                p_prod = request.args.get("producto") or request.args.get("p_prod")
                p_mes = request.args.get("mes")
                p_canal = request.args.get("canal")
                p_clasi = request.args.get("clasificacion") or request.args.get("clasi")
                p_linea = request.args.get("linea")

                # 2. Ejecución del SP
                cursor.callproc('sp_Dashboard_General1', (
                    n(p_prod), n(p_mes), n(p_canal), n(p_clasi), n(p_linea), f_inicio, f_fin
                ))

                # 3. KPIs
                resumen = fetch_all_safe(cursor)
                kpi_data = {"total_generado": 0, "cantidad_ventas": 0}
                if resumen and resumen[0].get('total_generado') is not None:
                    kpi_data = resumen[0]

                # 4. Otros datasets (Ventas mes, Canales, etc.)
                ventas_mes = fetch_all_safe(cursor) if cursor.nextset() else []
                
                # --- CORRECCIÓN CLAVE PARA PRODUCTOS TOP ---
                prod_raw = fetch_all_safe(cursor) if cursor.nextset() else []
                prod_top = []
                for p in prod_raw:
                    prod_top.append({
                        "PRODUCTO": p.get("PRODUCTO") or p.get("producto"),
                        # Sumamos unidades_test (del nuevo SQL) o UNIDADES (del antiguo)
                        "UNIDADES": float(p.get("unidades_test") or p.get("UNIDADES") or 0),
                        "DOCENAS": p.get("DOCENAS") or 0,
                        "PARES": p.get("PARES") or 0
                    })

                canales = fetch_all_safe(cursor) if cursor.nextset() else []
                clasificaciones = fetch_all_safe(cursor) if cursor.nextset() else []
                lineas = fetch_all_safe(cursor) if cursor.nextset() else []

                result = {
                    "kpis": kpi_data,
                    "ventas_por_mes": ventas_mes,
                    "productos_top": prod_top, # Ahora lleva los datos mapeados correctamente
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

                ranking = fetch_all_safe(cursor)
                
                # Mejora: Redondeo de montos en el ranking para evitar números largos
                for row in ranking:
                    if 'monto_total' in row:
                        row['monto_total'] = int(round(float(row['monto_total'] or 0)))

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

                # --- REPORTE ESPECÍFICO: ZEUS ELECTRIC ---
            elif tipo == "zeus_electric_report":
                p_prod = request.args.get("producto")
                p_mes = request.args.get("mes")
                p_canal = request.args.get("canal")
                p_clasi = request.args.get("clasificacion")
                p_linea = request.args.get("linea")

                # Llamamos al procedimiento que acabas de crear
                cursor.callproc('sp_Dashboard_ZeusElectric', (
                    n(p_prod), n(p_mes), n(p_canal), n(p_clasi), n(p_linea), f_inicio, f_fin
                ))

                # 1. KPIs (Total y Cantidad)
                resumen = fetch_all_safe(cursor)
                kpi_data = {"total_generado": 0, "cantidad_ventas": 0}
                if resumen and resumen[0].get('total_generado') is not None:
                    kpi_data = resumen[0]

                # 2. Clientes (Ranking central)
                clientes = []
                if cursor.nextset(): clientes = fetch_all_safe(cursor)

                # 3. Productos Vendidos (Ranking unidades)
                productos = []
                if cursor.nextset(): productos = fetch_all_safe(cursor)

                # 4. Canal de Ventas
                canales = []
                if cursor.nextset(): canales = fetch_all_safe(cursor)

                # 5. Ventas por Región
                regiones = []
                if cursor.nextset(): regiones = fetch_all_safe(cursor)

                # 6. Tipos de Pago
                pagos = []
                if cursor.nextset(): pagos = fetch_all_safe(cursor)

                # 7. Ventas por Mes (Tendencia inferior)
                ventas_mes = []
                if cursor.nextset(): ventas_mes = fetch_all_safe(cursor)

                result = {
                    "kpis": kpi_data,
                    "clientes": clientes,
                    "productos_vendidos": productos,
                    "canal_ventas": canales,
                    "ventas_region": regiones,
                    "tipos_pago": pagos,
                    "ventas_por_mes": ventas_mes
                }

                # --- REPORTE ESPECÍFICO: ZEUS SAFETY ---
            elif tipo == "zeus_safety_report":
                p_prod = request.args.get("producto")
                p_mes = request.args.get("mes")
                p_canal = request.args.get("canal")
                p_clasi = request.args.get("clasificacion")
                p_linea = request.args.get("linea")

                # Llamada al nuevo Stored Procedure corregido
                cursor.callproc('sp_Dashboard_ZeusSafety', (
                    n(p_prod), n(p_mes), n(p_canal), n(p_clasi), n(p_linea), f_inicio, f_fin
                ))

                # 1. KPIs (Total y Cantidad)
                resumen = fetch_all_safe(cursor)
                kpi_data = {"total_generado": 0, "cantidad_ventas": 0}
                if resumen and resumen[0].get('total_generado') is not None:
                    kpi_data = resumen[0]

                # 2. Clientes | 3. Productos | 4. Canal | 5. Región | 6. Pago | 7. Mes
                clientes = fetch_all_safe(cursor) if cursor.nextset() else []
                productos = fetch_all_safe(cursor) if cursor.nextset() else []
                canales = fetch_all_safe(cursor) if cursor.nextset() else []
                regiones = fetch_all_safe(cursor) if cursor.nextset() else []
                pagos = fetch_all_safe(cursor) if cursor.nextset() else []
                ventas_mes = fetch_all_safe(cursor) if cursor.nextset() else []

                result = {
                    "kpis": kpi_data,
                    "clientes": clientes,
                    "productos_vendidos": productos,
                    "canal_ventas": canales,
                    "ventas_region": regiones,
                    "tipos_pago": pagos,
                    "ventas_por_mes": ventas_mes
                } 
                
            else:
                return (json.dumps({"error": "Tipo no válido"}), 400, headers)

        return (json.dumps(result, default=str), 200, headers)
    
    except Exception as e:
        logging.error(f"Error en dashboard: {str(e)}")
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# --- LÓGICA DE VENTAS Y CLIENTES ---

def gestionar_venta_completa(data, headers):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cab = data['cabecera']
            sql_v = """INSERT INTO ventas_online (ASESOR, ID_CLIENTE, CLIENTE, TIPO_COMPROBANTE, 
                       N°_COMPR, FECHA, REGION, DISTRITO, FORMA_DE_PAGO, SALIDA_DE_PEDIDO, 
                       LINEA, CANAL_VENTA, CLASIFICACION) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql_v, (cab['asesor'], cab['id_cliente'], cab['cliente'], cab['tipo_comprobante'],
                                   cab['n_compr'], cab['fecha'], cab['region'], cab['distrito'], 
                                   cab['forma_pago'], cab['salida'], cab['linea'], cab['canal'], cab['clasificacion']))
            id_v = cursor.lastrowid
            
            for d in data['detalle']:
                sql_d = """INSERT INTO detalle_ventas (LINEA, CANAL_VENTA, N°_COMPR, CODIGO_PRODUCTO, 
                           PRODUCTO, CANTIDAD, UNIDAD_MEDIDA, PRECIO_VENTA, DELIVERY, TOTAL, 
                           ID_VENTA, CLASIFICACION, FECHA) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql_d, (d['linea'], d['canal'], d['n_compr'], d['codigo'], d['producto'],
                                       d['cantidad'], d['unidad'], d['precio'], d['delivery'], d['total'],
                                       id_v, d['clasificacion'], d['fecha']))
            conn.commit()
            return (json.dumps({"status": "ok", "id": id_v}), 200, headers)
    except Exception as e:
        conn.rollback()
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

def insertar_cliente(data, headers):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """INSERT INTO clientes_ventas (FECHA, CLIENTE, TELEFONO, RUC, DNI, REGION, 
                     DISTRITO, TIPO_CLIENTE, CANAL_ORIGEN) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (data['fecha'], data['cliente'], data['telefono'], data['ruc'], 
                                 data['dni'], data['region'], data['distrito'], data['tipo'], data['canal']))
            conn.commit()
            return (json.dumps({"status": "ok"}), 200, headers)
    except Exception as e:
        conn.rollback()
        return (json.dumps({"error": str(e)}), 500, headers)
    finally:
        conn.close()

# para cargar datos del excel a la tabla clientes ventas online
def cargar_excel_clientes(request, headers):
    if 'file' not in request.files:
        return (json.dumps({"error": "No se encontró el archivo"}), 400, headers)
    
    file = request.files['file']
    
    try:
        # 1. Leemos el Excel
        df = pd.read_excel(io.BytesIO(file.read()), sheet_name='Table1')

        # 2. Limpieza de nulos (Fundamenta para evitar el error 'nan can not be used with MySQL')
        import numpy as np
        # Convertimos a objeto y reemplazamos NaNs por None (NULL en MySQL)
        df = df.astype(object).replace({np.nan: None})

        # 3. Seleccionamos las columnas INCLUYENDO ID_CLIENTE
        columnas_requeridas = [
            'ID_CLIENTE', 'FECHA', 'CLIENTE', 'TELEFONO', 'RUC', 'DNI', 
            'REGION', 'DISTRITO', 'TIPO_CLIENTE', 'CANAL_ORIGEN'
        ]
        
        # Validación de seguridad: si falta alguna columna, la crea vacía para no romper el script
        for col in columnas_requeridas:
            if col not in df.columns:
                df[col] = None

        df_final = df[columnas_requeridas]

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # 4. Ajuste del SQL: Agregamos ID_CLIENTE al inicio
                sql = """INSERT INTO clientes_ventas (ID_CLIENTE, FECHA, CLIENTE, TELEFONO, RUC, DNI, REGION, 
                         DISTRITO, TIPO_CLIENTE, CANAL_ORIGEN) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                
                # Convertimos filas a tuplas
                valores = [tuple(x) for x in df_final.values]
                
                # Ejecución masiva
                cursor.executemany(sql, valores)
                
            conn.commit()
            return (json.dumps({"status": "ok", "mensaje": f"Cargados {len(valores)} clientes exitosamente"}), 200, headers)
            
        except Exception as e:
            if conn: conn.rollback()
            # Manejo específico por si el ID ya existe (Duplicate Entry)
            error_msg = str(e)
            if "Duplicate entry" in error_msg:
                error_msg = f"Error: Uno o más IDs ya existen en la base de datos. Detalles: {error_msg}"
            return (json.dumps({"error": f"Error DB: {error_msg}"}), 500, headers)
        finally:
            if conn: conn.close()

    except Exception as e:
        return (json.dumps({"error": f"Error Excel: {str(e)}"}), 500, headers)


# --- CARGA MASIVA DESDE EXCEL: VENTAS_ONLINE ---
def cargar_excel_ventas_online(request, headers):
    if 'file' not in request.files:
        return (json.dumps({"error": "No se encontró el archivo"}), 400, headers)

    file = request.files['file']

    try:
        # 1. Leemos el Excel (mismo criterio que clientes_ventas)
        df = pd.read_excel(io.BytesIO(file.read()), sheet_name='Table1')

        # 2. Limpieza de nulos
        import numpy as np
        df = df.astype(object).replace({np.nan: None})

        # 3. Columnas esperadas según tabla ventas_online
        columnas_requeridas = [
            'ASESOR',          # puede no venir en el Excel
            'ID_CLIENTE',
            'CLIENTE',
            'TIPO_COMPROBANTE',
            'N°_COMPR',
            'FECHA',
            'REGION',
            'DISTRITO',
            'FORMA_DE_PAGO',
            'SALIDA_DE_PEDIDO',
            'LINEA',
            'CANAL_VENTA',
            'CLASIFICACION'
        ]

        # Si no viene la columna ASESOR en el Excel, la creamos con un valor por defecto
        if 'ASESOR' not in df.columns:
            df['ASESOR'] = 'ONLINE'

        # Validación: si falta alguna columna (excepto ASESOR, ya tratada), la creamos vacía
        for col in columnas_requeridas:
            if col not in df.columns:
                df[col] = None

        df_final = df[columnas_requeridas]

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO ventas_online (
                        ASESOR, ID_CLIENTE, CLIENTE, TIPO_COMPROBANTE,
                        N°_COMPR, FECHA, REGION, DISTRITO,
                        FORMA_DE_PAGO, SALIDA_DE_PEDIDO, LINEA,
                        CANAL_VENTA, CLASIFICACION
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                valores = [tuple(x) for x in df_final.values]
                cursor.executemany(sql, valores)

            conn.commit()
            return (
                json.dumps({
                    "status": "ok",
                    "mensaje": f"Cargadas {len(valores)} filas en ventas_online"
                }),
                200,
                headers
            )
        except Exception as e:
            if conn:
                conn.rollback()
            return (json.dumps({"error": f"Error DB: {str(e)}"}), 500, headers)
        finally:
            if conn:
                conn.close()

    except Exception as e:
        return (json.dumps({"error": f"Error Excel: {str(e)}"}), 500, headers)


# --- CARGA MASIVA DESDE EXCEL: DETALLE_VENTAS ---
def cargar_excel_detalle_ventas(request, headers):
    if 'file' not in request.files:
        return (json.dumps({"error": "No se encontró el archivo"}), 400, headers)

    file = request.files['file']

    try:
        df = pd.read_excel(io.BytesIO(file.read()), sheet_name='Table1')

        import numpy as np
        df = df.astype(object).replace({np.nan: None})

        # Algunas plantillas pueden traer "PRECIO_VENT" en lugar de "PRECIO_VENTA"
        if 'PRECIO_VENTA' not in df.columns and 'PRECIO_VENT' in df.columns:
            df['PRECIO_VENTA'] = df['PRECIO_VENT']

        # Validamos que N°_COMPR exista (es crítico para buscar ID_VENTA)
        if 'N°_COMPR' not in df.columns:
            return (json.dumps({"error": "El Excel debe contener la columna 'N°_COMPR'"}), 400, headers)

        columnas_requeridas = [
            'LINEA',
            'CANAL_VENTA',
            'N°_COMPR',
            'CODIGO_PRODUCTO',
            'PRODUCTO',
            'CANTIDAD',
            'UNIDAD_MEDIDA',
            'PRECIO_VENTA',
            'DELIVERY',
            'TOTAL',
            'CLASIFICACION',
            'FECHA'
        ]

        # Creamos columnas faltantes como None para no romper el insert
        for col in columnas_requeridas:
            if col not in df.columns:
                df[col] = None

        df_final = df[columnas_requeridas]

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # Primero, creamos un diccionario que mapea N°_COMPR -> ID_VENTA
                # Normalizamos los valores (trim) para evitar problemas con espacios
                # Si hay múltiples registros con el mismo N°_COMPR, tomamos el ID_VENTA más reciente (mayor)
                cursor.execute("SELECT ID_VENTA, N°_COMPR FROM ventas_online ORDER BY ID_VENTA DESC")
                resultados = cursor.fetchall()
                
                # Creamos dos mapas: uno exacto y otro normalizado (sin espacios)
                # Si hay duplicados, el último (más reciente) sobrescribe el anterior
                mapa_comprobantes_exacto = {}
                mapa_comprobantes_normalizado = {}
                
                for row in resultados:
                    n_compr_original = row['N°_COMPR']
                    if n_compr_original:
                        # Mapa exacto (si hay duplicados, se queda con el más reciente por el ORDER BY DESC)
                        mapa_comprobantes_exacto[n_compr_original] = row['ID_VENTA']
                        # Mapa normalizado (trim y sin espacios extra)
                        n_compr_normalizado = str(n_compr_original).strip() if n_compr_original else None
                        if n_compr_normalizado:
                            mapa_comprobantes_normalizado[n_compr_normalizado] = row['ID_VENTA']

                sql = """
                    INSERT INTO detalle_ventas (
                        LINEA, CANAL_VENTA, N°_COMPR, CODIGO_PRODUCTO,
                        PRODUCTO, CANTIDAD, UNIDAD_MEDIDA, PRECIO_VENTA,
                        DELIVERY, TOTAL, ID_VENTA, CLASIFICACION, FECHA
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Procesamos fila por fila para buscar el ID_VENTA correcto
                filas_insertadas = 0
                filas_con_error = []
                
                for idx, row in df_final.iterrows():
                    n_compr = row['N°_COMPR']
                    
                    # Intentamos primero búsqueda exacta
                    id_venta = mapa_comprobantes_exacto.get(n_compr)
                    
                    # Si no encontramos, intentamos con valor normalizado (trim)
                    if id_venta is None and n_compr:
                        n_compr_normalizado = str(n_compr).strip()
                        id_venta = mapa_comprobantes_normalizado.get(n_compr_normalizado)
                    
                    if id_venta is None:
                        # Intentamos buscar valores similares en la BD para ayudar al diagnóstico
                        valores_similares = []
                        if n_compr:
                            n_compr_buscar = str(n_compr).strip()
                            for n_compr_bd, id_v in mapa_comprobantes_normalizado.items():
                                if n_compr_buscar in n_compr_bd or n_compr_bd in n_compr_buscar:
                                    valores_similares.append(f"'{n_compr_bd}' (ID_VENTA: {id_v})")
                        
                        error_msg = f"Fila {idx + 2}: N°_COMPR '{n_compr}' no encontrado en ventas_online"
                        if valores_similares:
                            error_msg += f". Valores similares encontrados: {', '.join(valores_similares[:3])}"
                        filas_con_error.append(error_msg)
                        continue
                    
                    valores = (
                        row['LINEA'],
                        row['CANAL_VENTA'],
                        n_compr,
                        row['CODIGO_PRODUCTO'],
                        row['PRODUCTO'],
                        row['CANTIDAD'],
                        row['UNIDAD_MEDIDA'],
                        row['PRECIO_VENTA'],
                        row['DELIVERY'],
                        row['TOTAL'],
                        id_venta,  # Usamos el ID_VENTA real de la BD
                        row['CLASIFICACION'],
                        row['FECHA']
                    )
                    
                    cursor.execute(sql, valores)
                    filas_insertadas += 1

            conn.commit()
            
            mensaje = f"Cargadas {filas_insertadas} filas en detalle_ventas"
            if filas_con_error:
                mensaje += f". Advertencias: {len(filas_con_error)} filas no se insertaron (N°_COMPR no encontrado)"
            
            return (
                json.dumps({
                    "status": "ok",
                    "mensaje": mensaje,
                    "filas_insertadas": filas_insertadas,
                    "errores": filas_con_error if filas_con_error else None
                }),
                200,
                headers
            )
        except Exception as e:
            if conn:
                conn.rollback()
            return (json.dumps({"error": f"Error DB: {str(e)}"}), 500, headers)
        finally:
            if conn:
                conn.close()

    except Exception as e:
        return (json.dumps({"error": f"Error Excel: {str(e)}"}), 500, headers)

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
        # CASO 1: Carga masiva por Excel (multipart/form-data)
        if 'file' in request.files:
            modo = request.args.get("modo")
            # clientes_ventas (modo por defecto o explícito)
            if modo in [None, "", "clientes_excel"]:
                return cargar_excel_clientes(request, headers)
            # nuevas funciones temporales para subir ventas y detalle desde Excel
            elif modo == "ventas_excel":
                return cargar_excel_ventas_online(request, headers)
            elif modo == "detalle_excel":
                return cargar_excel_detalle_ventas(request, headers)
            else:
                return (json.dumps({"error": "Modo de carga Excel no reconocido"}), 400, headers)
        
        # CASO 2: Inserción manual o Venta completa (JSON)
        data = request.get_json(silent=True)
        if data:
            if "cabecera" in data:
                return gestionar_venta_completa(data, headers)
            else:
                return insertar_cliente(data, headers)
        
        return (json.dumps({"error": "No se recibieron datos válidos"}), 400, headers)


    return (json.dumps({"error": "Método no permitido"}), 405, headers)