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
                # Soporta selección múltiple: valores separados por comas
                p_prod_raw = request.args.get("producto") or request.args.get("p_prod")
                p_mes_raw = request.args.get("mes")
                p_canal_raw = request.args.get("canal")
                p_clasi_raw = request.args.get("clasificacion") or request.args.get("clasi")
                p_linea_raw = request.args.get("linea")

                # Parsear valores múltiples (separados por comas)
                def parse_multiple(val):
                    if not val or val in ["null", "", "undefined", "None"]:
                        return []
                    # Si contiene comas, es múltiple
                    if ',' in str(val):
                        return [v.strip() for v in str(val).split(',') if v.strip()]
                    return [str(val).strip()]

                p_prod_list = parse_multiple(p_prod_raw)
                p_mes_list = parse_multiple(p_mes_raw)
                p_canal_list = parse_multiple(p_canal_raw)
                p_clasi_list = parse_multiple(p_clasi_raw)
                p_linea_list = parse_multiple(p_linea_raw)

                # Para compatibilidad con SP: usar primer valor o None
                p_prod = p_prod_list[0] if p_prod_list else None
                p_mes = p_mes_list[0] if p_mes_list else None
                p_canal = p_canal_list[0] if p_canal_list else None
                p_clasi = p_clasi_list[0] if p_clasi_list else None
                p_linea = p_linea_list[0] if p_linea_list else None

                # 2. Ejecución del SP (usa primer valor para compatibilidad)
                # Si hay múltiples productos, necesitamos recalcular KPIs y ventas por mes con filtros múltiples
                usar_sp_directo = len(p_prod_list) <= 1 and len(p_mes_list) <= 1 and len(p_canal_list) <= 1 and len(p_clasi_list) <= 1 and len(p_linea_list) <= 1
                
                if usar_sp_directo:
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
                else:
                    # Múltiples filtros: ejecutar consultas SQL directas para KPIs y ventas por mes
                    # Construir WHERE con filtros múltiples
                    where_kpi = ["vo.FECHA BETWEEN %s AND %s"]
                    params_kpi = [f_inicio, f_fin]
                    
                    if p_prod_list:
                        if len(p_prod_list) == 1:
                            where_kpi.append("EXISTS (SELECT 1 FROM detalle_ventas dv WHERE dv.ID_VENTA = vo.ID_VENTA AND dv.PRODUCTO LIKE %s)")
                            params_kpi.append(f"%{p_prod_list[0]}%")
                        else:
                            placeholders = ','.join(['%s'] * len(p_prod_list))
                            where_kpi.append(f"EXISTS (SELECT 1 FROM detalle_ventas dv WHERE dv.ID_VENTA = vo.ID_VENTA AND dv.PRODUCTO IN ({placeholders}))")
                            params_kpi.extend([f"%{p}%" for p in p_prod_list])
                    
                    if p_mes_list:
                        if len(p_mes_list) == 1:
                            where_kpi.append("MONTH(vo.FECHA) = %s AND YEAR(vo.FECHA) = %s")
                            mes_parts = p_mes_list[0].split('-')
                            if len(mes_parts) == 2:
                                params_kpi.extend([mes_parts[1], mes_parts[0]])
                        else:
                            mes_conditions = []
                            for mes_val in p_mes_list:
                                mes_parts = mes_val.split('-')
                                if len(mes_parts) == 2:
                                    mes_conditions.append("(MONTH(vo.FECHA) = %s AND YEAR(vo.FECHA) = %s)")
                                    params_kpi.extend([mes_parts[1], mes_parts[0]])
                            if mes_conditions:
                                where_kpi.append(f"({' OR '.join(mes_conditions)})")
                    
                    if p_canal_list:
                        if len(p_canal_list) == 1:
                            where_kpi.append("vo.CANAL_VENTA = %s")
                            params_kpi.append(p_canal_list[0])
                        else:
                            placeholders = ','.join(['%s'] * len(p_canal_list))
                            where_kpi.append(f"vo.CANAL_VENTA IN ({placeholders})")
                            params_kpi.extend(p_canal_list)
                    
                    if p_clasi_list:
                        if len(p_clasi_list) == 1:
                            where_kpi.append("vo.CLASIFICACION = %s")
                            params_kpi.append(p_clasi_list[0])
                        else:
                            placeholders = ','.join(['%s'] * len(p_clasi_list))
                            where_kpi.append(f"vo.CLASIFICACION IN ({placeholders})")
                            params_kpi.extend(p_clasi_list)
                    
                    if p_linea_list:
                        if len(p_linea_list) == 1:
                            where_kpi.append("vo.LINEA = %s")
                            params_kpi.append(p_linea_list[0])
                        else:
                            placeholders = ','.join(['%s'] * len(p_linea_list))
                            where_kpi.append(f"vo.LINEA IN ({placeholders})")
                            params_kpi.extend(p_linea_list)
                    
                    # Calcular KPIs con filtros múltiples
                    sql_kpi = f"""SELECT 
                                    COALESCE(SUM(vo.TOTAL), 0) as total_generado,
                                    COUNT(DISTINCT vo.ID_VENTA) as cantidad_ventas
                                  FROM ventas_online vo
                                  WHERE {' AND '.join(where_kpi)}"""
                    cursor.execute(sql_kpi, params_kpi)
                    kpi_row = cursor.fetchone()
                    kpi_data = {
                        "total_generado": float(kpi_row.get('total_generado', 0)) if kpi_row else 0,
                        "cantidad_ventas": int(kpi_row.get('cantidad_ventas', 0)) if kpi_row else 0
                    }
                    
                    # Calcular ventas por mes con filtros múltiples
                    sql_ventas_mes = f"""SELECT 
                                           CONCAT(YEAR(vo.FECHA), '-', LPAD(MONTH(vo.FECHA), 2, '0')) as mes,
                                           YEAR(vo.FECHA) as anio,
                                           MONTH(vo.FECHA) as mes_num,
                                           COALESCE(SUM(vo.TOTAL), 0) as total
                                         FROM ventas_online vo
                                         WHERE {' AND '.join(where_kpi)}
                                         GROUP BY YEAR(vo.FECHA), MONTH(vo.FECHA)
                                         ORDER BY YEAR(vo.FECHA), MONTH(vo.FECHA)"""
                    cursor.execute(sql_ventas_mes, params_kpi)
                    ventas_mes_raw = cursor.fetchall()
                    ventas_mes = []
                    meses_nombres = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
                    for row in ventas_mes_raw:
                        mes_num = row.get('mes_num', 0)
                        mes_nombre = meses_nombres[mes_num - 1] if 1 <= mes_num <= 12 else f"Mes {mes_num}"
                        ventas_mes.append({
                            "mes": row.get('mes'),
                            "mesLabel": f"{mes_nombre} {row.get('anio')}",
                            "total": float(row.get('total', 0))
                        })
                    
                    # Ejecutar SP solo para obtener productos_top, canales, clasificaciones y lineas (sin filtros múltiples en estos)
                    cursor.callproc('sp_Dashboard_General1', (
                        n(p_prod), n(p_mes), n(p_canal), n(p_clasi), n(p_linea), f_inicio, f_fin
                    ))
                    # Saltar KPIs y ventas_mes del SP (ya los calculamos)
                    _ = fetch_all_safe(cursor)  # KPIs (ignorar)
                    _ = fetch_all_safe(cursor) if cursor.nextset() else []  # ventas_mes (ignorar)
                
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

                # Procesar líneas: agregar conteo de registros (COUNT) además del total
                # Construir WHERE con los mismos filtros que el SP (excepto linea)
                # Soporta selección múltiple con IN clauses
                where_conditions_lineas = ["vo.FECHA BETWEEN %s AND %s"]
                params_lineas = [f_inicio, f_fin]
                
                if p_prod_list:
                    # Si hay múltiples productos, usar IN con subconsulta
                    if len(p_prod_list) == 1:
                        where_conditions_lineas.append("EXISTS (SELECT 1 FROM detalle_ventas dv WHERE dv.ID_VENTA = vo.ID_VENTA AND dv.PRODUCTO LIKE %s)")
                        params_lineas.append(f"%{p_prod_list[0]}%")
                    else:
                        # Múltiples productos: usar IN
                        placeholders = ','.join(['%s'] * len(p_prod_list))
                        where_conditions_lineas.append(f"EXISTS (SELECT 1 FROM detalle_ventas dv WHERE dv.ID_VENTA = vo.ID_VENTA AND dv.PRODUCTO IN ({placeholders}))")
                        params_lineas.extend([f"%{p}%" for p in p_prod_list])
                
                if p_mes_list:
                    # Múltiples meses: construir condiciones OR
                    if len(p_mes_list) == 1:
                        where_conditions_lineas.append("MONTH(vo.FECHA) = %s AND YEAR(vo.FECHA) = %s")
                        mes_parts = p_mes_list[0].split('-')
                        if len(mes_parts) == 2:
                            params_lineas.extend([mes_parts[1], mes_parts[0]])
                    else:
                        # Múltiples meses
                        mes_conditions = []
                        for mes_val in p_mes_list:
                            mes_parts = mes_val.split('-')
                            if len(mes_parts) == 2:
                                mes_conditions.append("(MONTH(vo.FECHA) = %s AND YEAR(vo.FECHA) = %s)")
                                params_lineas.extend([mes_parts[1], mes_parts[0]])
                        if mes_conditions:
                            where_conditions_lineas.append(f"({' OR '.join(mes_conditions)})")
                
                if p_canal_list:
                    if len(p_canal_list) == 1:
                        where_conditions_lineas.append("vo.CANAL_VENTA = %s")
                        params_lineas.append(p_canal_list[0])
                    else:
                        placeholders = ','.join(['%s'] * len(p_canal_list))
                        where_conditions_lineas.append(f"vo.CANAL_VENTA IN ({placeholders})")
                        params_lineas.extend(p_canal_list)
                
                if p_clasi_list:
                    if len(p_clasi_list) == 1:
                        where_conditions_lineas.append("vo.CLASIFICACION = %s")
                        params_lineas.append(p_clasi_list[0])
                    else:
                        placeholders = ','.join(['%s'] * len(p_clasi_list))
                        where_conditions_lineas.append(f"vo.CLASIFICACION IN ({placeholders})")
                        params_lineas.extend(p_clasi_list)
                
                sql_count_lineas = f"""SELECT LINEA, COUNT(*) as total_registros 
                                      FROM ventas_online vo
                                      WHERE {' AND '.join(where_conditions_lineas)}
                                      GROUP BY LINEA"""
                cursor.execute(sql_count_lineas, params_lineas)
                counts_map_lineas = {}
                for row in cursor.fetchall():
                    linea_key = row.get('LINEA') or row.get('linea')
                    if linea_key:
                        counts_map_lineas[linea_key] = row.get('total_registros', 0)

                # Agregar total_registros a cada línea
                lineas_procesadas = []
                for linea in lineas:
                    linea_name = linea.get('LINEA') or linea.get('linea') or linea.get('nombre') or linea.get('NOMBRE') or linea.get('descripcion') or linea.get('DESCRIPCION')
                    # Siempre agregar total_registros, usar 0 si no está en el mapa (no hay registros para este filtro)
                    linea['total_registros'] = counts_map_lineas.get(linea_name, 0) if linea_name else 0
                    linea['TOTAL_REGISTROS'] = counts_map_lineas.get(linea_name, 0) if linea_name else 0
                    lineas_procesadas.append(linea)

                # Procesar clasificaciones: agregar conteo de registros (COUNT) además del total
                # Hacer consulta única para obtener todos los conteos agrupados
                # Construir WHERE con los mismos filtros que el SP (excepto clasificación)
                # Soporta selección múltiple con IN clauses
                where_conditions = ["vo.FECHA BETWEEN %s AND %s"]
                params = [f_inicio, f_fin]
                
                if p_prod_list:
                    if len(p_prod_list) == 1:
                        where_conditions.append("EXISTS (SELECT 1 FROM detalle_ventas dv WHERE dv.ID_VENTA = vo.ID_VENTA AND dv.PRODUCTO LIKE %s)")
                        params.append(f"%{p_prod_list[0]}%")
                    else:
                        placeholders = ','.join(['%s'] * len(p_prod_list))
                        where_conditions.append(f"EXISTS (SELECT 1 FROM detalle_ventas dv WHERE dv.ID_VENTA = vo.ID_VENTA AND dv.PRODUCTO IN ({placeholders}))")
                        params.extend([f"%{p}%" for p in p_prod_list])
                
                if p_mes_list:
                    if len(p_mes_list) == 1:
                        where_conditions.append("MONTH(vo.FECHA) = %s AND YEAR(vo.FECHA) = %s")
                        mes_parts = p_mes_list[0].split('-')
                        if len(mes_parts) == 2:
                            params.extend([mes_parts[1], mes_parts[0]])
                    else:
                        mes_conditions = []
                        for mes_val in p_mes_list:
                            mes_parts = mes_val.split('-')
                            if len(mes_parts) == 2:
                                mes_conditions.append("(MONTH(vo.FECHA) = %s AND YEAR(vo.FECHA) = %s)")
                                params.extend([mes_parts[1], mes_parts[0]])
                        if mes_conditions:
                            where_conditions.append(f"({' OR '.join(mes_conditions)})")
                
                if p_canal_list:
                    if len(p_canal_list) == 1:
                        where_conditions.append("vo.CANAL_VENTA = %s")
                        params.append(p_canal_list[0])
                    else:
                        placeholders = ','.join(['%s'] * len(p_canal_list))
                        where_conditions.append(f"vo.CANAL_VENTA IN ({placeholders})")
                        params.extend(p_canal_list)
                
                if p_linea_list:
                    if len(p_linea_list) == 1:
                        where_conditions.append("vo.LINEA = %s")
                        params.append(p_linea_list[0])
                    else:
                        placeholders = ','.join(['%s'] * len(p_linea_list))
                        where_conditions.append(f"vo.LINEA IN ({placeholders})")
                        params.extend(p_linea_list)
                
                sql_count = f"""SELECT CLASIFICACION, COUNT(*) as total_registros 
                               FROM ventas_online vo
                               WHERE {' AND '.join(where_conditions)}
                               GROUP BY CLASIFICACION"""
                cursor.execute(sql_count, params)
                counts_map = {}
                for row in cursor.fetchall():
                    clasi_key = row.get('CLASIFICACION') or row.get('clasificacion')
                    if clasi_key:
                        counts_map[clasi_key] = row.get('total_registros', 0)

                # Agregar total_registros a cada clasificación
                clasificaciones_procesadas = []
                for clasi in clasificaciones:
                    clasi_name = clasi.get('clasificacion_pedido') or clasi.get('CLASIFICACION_PEDIDO') or clasi.get('clasificacion') or clasi.get('CLASIFICACION') or clasi.get('nombre') or clasi.get('NOMBRE')
                    # Siempre agregar total_registros, usar 0 si no está en el mapa (no hay registros para este filtro)
                    clasi['total_registros'] = counts_map.get(clasi_name, 0) if clasi_name else 0
                    clasi['TOTAL_REGISTROS'] = counts_map.get(clasi_name, 0) if clasi_name else 0
                    clasificaciones_procesadas.append(clasi)

                result = {
                    "kpis": kpi_data,
                    "ventas_por_mes": ventas_mes,
                    "productos_top": prod_top, # Ahora lleva los datos mapeados correctamente
                    "canales_venta": canales,
                    "clasificacion_pedidos": clasificaciones_procesadas,
                    "lineas": lineas_procesadas
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

                # Procesar canales: agregar numero_de_pedidos (COUNT de pedidos) además del monto_total
                # Construir WHERE con los mismos filtros que el SP
                where_conditions_canales = ["v.FECHA BETWEEN %s AND %s"]
                params_canales = [f_inicio, f_fin]
                
                if p_prod:
                    where_conditions_canales.append("EXISTS (SELECT 1 FROM detalle_ventas dv2 WHERE dv2.ID_VENTA = v.ID_VENTA AND dv2.PRODUCTO LIKE %s)")
                    params_canales.append(f"%{p_prod}%")
                
                if p_mes:
                    where_conditions_canales.append("MONTH(v.FECHA) = %s AND YEAR(v.FECHA) = %s")
                    mes_parts = p_mes.split('-')
                    if len(mes_parts) == 2:
                        params_canales.extend([mes_parts[1], mes_parts[0]])
                
                if p_clasi:
                    where_conditions_canales.append("v.CLASIFICACION = %s")
                    params_canales.append(p_clasi)
                
                if p_linea:
                    where_conditions_canales.append("v.LINEA = %s")
                    params_canales.append(p_linea)
                
                # Nota: No filtramos por p_canal aquí porque queremos el COUNT de todos los canales
                # El filtro de canal se aplica en el stored procedure para otros datos
                
                # Calcular numero_de_pedidos (COUNT de pedidos) por canal
                sql_canales_count = f"""SELECT 
                                          v.CANAL_VENTA,
                                          COUNT(DISTINCT v.ID_VENTA) as numero_de_pedidos
                                        FROM ventas_online v
                                        WHERE {' AND '.join(where_conditions_canales)}
                                        GROUP BY v.CANAL_VENTA"""
                cursor.execute(sql_canales_count, params_canales)
                counts_map_canales = {}
                for row in cursor.fetchall():
                    canal_key = row.get('CANAL_VENTA') or row.get('canal_venta')
                    if canal_key:
                        counts_map_canales[canal_key] = row.get('numero_de_pedidos', 0)

                # Agregar numero_de_pedidos a cada canal
                canales_procesados = []
                for canal in canales:
                    canal_name = canal.get('canal_venta') or canal.get('CANAL_VENTA') or canal.get('canal') or canal.get('CANAL') or canal.get('nombre') or canal.get('NOMBRE')
                    if canal_name and canal_name in counts_map_canales:
                        canal['numero_de_pedidos'] = counts_map_canales[canal_name]
                        canal['NUMERO_DE_PEDIDOS'] = counts_map_canales[canal_name]
                    canales_procesados.append(canal)

                result = {
                    "kpis": kpi_data,
                    "clientes": clientes,
                    "productos_vendidos": productos,
                    "canal_ventas": canales_procesados,  # Usar los procesados con numero_de_pedidos
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
            return cargar_excel_clientes(request, headers)
        
        # CASO 2: Inserción manual o Venta completa (JSON)
        data = request.get_json(silent=True)
        if data:
            if "cabecera" in data:
                return gestionar_venta_completa(data, headers)
            else:
                return insertar_cliente(data, headers)
        
        return (json.dumps({"error": "No se recibieron datos válidos"}), 400, headers)


    return (json.dumps({"error": "Método no permitido"}), 405, headers)