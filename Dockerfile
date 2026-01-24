# Usar la imagen oficial de Python 3.11 ligera
FROM python:3.13-slim

# Establecer el directorio de trabajo
WORKDIR /workspace

# Copiar el archivo de dependencias (Asegúrate que el archivo se llame requeriments.txt)
COPY requeriments.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requeriments.txt

# Copiar el código del main.py
COPY main.py .

# Exponer el puerto 8080 estándar de Cloud Run
EXPOSE 8080
ENV PORT=8080

# COMANDO CORREGIDO: El target debe ser igual al nombre de la función en main.py
CMD exec functions-framework --target=reporte_ventas_online --port=$PORT --host=0.0.0.0