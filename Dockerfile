# Usa una imagen de Python 3.10
FROM python:3.10.13-alpine

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia el archivo requirements.txt al contenedor en /app
COPY requirements.txt .

# Instala el paquete Brotli directamente
RUN pip install --no-cache-dir -r requirements.txt

# Instala las dependencias especificadas en requirements.txt
RUN pip install -r requirements.txt

# Copia el resto de la aplicación al contenedor en /app
COPY . .

# Especifica el comando por defecto a ejecutar cuando se inicia el contenedor
CMD ["python", "app_back_v6_loc.py"]

