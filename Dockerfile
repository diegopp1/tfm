# Usa una imagen de Python 3.10
FROM python:3.10.13-slim

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia el archivo requirements.txt al contenedor en /app
COPY requirements.txt .

# Instala el paquete Brotli directamente
RUN apt-get update && apt-get install -y libbrotli1 libbrotli-dev

# Instala las dependencias especificadas en requirements.txt
RUN pip install -r requirements.txt

# Copia el resto de la aplicación al contenedor en /app
COPY . .

# Especifica el comando por defecto a ejecutar cuando se inicia el contenedor
CMD ["python", "tu_script_principal.py"]

