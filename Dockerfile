# Usa una imagen de Python 3.10
FROM python:3.10.13-slim

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia el archivo requirements.txt al contenedor en /app
COPY requirements.txt .

# Copia el resto de la aplicación al contenedor en /app
COPY . .

# Instala el paquete Brotli
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto 5000
EXPOSE 5000

# Especifica el comando por defecto a ejecutar cuando se inicia el contenedor
CMD ["python", "Backend/app_back_v6_loc.py", "--host=0.0.0.0"]