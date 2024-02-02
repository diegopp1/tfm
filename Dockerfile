# Usa una imagen base con Python
FROM python:3.10.13-slim

# Establece el directorio de trabajo en /app
WORKDIR /app

# Copia el archivo requirements.txt al contenedor
COPY requirements.txt .

# Instala las dependencias de la aplicación
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expone el puerto 5000 para que la aplicación Flask pueda ser accedida externamente
EXPOSE 5000

# Comando para ejecutar la aplicación cuando el contenedor se inicie
CMD ["python", "Backend/app_back_v6_loc.py"]
