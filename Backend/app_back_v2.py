from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import json

app = FastAPI()

# Lista para almacenar los clientes WebSocket conectados
websocket_clients = set()

# Montar el directorio "templates" como archivos estáticos para que FastAPI pueda servir el archivo HTML
app.mount("/templates", StaticFiles(directory="templates", html=True), name="templates")

# Ruta para servir el archivo HTML
@app.get("/", response_class=HTMLResponse)
async def get():
    return FileResponse("templates/index2.html")

# Ruta WebSocket para la transmisión de datos
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    # Agregar el cliente WebSocket a la lista
    websocket_clients.add(websocket)

    try:
        while True:
            # Esperar mensajes
            data = await websocket.receive_text()
            # Procesar datos (puedes hacer algo más avanzado aquí)
            processed_data = {"received_data": data}

            # Enviar datos a todos los clientes WebSocket conectados
            for client in websocket_clients:
                await client.send_text(json.dumps(processed_data))
    except Exception as e:
        print(f"Error en WebSocket: {e}")
    finally:
        # Eliminar el cliente WebSocket al salir
        websocket_clients.remove(websocket)

# Puedes ejecutar la aplicación con Uvicorn:
# uvicorn nombre_de_tu_script:app --host 0.0.0.0 --port 8000 --reload

