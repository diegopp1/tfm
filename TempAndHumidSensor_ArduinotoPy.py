import serial
import time

puerto_serial = 'COM3'
baudios = 9600

try:
    conexion_serial = serial.Serial(puerto_serial, baudios, timeout=1)
    print(f"Conexión establecida con {puerto_serial}")

    comando = 'H'
    print(f"Enviando comando al Arduino: {comando}")
    conexion_serial.write(comando.encode()) # Envía el comando al Arduino

    time.sleep(2)  # Aumenta el tiempo de espera

    respuesta = conexion_serial.readline().decode().strip()
    print(f"Respuesta del Arduino: {respuesta}")

except serial.SerialException as e:
    print(f"Error al abrir el puerto serial: {e}")

finally:
    if 'conexion_serial' in locals() and conexion_serial.is_open:
        conexion_serial.close()
        print("Conexión cerrada")
