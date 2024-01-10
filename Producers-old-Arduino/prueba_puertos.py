import serial.tools.list_ports

def detectar_puertos_arduino():
    # Obtener una lista de puertos seriales disponibles
    puertos_disponibles = serial.tools.list_ports.comports()

    # Filtrar los puertos que podrían ser Arduino
    puertos_arduino = [
        puerto.device
        for puerto in puertos_disponibles
        if 'Arduino' in puerto.description or 'Arduino' in puerto.manufacturer
    ]

    return puertos_arduino

if __name__ == "__main__":
    puertos_arduino = detectar_puertos_arduino()

    if puertos_arduino:
        print("Arduino(s) detectado(s) en los siguientes puertos:")
        for puerto in puertos_arduino:
            print(f"- {puerto}")
    else:
        print("No se detectaron Arduinos conectados.")
