import pandas as pd

# URL del conjunto de datos del enlace proporcionado
url = 'https://data.apps.fao.org/catalog/dataset/59c98163-83a1-4bf6-a7a1-0ef2dc1c364f/resource/9501e2b0-7767-4edc-bff5-c02e4c987db0/download/m49.csv'
dataset = pd.read_csv(url)

# Lista de países en tu proyecto
selected_countries = ['Argentina', 'Canada', 'Chile', 'Germany', 'France', 'Italy', 'United States of America', 'Spain', 'Portugal', 'China', 'Japan', 'Brazil', 'Australia', 'South Africa']

# Filtrar el conjunto de datos para incluir solo los países seleccionados
filtered_countries_data = dataset[dataset['country_name_en'].isin(selected_countries)]

# Crear un diccionario que mapee los nombres de países a los códigos M49
country_to_m49_mapping = dict(zip(filtered_countries_data['country_name_en'], filtered_countries_data['m49']))

# Imprimir el diccionario
print("Country to M49 Mapping:")
print(country_to_m49_mapping)
