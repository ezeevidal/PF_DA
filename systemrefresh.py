import pyodbc
import pandas as pd

# Lista de archivos CSV y sus tablas correspondientes
archivos_y_tablas = [
    {"archivo": "big_inventory_modified.csv", "tabla": "big_inventory"},
    {"archivo": "end_inventory_modified.csv", "tabla": "end_inventory"},
    {"archivo": "invoice_purchases_modified.csv", "tabla": "invoice_purchases"},
    {"archivo": "purchase_prices_2017_modified.csv", "tabla": "purchase_prices_2017"},
    {"archivo": "purchases_final_modified.csv", "tabla": "purchases_final"},
    {"archivo": "sales_final_modified.csv", "tabla": "sales_final"}
]

# Configuración del servidor y base de datos
server_config = {
    "server": r'(localdb)\EzeServer',  # Nombre del servidor
    "database": 'PF',  # Nombre de la base de datos
}

# Función para cargar datos a una tabla específica
def cargar_csv_a_tabla(server_config, archivo, tabla):
    try:
        # Leer el archivo CSV
        data = pd.read_csv(archivo)
        
        # Conectar a la base de datos usando autenticación de Windows
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_config['server']};"
            f"DATABASE={server_config['database']};"
            f"Trusted_Connection=yes;"
        )
        cursor = conn.cursor()

        # Insertar datos en la tabla
        # Obtener el nombre de las columnas de la tabla
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabla}'")
        columns = [column[0] for column in cursor.fetchall()]

        for i, row in data.iterrows():
            # Asegurarse de que el número de columnas en el archivo CSV coincida con el de la tabla
            if len(row) == len(columns):
                placeholders = ", ".join(["?" for _ in row])
                query = f"INSERT INTO {tabla} ({', '.join(columns)}) VALUES ({placeholders})"
                cursor.execute(query, tuple(row))
            else:
                print(f"Advertencia: El número de columnas en {archivo} no coincide con las de la tabla {tabla}.")
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Datos del archivo '{archivo}' cargados correctamente en la tabla '{tabla}' de la base '{server_config['database']}'.")

    except Exception as e:
        print(f"Error al cargar '{archivo}' en '{tabla}' en '{server_config['database']}': {e}")

# Ejecutar la carga para cada archivo
for archivo_tabla in archivos_y_tablas:
    cargar_csv_a_tabla(server_config, archivo_tabla["archivo"], archivo_tabla["tabla"])

print("Carga de datos completada.")