import pandas as pd
import pyodbc

# Configuración de conexión
server = 'DESKTOP-5CFE65F\\SQLEXPRESS'  # Nombre del servidor
database = 'Alcoholic_BeverageDB'       # Nombre de la base de datos
table_name = 'Productos'                # Nombre de la tabla
csv_file = r'C:\Users\ASUS\Documents\Productos_new.csv'     # Archivo CSV de entrada

# Columnas únicas para detectar duplicados
unique_columns = ['ProductID', 'Description', 'Size']

# Cadena de conexión
connection_string = f"Driver={{SQL Server}};Server={server};Database={database};Trusted_Connection=yes;"

try:
    # Conectar a SQL Server
    conn = pyodbc.connect(connection_string)
    print("Conexión exitosa a SQL Server.")

    # Leer el archivo CSV
    print(f"Leyendo archivo CSV: {csv_file}")
    df = pd.read_csv(csv_file)

    # Obtener claves existentes de la tabla Productos
    print("Obteniendo claves existentes de la tabla Productos...")
    query = f"SELECT {', '.join(unique_columns)} FROM {table_name}"
    existing_keys = pd.read_sql(query, conn)

    # Crear una clave única para comparación
    existing_keys['unique_key'] = existing_keys[unique_columns].astype(str).agg('_'.join, axis=1)
    df['unique_key'] = df[unique_columns].astype(str).agg('_'.join, axis=1)

    # Filtrar registros nuevos que no están en la tabla Productos
    print("Filtrando registros no duplicados...")
    new_data = df[~df['unique_key'].isin(existing_keys['unique_key'])].drop(columns=['unique_key'])

    if not new_data.empty:
        print(f"Insertando {len(new_data)} registros nuevos en la tabla {table_name}...")
        for index, row in new_data.iterrows():
            placeholders = ', '.join('?' for _ in row)
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            conn.cursor().execute(sql, tuple(row))
        conn.commit()
        print("Datos nuevos cargados exitosamente.")
    else:
        print("No hay datos nuevos para insertar.")

except Exception as e:
    print(f"Error: {e}")
finally:
    # Cerrar la conexión
    if 'conn' in locals():
        conn.close()
        print("Conexión cerrada.")