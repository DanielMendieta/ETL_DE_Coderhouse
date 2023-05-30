#1-EXTRACCIÓN DE UNA API
import requests
from pprint import pprint
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

url = "https://random-data-api.com/api/commerce/random_commerce?size=100"

response = requests.get(url)
data = response.json()

tabla = pd.DataFrame(data)
tabla

#2- COMIENZO CON LA LIMPIEZA:
#DESCARTO COLUMNAS QUE NO ME INTERESAN.
del tabla ["uid"]
del tabla ["price_string"]
del tabla ["promo_code"]
tabla

#ME QUEDO SOLO CON LOS PRIMEROS 50 RESULTADOS
tabla.drop(tabla.index[51:100], inplace=True)
tabla

#BORRAMOS POSIBLES DUPLICADOS
tabla.drop_duplicates()
tabla

#Ordenamos los productos de valor mas elevado al mas barato.
tabla.sort_values(by=['price'], inplace=True, ascending=False)
tabla

#3-Conexion a Base de datos  

urll="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="mendietadaniel1994_coderhouse"
with open ("pass.txt") as f:
    pwd=f.read()
try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print("Connected to Redshift successfully!")
    
except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)  
#Especifico la tabla
def cargar_en_redshift(conn, table_name, dataframe):
    dtypes= dataframe.dtypes
    cols= list(dtypes.index )
    tipos= list(dtypes.values)
    type_map = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)','bool':'BOOLEAN'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    table_schema = f"""
                         {table_name} (
            {', '.join(column_defs)}
        );
        """
 #Ingreso los valores
    cur = conn.cursor()
    cur.execute(table_schema)
    # Generar los valores a insertar
    values = [tuple(x) for x in dataframe.to_numpy()]
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')   


#Declaracion de parametros.
cargar_en_redshift(conn=conn, table_name='proyecto2', dataframe=tabla)    
