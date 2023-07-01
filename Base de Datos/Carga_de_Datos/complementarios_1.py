import pandas as pd
import mysql.connector

dtypesProvincia = {
    'cod_provincia': str,
    'nombre_provincia': str,
    'empleo_bruto': float,
    'empleo_no_remunerado': float,
    'tasa_desempleo': float,
    'tasa_analfabetismo': float,
    'asistencia_edu_basica': float
}

dtypesCanton = {
    'cod_canton': str,
    'nombre_canton': str
}

cols = ['cod_parroquia', 'nombre_parroquia']
dtype = {col: str for col in cols}

dfProvincias = pd.read_csv('C:/Users/Daniel/Utpl/4TO CICLO/ProyectoIntegrador/Complementarios/Provincias.csv',
                           sep=';',
                           header=0,
                           encoding='latin1',
                           dtype=dtypesProvincia,
                           decimal=',')

dfCantones = pd.read_csv('C:/Users/Daniel/Utpl/4TO CICLO/ProyectoIntegrador/Complementarios/Cantones.csv',
                         sep=';',
                         header=0,
                         encoding='latin1',
                         dtype=dtypesCanton)

dfParroquias = pd.read_csv('C:/Users/Daniel/Utpl/4TO CICLO/ProyectoIntegrador/Complementarios/Parroquias.csv',
                           sep=';',
                           names=cols,
                           header=0,
                           encoding='latin1',
                           dtype=dtype)

dfProvincias = dfProvincias.fillna(0)

# Establish a connection to the MySQL database
cnx = mysql.connector.connect(
    host='your_host',
    user='your_user',
    password='password',
    database='database'
)

cursor01 = cnx.cursor()
for index, row in dfProvincias.iterrows():
    # Construct the INSERT query
    query = "INSERT INTO provincia (cod_provincia, nombre_provincia, tasa_desempleo, empleo_no_remunerado, " \
            "empleo_bruto, asistencia_edu_basica, tasa_analfabetismo) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    values = (row['cod_provincia'], row['nombre_provincia'], row['empleo_bruto'], row['empleo_no_remunerado'],
              row['tasa_desempleo'], row['tasa_analfabetismo'], row['asistencia_edu_basica'])
    # Execute the query
    cursor01.execute(query, values)
# Commit the changes to the database
cnx.commit()
# Close the cursor and connection
cursor01.close()

cursor02 = cnx.cursor()
for index, row in dfCantones.iterrows():
    cod_canton = row['cod_canton']
    nombre_canton = row['nombre_canton']

    # Transformations
    if len(str(cod_canton)) == 3:
        provincia_canton = cod_canton[0]
    elif len(str(cod_canton)) == 4:
        provincia_canton = cod_canton[0:2]

    query = "INSERT INTO canton(cod_canton, nombre_canton, provincia_canton) VALUES (%s, %s, %s)"
    values = (cod_canton, nombre_canton, provincia_canton)
    cursor02.execute(query, values)
# Commit the changes to the database
cnx.commit()
# Close the cursor and connection
cursor02.close()

cursor03 = cnx.cursor()
for index, row in dfParroquias.iterrows():
    cod = row['cod_parroquia']
    nombre_parroquia = row['nombre_parroquia']

    # Transformations
    if len(str(cod)) == 5:
        cod_canton = cod[0:3]
    elif len(str(cod)) == 6:
        cod_canton = cod[0:4]

    query = "INSERT INTO parroquia(cod_parroquia, nombre_parroquia, cod_canton) VALUES (%s, %s, %s)"
    values = (cod, nombre_parroquia, cod_canton)

    cursor03.execute(query, values)
# Commit the changes to the database
cnx.commit()
# Close the cursor and connection
cursor03.close()
cnx.close()