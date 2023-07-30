
#Recordar instalar pandas y mysql-connector
import pandas as pd
import mysql.connector

#Columnas del csv
cols = ['area', 'ciudad', 'conglomerado', 'panelm', 'nro_vivienda', 'nro_hogar', 'acceso_principal',
        'tipo_vivienda', 'techo_material', 'estado_techo', 'piso_material', 'estado_piso', 'pared_material',
        'estado_pared', 'nro_cuartos', 'nro_dormitorios', 'nro_cuartos_negocio', 'cocina_cuarto',
        'cocinar_material', 'servicio_higienico', 'sh_alternativa', 'sh_prestado', 'agua_obtencion',
        'agua_medidor', 'agua_junta', 'agua_tuberia', 'ducha', 'tipo_alumbrado', 'eliminacion_basura',
        'tipo_tenencia', 'valor_arriendo', 'agua_incluida', 'luz_incluida', 'parentesco', 'posesion_vehiculos',
        'cantidad_vehiculos', 'posesion_motos', 'cantidad_motos', 'abastecimiento_super', 'gasto_super',
        'abastecimiento_extra', 'gasto_extra', 'abastecimiento_diesel', 'gasto_diesel', 'abastecimiento_eco',
        'gasto_eco', 'abastecimiento_elect', 'gasto_elect', 'abastecimiento_gas', 'gasto_gas', 'estrato',
        'fexp', 'upm', 'id_vivienda', 'id_hogar', 'periodo', 'mes']

#Colocar todas las columnas como tipo String
dtype = {col: str for col in cols}

#Leer csv con Raw data
ubiRawData = '/workspace/zeppelin-paavanzada/enemdu_vivienda_hogar_2023_I_trimestre.csv'

df = pd.read_csv(ubiRawData,
    sep=';',
    names=cols,
    header=0,
    dtype=dtype)

#Convertir a numerico las columnas necesarias
df['valor_arriendo'] = pd.to_numeric(df['valor_arriendo'], errors='coerce').astype('Int64')
df['gasto_super'] = pd.to_numeric(df['gasto_super'], errors='coerce').astype('Int64')
df['gasto_extra'] = pd.to_numeric(df['gasto_extra'], errors='coerce').astype('Int64')
df['gasto_diesel'] = pd.to_numeric(df['gasto_diesel'], errors='coerce').astype('Int64')
df['gasto_eco'] = pd.to_numeric(df['gasto_eco'], errors='coerce').astype('Int64')
df['gasto_elect'] = pd.to_numeric(df['gasto_elect'], errors='coerce').astype('Int64')
df['gasto_gas'] = pd.to_numeric(df['gasto_gas'], errors='coerce').astype('Int64')

# -----------------------------------------------------------------------------------------------------------------------

# Tabla VIVIENDA
viviendas = df[['id_vivienda', 'tipo_vivienda', 'nro_vivienda', 'estrato', 'area', 'conglomerado', 'ciudad']]
# Drop duplicate rows based on 'id_vivienda'
viviendas = viviendas.drop_duplicates(subset='id_vivienda')
# Reset the index of the new dataframe
viviendas = viviendas.reset_index(drop=True)

#Tabla HOGAR
hogar = df[['id_vivienda', 'nro_hogar', 'tipo_tenencia', 'acceso_principal', 'cantidad_vehiculos', 'cantidad_motos', 'ducha',
            'cocinar_material', 'techo_material', 'estado_techo', 'piso_material', 'estado_piso', 'pared_material',
            'estado_pared', 'valor_arriendo', 'fexp', 'upm', 'periodo', 'mes', 'id_hogar']]

#Tabla CUARTOS
cuartos = df[['nro_cuartos', 'nro_dormitorios', 'nro_cuartos_negocio', 'cocina_cuarto', 'id_hogar']]

#Tabla SERVICIOS BÁSICOS
servicios_basicos = df[['servicio_higienico', 'sh_alternativa', 'sh_prestado', 'agua_obtencion', 'tipo_alumbrado',
                        'agua_medidor', 'agua_junta', 'agua_tuberia', 'eliminacion_basura', 'id_hogar']]

#Tabla DATOS ARRIENDO
datos_arriendo = df[['agua_incluida', 'luz_incluida', 'parentesco', 'id_hogar']]

#Tabla HOGAR_COMBUSTIBLES
hogar_combustibles = df[['id_hogar', 'abastecimiento_super', 'gasto_super', 'abastecimiento_extra',
                         'gasto_extra', 'abastecimiento_diesel', 'gasto_diesel', 'abastecimiento_eco',
                         'gasto_eco', 'abastecimiento_elect', 'gasto_elect', 'abastecimiento_gas', 'gasto_gas']]


# -----------------------------------------------------------------------------------------------------------------------
# Establecer coneccion MySQL database asumiendo que se ejecutó el DDL
cnx = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='',
    database='mydb_integrador'
)

# -----------------------------------------------------------------------------------------------------------------------
# Create a cursor object to execute SQL queries
cursor04 = cnx.cursor()

for index, row in viviendas.iterrows():
    id_vivienda = row['id_vivienda']
    tipo_vivienda = row['tipo_vivienda']
    nro_vivienda = row['nro_vivienda']
    estrato = row['estrato']
    area = row['area']
    conglomerado = row['conglomerado']
    cod_parroquia = row['ciudad']

    #Controlar ciudades desconocidas
    if cod_parroquia == '100300' or cod_parroquia == '130400' or cod_parroquia == '230200':
        cod_parroquia = '909999'

    # Perform any necessary transformations or formatting on the data
    if area == '1':
        area = 'Urbana'
    else:
        area = 'Rural'

    if tipo_vivienda == '1':
        tipo_vivienda = 'Casa o Villa'
    elif tipo_vivienda == '2':
        tipo_vivienda = 'Departamento'
    elif tipo_vivienda == '3':
        tipo_vivienda = 'Cuartos en casa de inquilinato'
    elif tipo_vivienda == '4':
        tipo_vivienda = 'Mediagua'
    elif tipo_vivienda == '5':
        tipo_vivienda = 'Rancho, covacha'
    elif tipo_vivienda == '6':
        tipo_vivienda = 'Choza'
    elif tipo_vivienda == '7':
        tipo_vivienda = 'Otra'
    else:
        tipo_vivienda = ''

    # Construct the INSERT query
    query = "INSERT INTO vivienda (id_vivienda, tipo_vivienda, nro_vivienda, estrato, area, conglomerado, cod_parroquia) " \
            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    values = (id_vivienda, tipo_vivienda, nro_vivienda, estrato, area, conglomerado, cod_parroquia)
    # Execute the query
    cursor04.execute(query, values)
# Commit the changes to the database
cnx.commit()
# Close the cursor and connection
cursor04.close()

# -----------------------------------------------------------------------------------------------------------------------
cursor05 = cnx.cursor()
for index, row in hogar.iterrows():
    id_hogar = row['id_hogar']
    id_vivienda = row['id_vivienda']
    nro_hogar = row['nro_hogar']
    tipo_tenencia = row['tipo_tenencia']
    acceso_principal = row['acceso_principal']
    cant_vehiculos = row['cantidad_vehiculos']
    cant_motos = row['cantidad_motos']
    ducha = row['ducha']
    combustible_cocina = row['cocinar_material']
    material_techo = row['techo_material']
    estado_techo = row['estado_techo']
    material_piso = row['piso_material']
    estado_piso = row['estado_piso']
    material_paredes = row['pared_material']
    estado_paredes = row['estado_pared']
    valor_arriendo = row['valor_arriendo']
    fexp = row['fexp']
    upm = row['upm']
    periodo = row['periodo']
    mes = row['mes']

    # Perform any necessary transformations or formatting on the data
    if tipo_tenencia == '1':
        tipo_tenencia = 'En Arriendo'
    elif tipo_tenencia == '2':
        tipo_tenencia = 'Anticresis y/o arriendo'
    elif tipo_tenencia == '3':
        tipo_tenencia = 'Propia y la esta pagando'
    elif tipo_tenencia == '4':
        tipo_tenencia = 'Propia y totalmente pagada'
    elif tipo_tenencia == '5':
        tipo_tenencia = 'Cedida'
    elif tipo_tenencia == '6':
        tipo_tenencia = 'Recibida por servicios'
    elif tipo_tenencia == '7':
        tipo_tenencia = 'Otra'
    else:
        tipo_tenencia = ''

    ##  Acceso principal
    if acceso_principal == '1':
        acceso_principal = 'Carretera, calle pavimentada'
    elif acceso_principal == '2':
        acceso_principal = 'Empedrado'
    elif acceso_principal == '3':
        acceso_principal = 'Lastrado, calle de tierra'
    elif acceso_principal == '4':
        acceso_principal = 'Sendero'
    elif acceso_principal == '5':
        acceso_principal = 'Rio, mar'
    elif acceso_principal == '6':
        acceso_principal = 'Otro'

    ##  ducha
    if ducha == '1':
        ducha = 'Exclusivo del hogar'
    elif ducha == '2':
        ducha = 'Compartido con otros hogares'
    elif ducha == '3':
        ducha = 'No tiene'
    else:
        ducha = ''

    ##  combustible_cocina
    if combustible_cocina == '1':
        combustible_cocina = 'Gas'
    elif combustible_cocina == '2':
        combustible_cocina = 'Leña, carbon'
    elif combustible_cocina == '3':
        combustible_cocina = 'Electricidad'
    elif combustible_cocina == '4':
        combustible_cocina = 'Otro'
    else:
        combustible_cocina = ''

    ##  material_techo
    if material_techo == '1':
        material_techo = 'Hormigón (losa, cemento)'
    elif material_techo == '2':
        material_techo = 'Fibrocemento, asbesto (eternit, eurolit)'
    elif material_techo == '3':
        material_techo = 'Zinc, Aluminio'
    elif material_techo == '4':
        material_techo = 'Teja'
    elif material_techo == '5':
        material_techo = 'Palma, paja u hoja'
    elif material_techo == '6':
        material_techo = 'Otro Material'
    else:
        material_techo = ''

    ##  estado_techo
    if estado_techo == '1':
        estado_techo = 'Bueno'
    elif estado_techo == '2':
        estado_techo = 'Regular'
    elif estado_techo == '3':
        estado_techo = 'Malo'
    else:
        estado_techo = ''

    ##  material_piso
    if material_piso == '1':
        material_piso = 'Duela, parquet, tablón tratado o piso flotante'
    elif material_piso == '2':
        material_piso = 'Cerámica, baldosa, vinil o porcelanato'
    elif material_piso == '3':
        material_piso = 'Mármol o marmeton'
    elif material_piso == '4':
        material_piso = 'Ladrillo o cemento'
    elif material_piso == '5':
        material_piso = 'Tabla / tablón no tratado'
    elif material_piso == '6':
        material_piso = 'Caña'
    elif material_piso == '7':
        material_piso = 'Tierra'
    elif material_piso == '8':
        material_piso = 'Otro Material'
    else:
        material_piso = ''

    ##  estado_piso
    if estado_piso == '1':
        estado_piso = 'Bueno'
    elif estado_piso == '2':
        estado_piso = 'Regular'
    elif estado_piso == '3':
        estado_piso = 'Malo'
    else:
        estado_piso = ''
    
    ##  material_paredes
    if material_paredes == '1':
        material_paredes = 'Hormigón/Ladrillo o Bloque'
    elif material_paredes == '2':
        material_paredes = 'Asbesto/Cemento (Fibrolit)'
    elif material_paredes == '3':
        material_paredes = 'Adobe o Tapia'
    elif material_paredes == '4':
        material_paredes = 'Madera'
    elif material_paredes == '5':
        material_paredes = 'Caña revestida o bahareque'
    elif material_paredes == '6':
        material_paredes = 'Caña no revestida o estera'
    elif material_paredes == '7':
        material_paredes = 'Otro material'
    else:
        material_paredes = ''

    ##  estado_paredes
    if estado_paredes == '1':
        estado_paredes = 'Bueno'
    elif estado_paredes == '2':
        estado_paredes = 'Regular'
    elif estado_paredes == '3':
        estado_paredes = 'Malo'
    else:
        estado_paredes = ''

    if cant_vehiculos == ' ':
        cant_vehiculos = 0

    if cant_motos == ' ':
        cant_motos = 0
    
    # Construct the INSERT query
    query = "INSERT INTO hogar (id_hogar, id_vivienda, nro_hogar, tipo_tenencia, acceso_principal, " \
            "cant_vehiculos, cant_motos, ducha, combustible_cocina, material_techo, " \
            "estado_techo, material_piso, estado_piso, material_paredes, estado_paredes, valor_arriendo," \
            "fexp, upm, periodo, mes) " \
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = (id_hogar, id_vivienda, nro_hogar, tipo_tenencia, acceso_principal, cant_vehiculos, cant_motos,
              ducha, combustible_cocina, material_techo, estado_techo, material_piso, estado_piso,
              material_paredes, estado_paredes, valor_arriendo, fexp, upm, periodo, mes)

    # Execute the query
    cursor05.execute(query, values)
# Commit the changes to the database
cnx.commit()
# Close the cursor and connection
cursor05.close()
# -----------------------------------------------------------------------------------------------------------------------
# Create a cursor2 object to execute SQL queries
cursor06 = cnx.cursor()
for index, row in cuartos.iterrows():
    nro_cuartos = row['nro_cuartos']
    nro_dormitorios = row['nro_dormitorios']
    nro_cuartos_negocio = row['nro_cuartos_negocio']
    cuarto_cocina = row['cocina_cuarto']
    id_hogar = row['id_hogar']

    # Perform any necessary transformations or formatting on the data
    if cuarto_cocina == '1':
        cuarto_cocina = 'Si'
    else:
        cuarto_cocina = 'No'
    # Construct the INSERT query
    query = "INSERT INTO cuartos (nro_cuartos, nro_dormitorios, nro_cuartos_negocio, cuarto_cocina, id_hogar) VALUES (%s, %s, %s, %s, %s)"
    values = (nro_cuartos, nro_dormitorios, nro_cuartos_negocio, cuarto_cocina, id_hogar)
    cursor06.execute(query, values)
# Commit the changes to the database
cnx.commit()
cursor06.close()
# ----------------------------------------------------------------------------------------------------------------------
cursor07 = cnx.cursor()
for index, row in servicios_basicos.iterrows():
    servicio_higienico = row['servicio_higienico']
    sshh_alternativa = row['sh_alternativa']
    sshh_prestado = row['sh_prestado']
    obtencion_agua = row['agua_obtencion']
    tipo_alumbrado = row['tipo_alumbrado']
    agua_medidor = row['agua_medidor']
    agua_junta = row['agua_junta']
    tipo_tuberia = row['agua_tuberia']
    eliminacion_basura = row['eliminacion_basura']
    id_hogar = row['id_hogar']

    if servicio_higienico == '1':
        servicio_higienico = 'Excusado y alcantarillado'
    elif servicio_higienico == '2':
        servicio_higienico = 'Excusado y pozo séptico'
    elif servicio_higienico == '3':
        servicio_higienico = 'Excusado y pozo ciego'
    elif servicio_higienico == '4':
        servicio_higienico = 'Letrina'
    elif servicio_higienico == '5':
        servicio_higienico = 'No tiene'
    else:
        servicio_higienico = ''

    if sshh_alternativa == '1':
        sshh_alternativa = 'Descarga directa al mar, río, lago o quebrada'
    elif sshh_alternativa == '2':
        sshh_alternativa = 'Van al monte, campo, bota la basura en paquete'
    elif sshh_alternativa == '3':
        sshh_alternativa = 'Usan una instalación sanitaria cercana y/o prestada'
    else:
        sshh_alternativa = ''

    if sshh_prestado == '1':
        sshh_prestado = 'Excusado y alcantarillado'
    elif sshh_prestado == '2':
        sshh_prestado = 'Excusado y pozo séptico'
    elif sshh_prestado == '3':
        sshh_prestado = 'Excusado y pozo ciego'
    elif sshh_prestado == '4':
        sshh_prestado = 'Letrina'
    else:
        sshh_prestado = ''

    if obtencion_agua == '1':
        obtencion_agua = 'Red pública'
    elif obtencion_agua == '2':
        obtencion_agua = 'Pila o llave pública'
    elif obtencion_agua == '3':
        obtencion_agua = 'Otra fuente por tubería'
    elif obtencion_agua == '4':
        obtencion_agua = 'Carro repartidor, triciclo'
    elif obtencion_agua == '5':
        obtencion_agua = 'Pozo'
    elif obtencion_agua == '6':
        obtencion_agua = 'Río, vertiente, acequia'
    elif obtencion_agua == '7':
        obtencion_agua = 'Otro'
    else:
        obtencion_agua = ''

    if agua_medidor == '1':
        agua_medidor = 'Si'
    elif agua_medidor == '2':
        agua_medidor = 'No'
    else:
        agua_medidor = ''

    if agua_junta == '1':
        agua_junta = 'Si'
    elif agua_junta == '2':
        agua_junta = 'No'
    else:
        agua_junta = ''

    if tipo_alumbrado == '1':
        tipo_alumbrado = 'Empresa eléctrica pública'
    elif tipo_alumbrado == '2':
        tipo_alumbrado = 'Planta eléctrica privada'
    elif tipo_alumbrado == '3':
        tipo_alumbrado = 'Vela, candil, mechero, gas'
    else:
        tipo_alumbrado = 'Ninguno'


    if tipo_tuberia == '1':
        tipo_tuberia = 'Por tubería dentro de la vivienda'
    elif tipo_tuberia == '2':
        tipo_tuberia = 'Por tubería fuera de la vivienda pero en el lote'
    elif tipo_tuberia == '3':
        tipo_tuberia = 'Por tubería fuera de la vivienda, lote o terreno'
    elif tipo_tuberia == '4':
        tipo_tuberia = 'No recibe agua por tubería sino por otros medios'
    else:
        tipo_tuberia = ''

    if eliminacion_basura == '1':
        eliminacion_basura = 'Contratan el servicio'
    elif eliminacion_basura == '2':
        eliminacion_basura = 'Servicio municipal'
    elif eliminacion_basura == '3':
        eliminacion_basura = 'Botan a la calle, quebrada, río'
    elif eliminacion_basura == '4':
        eliminacion_basura = 'La queman, entierran'
    elif eliminacion_basura == '5':
        eliminacion_basura = 'Otra'
    else:
        eliminacion_basura = ''

    # Construct the INSERT query for servicios_basicos table
    query = "INSERT INTO servicios_basicos (servicio_higienico, sshh_alternativa, sshh_prestado, agua_obtencion, tipo_alumbrado, agua_medidor, agua_junta, agua_tuberia, eliminacion_basura, id_hogar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = (
        servicio_higienico, sshh_alternativa, sshh_prestado, obtencion_agua, tipo_alumbrado, agua_medidor, agua_junta,
        tipo_tuberia, eliminacion_basura, id_hogar)
    cursor07.execute(query, values)
    # Commit the changes to the database
cnx.commit()
cursor07.close()
# ----------------------------------------------------------------------------------------------------------------

# Create a cursor2 object to execute SQL queries
cursor08 = cnx.cursor()
for index, row in datos_arriendo.iterrows():
    agua_incluida = row['agua_incluida']
    luz_incluida = row['luz_incluida']
    parentesco_propietario = row['parentesco']
    id_hogar = row['id_hogar']

    # Perform any necessary transformations or formatting on the data
    if agua_incluida == '1':
        agua_incluida = 'Si'
    else:
        agua_incluida = 'No'

    if luz_incluida == '1':
        luz_incluida = 'Si'
    else:
        luz_incluida = 'No'

    if parentesco_propietario == '1':
        parentesco_propietario = 'Si'
    else:
        parentesco_propietario = 'No'
    # Construct the INSERT query
    query = "INSERT INTO datos_arriendo (agua_incluida, luz_incluida, parentesco_propietario, id_hogar) " \
            "VALUES (%s, %s, %s, %s)"
    values = (agua_incluida, luz_incluida, parentesco_propietario, id_hogar)
    cursor08.execute(query, values)
# Commit the changes to the database
cnx.commit()
cursor08.close()
# -----------------------------------------------------------------------------------------------------------------------

cursor09 = cnx.cursor()
for index, row in hogar_combustibles.iterrows():
    id_hogar = row['id_hogar']
    gasto_super = row['gasto_super']
    gasto_extra = row['gasto_extra']
    gasto_diesel = row['gasto_diesel']
    gasto_eco = row['gasto_eco']
    gasto_elect = row['gasto_elect']
    gasto_gas = row['gasto_gas']
    # Perform any necessary transformations or formatting on the data
    if gasto_super is not None and not pd.isna(gasto_super):
        tipo_combustible = 'Súper'
        gasto_combustible = gasto_super
        query = "INSERT INTO hogar_combustibles (id_hogar, tipo_combustible, gasto_combustible) " \
                "VALUES (%s, %s, %s)"
        values = (id_hogar, tipo_combustible, gasto_combustible)
        cursor09.execute(query, values)
    if gasto_extra is not None and not pd.isna(gasto_extra):
        tipo_combustible = 'Extra'
        gasto_combustible = gasto_extra
        query = "INSERT INTO hogar_combustibles (id_hogar, tipo_combustible, gasto_combustible) " \
                "VALUES (%s, %s, %s)"
        values = (id_hogar, tipo_combustible, gasto_combustible)
        cursor09.execute(query, values)
    if gasto_diesel is not None and not pd.isna(gasto_diesel):
        tipo_combustible = 'Diésel'
        gasto_combustible = gasto_diesel
        query = "INSERT INTO hogar_combustibles (id_hogar, tipo_combustible, gasto_combustible) " \
                "VALUES (%s, %s, %s)"
        values = (id_hogar, tipo_combustible, gasto_combustible)
        cursor09.execute(query, values)
    if gasto_eco is not None and not pd.isna(gasto_eco):
        tipo_combustible = 'Ecopaís'
        gasto_combustible = gasto_eco
        query = "INSERT INTO hogar_combustibles (id_hogar, tipo_combustible, gasto_combustible) " \
                "VALUES (%s, %s, %s)"
        values = (id_hogar, tipo_combustible, gasto_combustible)
        cursor09.execute(query, values)
    if gasto_elect is not None and not pd.isna(gasto_elect):
        tipo_combustible = 'Electricidad'
        gasto_combustible = gasto_elect
        query = "INSERT INTO hogar_combustibles (id_hogar, tipo_combustible, gasto_combustible) " \
                "VALUES (%s, %s, %s)"
        values = (id_hogar, tipo_combustible, gasto_combustible)
        cursor09.execute(query, values)

# Commit the changes to the database
cnx.commit()
cursor09.close()
cnx.close()
