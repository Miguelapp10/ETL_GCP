################################################################################################################################################
#CONSOLIDAD DE STORE PICKUP - LIMA
################################################################################################################################################
import pandas as pd
from pandas_gbq import to_gbq
import time
import numpy as np
import datetime as dt  # Para fechas (opcional)
import glob  # Para jalar todo los archivos en una carpeta
import os  # Para trabajar con rutas
# Usuario
usuario = r'Mpazos'
# Ruta
ruta = r'Devolucion_Postventa - Documentos' 
# 1. Rutas base
ruta_base = r'C:\\Users\\' + usuario + r'\\Falabella\\' + ruta 
## lista Personal de recolección diaria
ruta_Tienda = ruta_base + r'\\PRD_Tienda'
ruta_Lista_STORE_PICKUP = ruta_base + r'\\PRD_Tienda\\Lista_STORE_PICKUP'
ruta_lista_Seguimiento_Recoleccion = ruta_base + r'\\PRD_Tienda\\Lista_Seguimiento_Recoleccion'
ruta_consolidado_Provincia = ruta_base + r'\\PRD_Tienda\\Lista_PRD_Provincia\\Consolidado_Provincia'

# Importar librerías de funciones

import datetime as dt  # Para fechas (opcional)
import glob  # Para jalar todo los archivos en una carpeta
import os  # Para trabajar con rutas
import warnings  # Para evitar que salgan errores en formatos de archivo (no altera el producto)
from datetime import datetime, timedelta # Para fechas (opcional)
from io import StringIO  # Usado para definir función nueva
from pathlib import Path
import numpy as np  # Para operaciones matemáticas
import xlsxwriter  # Funcionalidad para trabajar con archivos Excel
from fpdf import FPDF  # Para crear archivos PDF
from pandas import ExcelWriter  # Para exportar tabla a Excel
from xlsx2csv import Xlsx2csv  # Usado para definir función nueva
import time

# 1. Rutas base
### Personal de recoleccion diaria
ruta_PICKUP = glob.glob(os.path.join(ruta_Lista_STORE_PICKUP, ('Consolidado STORE PICK-UP'  +  '*.xlsx')))
# Verificar si se encontraron archivos
if not ruta_PICKUP:
    raise FileNotFoundError("No se encontraron archivos que coincidan con el patrón especificado.")

ruta_PICKUP_ = []  # Initialize an empty list instead of a DataFrame

for i in range(len(ruta_PICKUP)):
    try:
        x = pd.read_excel(ruta_PICKUP[i], 'CONSOLIDADO TOTAL', dtype={"num_rastreo": str},header=0)
        ruta_PICKUP_.append(x)  # Append each DataFrame to the list
    except Exception as e:
        print(f"Error al leer el archivo {i}: {e}")

# Verificar si se agregaron DataFrames a la lista
if not ruta_PICKUP_:
    raise ValueError("No se pudieron leer datos de ningún archivo.")

ruta_PICKUP_ = pd.concat(ruta_PICKUP_, ignore_index=True)

# Quitar inconsistencias en nombres de columnas
ruta_PICKUP = ruta_PICKUP_ ##.rename(columns=lambda x: x.strip()).copy()

# Cambiar el nombre de la columna
ruta_PICKUP = ruta_PICKUP.rename(columns={'Sub Proceso': 'Sub_Proceso','big ticket':'big_ticket','ID Tienda':'ID_Tienda',
                                          'Tipo Devolución':'Tipo_Devolucion','Tipo Vehiculo':'Tipo_Vehiculo',
                                          'Nombre_Tienda':'TIENDA',
                                          'Placa SR':'Placa_SR','Fecha de Planifiación':'Fecha_Planificacion','Fecha de ruta':'Fecha_ruta'})
##print(ruta_SR)
ruta_PICKUP = ruta_PICKUP[[ 'Sub_Proceso','numero_order','num_rastreo','sku_simple','sku_name','status','date_status',
                           'item_shipment_method','Num_Items','big_ticket','ID_Tienda','TIENDA','Zona','BU',
                           'Tipo_Devolucion','Guia','Tipo_Vehiculo','Placa_SR','Fecha_Planificacion','Fecha_ruta']]

from google.cloud import storage, bigquery

# Convertir la columna "numero_order" a tipo de datos str
ruta_PICKUP['numero_order'] = ruta_PICKUP['numero_order'].astype(str)
ruta_PICKUP['num_rastreo'] = ruta_PICKUP['num_rastreo'].astype(str)
ruta_PICKUP['sku_simple'] = ruta_PICKUP['sku_simple'].astype(str)
ruta_PICKUP['sku_name'] = ruta_PICKUP['sku_name'].astype(str)
ruta_PICKUP['status'] = ruta_PICKUP['status'].astype(str)
ruta_PICKUP['date_status'] = ruta_PICKUP['date_status'].astype(str)
ruta_PICKUP['ID_Tienda'] = ruta_PICKUP['ID_Tienda'].astype(str)
# Convert the column to string type
ruta_PICKUP['item_shipment_method'] = ruta_PICKUP['item_shipment_method'].astype(str)
#ruta_PICKUP['Fecha_ruta'] = pd.to_datetime(ruta_PICKUP['Fecha_ruta'], unit='s').dt.strftime('%Y-%m-%d')
#ruta_PICKUP['Fecha_ruta'] = pd.to_datetime(ruta_PICKUP['Fecha_ruta'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
#ruta_PICKUP['Fecha_Planificacion'] = pd.to_datetime(ruta_PICKUP['Fecha_Planificacion'], unit='s').dt.strftime('%Y-%m-%d')
# Convierte la columna a formato de fecha sin especificar 'unit' y formatea
# Identifica las filas con fechas no válidas
#invalid_dates = ruta_PICKUP[~pd.to_datetime(ruta_PICKUP['Fecha_Planificacion'], errors='coerce').notna()]
# Muestra las fechas inválidas para revisión
#print("Fechas no válidas encontradas:", invalid_dates['Fecha_Planificacion'])
# Ahora convierte sólo las fechas válidas
ruta_PICKUP['Fecha_Planificacion'] = pd.to_datetime(ruta_PICKUP['Fecha_Planificacion'], errors='coerce').dt.strftime('%Y-%m-%d')
#ruta_PICKUP['Fecha_Planificacion'] = pd.to_datetime(ruta_PICKUP['Fecha_Planificacion'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
# Assuming ruta_PICKUP is your DataFrame
#ruta_PICKUP['Fecha_Planificacion'] = pd.to_datetime(ruta_PICKUP['Fecha_Planificacion']).dt.strftime('%Y-%m-%d')
ruta_PICKUP['Fecha_ruta'] = pd.to_datetime(ruta_PICKUP['Fecha_ruta']).dt.strftime('%Y-%m-%d')

ruta_PICKUP['TIENDA'] = ruta_PICKUP['TIENDA'].str.upper()

### Cambiar los valores de las columnas
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 09', 'Placa_SR'] = 'MOY09'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 10', 'Placa_SR'] = 'MOY10'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 11', 'Placa_SR'] = 'MOY11'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 12', 'Placa_SR'] = 'MOY12'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 13', 'Placa_SR'] = 'MOY13'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 14', 'Placa_SR'] = 'MOY14'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 18', 'Placa_SR'] = 'MOY18'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 19', 'Placa_SR'] = 'MOY19'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 20', 'Placa_SR'] = 'MOY20'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY IL 21', 'Placa_SR'] = 'MOY21'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY SPU 01', 'Placa_SR'] = 'MOY01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY SPU 02', 'Placa_SR'] = 'MOY02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY SPU 03', 'Placa_SR'] = 'MOY03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY SPU 12', 'Placa_SR'] = 'MOY12'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'MOY SPU 13', 'Placa_SR'] = 'MOY13'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTMOY01', 'Placa_SR'] = 'MOY01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTMOY02', 'Placa_SR'] = 'MOY02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTMOY03', 'Placa_SR'] = 'MOY03'

ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ01', 'Placa_SR'] = 'ACJ01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ02', 'Placa_SR'] = 'ACJ02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ03', 'Placa_SR'] = 'ACJ03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ04', 'Placa_SR'] = 'ACJ04'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ05', 'Placa_SR'] = 'ACJ05'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ SPU 01', 'Placa_SR'] = 'ACJ01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ SPU 02', 'Placa_SR'] = 'ACJ02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ SPU 03', 'Placa_SR'] = 'ACJ03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ SPU 04', 'Placa_SR'] = 'ACJ02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ACJ SPU 05', 'Placa_SR'] = 'ACJ03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTACJ01', 'Placa_SR'] = 'ACJ01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTACJ02', 'Placa_SR'] = 'ACJ02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTACJ03', 'Placa_SR'] = 'ACJ03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTACJ04', 'Placa_SR'] = 'ACJ04'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTACJ05', 'Placa_SR'] = 'ACJ05'

ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL IL 01', 'Placa_SR'] = 'SEDEL01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL IL 02', 'Placa_SR'] = 'SEDEL02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL IL 03', 'Placa_SR'] = 'SEDEL03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL IL 04', 'Placa_SR'] = 'SEDEL04'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL IL 11', 'Placa_SR'] = 'SEDEL11'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL IL 12', 'Placa_SR'] = 'SEDEL12'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL IL 13', 'Placa_SR'] = 'SEDEL13'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL SPU 01', 'Placa_SR'] = 'SEDEL01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL SPU 02', 'Placa_SR'] = 'SEDEL02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL SPU 03', 'Placa_SR'] = 'SEDEL03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'SEDEL SPU 04', 'Placa_SR'] = 'SEDEL04'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTSEDEL01', 'Placa_SR'] = 'SEDEL01'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTSEDEL02', 'Placa_SR'] = 'SEDEL02'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTSEDEL03', 'Placa_SR'] = 'SEDEL03'
ruta_PICKUP.loc[ruta_PICKUP['Placa_SR'] == 'ILTSEDEL04', 'Placa_SR'] = 'SEDEL04'

# Apply the row number logic
ruta_PICKUP['Repro'] = ruta_PICKUP.sort_values(by='Fecha_ruta').groupby(['numero_order','num_rastreo','sku_simple']).cumcount() + 1
# Define tu proyecto de BigQuery
project_id = "bi-local-pe"

# Define el nombre del conjunto de datos y de la tabla en BigQuery
dataset_id = "Devolucion"
table_id = "STORE"

# Carga el DataFrame a BigQuery
ruta_PICKUP.to_gbq(destination_table=f"{dataset_id}.{table_id}",
                   project_id=project_id,
                   if_exists="replace")  # Opciones: "append", "replace", "fail"

print("Los datos se han cargado exitosamente en BigQuery.")

################################################################################################################################################
#SEGUIMIENTO DE RECOLECCION
################################################################################################################################################
# 1. Rutas base
### Personal de recoleccion diaria

# ruta_SR = glob.glob(os.path.join(ruta_lista_Seguimiento_Recoleccion, ('Seguimiento recoleccion'  +  '*.xlsx')))
# ruta_SR_ = []  # Initialize an empty list instead of a DataFrame

# for i in range(len(ruta_SR)):
#     x = pd.read_excel(ruta_SR[i], 'Sheet1', header=0)
#     ruta_SR_.append(x)  # Append each DataFrame to the list

# ruta_SR_ = pd.concat(ruta_SR_, ignore_index=True)

# Quitar inconsistencias en nombres de columnas
#ruta_SR = ruta_SR_ ##.rename(columns=lambda x: x.strip()).copy()


# Cambiar el nombre de la columna
#ruta_SR = ruta_SR.rename(columns={'#': 'Planificado','COMENTARIO TRANSPORTE':'COMENTARIO_TRANSPORTE','Fecha planificada':'Fecha_ruta'})

#ruta_SR = ruta_SR[[ 'TIENDA','Planificado','Checkin','Checkout','RESULTADO','OBS','COMENTARIO_TRANSPORTE','Fecha_ruta']]
#ruta_SR[[ 'Tracking ID','Id de referencia','Fecha planificada','Vehículo','Título','Planificado','Checkin','Checkout','Estado',
 #                  'Comentarios','Observaciones','Persona de contacto','RESULTADO','OBS','COMENTARIO TRANSPORTE','TIENDA']]
# Convertir la columna "numero_order" a tipo de datos str
# ruta_SR['COMENTARIO_TRANSPORTE'] = ruta_SR['COMENTARIO_TRANSPORTE'].astype(str)
# ruta_SR['OBS'] = ruta_SR['OBS'].astype(str)
# ruta_SR['TIENDA'] = ruta_SR['TIENDA'].str.upper()

# # Convert it to datetime first
# ruta_SR['Fecha_ruta'] = pd.to_datetime(ruta_SR['Fecha_ruta'], unit='s').dt.strftime('%Y-%m-%d')


# Define tu proyecto de BigQuery
# project_id = "bi-local-pe"

# # Define el nombre del conjunto de datos y de la tabla en BigQuery
# dataset_id = "Devolucion"
# table_id = "Seguimiento_Recoleccion_Devolucion"

# # Carga el DataFrame a BigQuery
# ruta_SR.to_gbq(destination_table=f"{dataset_id}.{table_id}",
#                    project_id=project_id,
#                    if_exists="replace")  # Opciones: "append", "replace", "fail"

# print("Los datos se han cargado exitosamente en BigQuery.")
################################################################################################################################################
#CONSOLIDADO STORE PICKUP (LIMA) Y NO SHOW (PROVINCIA)
################################################################################################################################################
### reporte DEVOLUCION Y NO SHOW
ruta_UM_CT = glob.glob(os.path.join(ruta_consolidado_Provincia,('Consolidado No Show' +  '*.xlsx')))
ruta_UM_CT_ = pd.DataFrame()
ruta_UM_CT_ = []  # Initialize an empty list instead of a DataFrame
x = pd.DataFrame()
for i in range(len(ruta_UM_CT)):
    x = pd.read_excel(ruta_UM_CT[i], 'CONSOLIDADO', dtype={"ORDER_NUMBER": str,"RASTREO": str})
    ruta_UM_CT_.append(x)
ruta_UM_CT_ = pd.concat(ruta_UM_CT_, ignore_index=True)

ruta_PICKUP = glob.glob(os.path.join(ruta_Lista_STORE_PICKUP, ('Consolidado STORE PICK-UP'  +  '*.xlsx')))
ruta_PICKUP_ = []  # Initialize an empty list instead of a DataFrame

for i in range(len(ruta_PICKUP)):
    x = pd.read_excel(ruta_PICKUP[i], 'CONSOLIDADO TOTAL', dtype={"num_rastreo": str,'Lpn_compra': str},header=0)
    ruta_PICKUP_.append(x)  # Append each DataFrame to the list

ruta_PICKUP_ = pd.concat(ruta_PICKUP_, ignore_index=True)

# Quitar inconsistencias en nombres de columnas
ruta_PICKUP = ruta_PICKUP_.rename(columns=lambda x: x.strip()).copy() 
ruta_UM_CT = ruta_UM_CT_.rename(columns=lambda x: x.strip()).copy()

ruta_UM_CT['SUBPROCESO'] = ruta_UM_CT['PROVEEDOR']
ruta_UM_CT['PLACA_SR'] = ruta_UM_CT['PROVEEDOR'] 
ruta_UM_CT['VEHICULO'] = ruta_UM_CT['PROVEEDOR']
ruta_UM_CT['LPN'] = ruta_UM_CT['RASTREO'].fillna('').astype(str)
ruta_UM_CT['status'] = ''
ruta_UM_CT['date_status'] = ''
ruta_UM_CT['Fecha de Planifiación'] = ''
ruta_UM_CT['UBICACION_TIENDA'] = 'PROVINCIA'
ruta_UM_CT = ruta_UM_CT[[ 'CT','SUBPROCESO','RLO_ID','RASTREO','LPN','ORDER_NUMBER','SKU','NOMBRE_PRODUCTO',
                   'status','date_status','ORIGEN','ID_TIENDA','TIENDA_ORIGEN','DEPARTAMENTO_TIENDA',
                   'PROVINCIA_TIENDA','DIRECCION_TIENDA','DISTRITO_TIENDA','BU','FLUJO','VEHICULO',
                   'TIPO DE FLOTA','PLACA_SR','Fecha de Planifiación','FECHA PICKUP','UBICACION_TIENDA']] #,'VENTANA HORARIA'
# Cambiar el nombre de la columna
ruta_UM_CT = ruta_UM_CT.rename(columns={'CT': 'PROCESO','status':'STATUS','date_status':'DATE_STATUS',
                                        'TIPO DE FLOTA':'TIPO_FLOTA', 
                                        'Fecha de Planifiación':'FECHA_PLANIFI','FECHA PICKUP':'FECHA_RUTA'}) 

ruta_UM_CT = ruta_UM_CT[[ 'PROCESO','SUBPROCESO','RLO_ID','RASTREO','LPN','ORDER_NUMBER','SKU','NOMBRE_PRODUCTO',
                   'STATUS','DATE_STATUS','ORIGEN','ID_TIENDA','TIENDA_ORIGEN','DEPARTAMENTO_TIENDA',
                   'PROVINCIA_TIENDA','DIRECCION_TIENDA','DISTRITO_TIENDA','BU','FLUJO','VEHICULO',
                   'TIPO_FLOTA','PLACA_SR','FECHA_PLANIFI','FECHA_RUTA','UBICACION_TIENDA']]

ruta_PICKUP['ORDER_NUMBER'] = ''
ruta_PICKUP['DEPARTAMENTO_TIENDA'] = '' 
ruta_PICKUP['PROVINCIA_TIENDA'] = ''
ruta_PICKUP['UBICACION_TIENDA'] = 'LIMA'
ruta_PICKUP = ruta_PICKUP[['Proceso','Sub Proceso','numero_order','num_rastreo','Lpn_compra','ORDER_NUMBER','sku_simple',
                          'sku_name','status','date_status','item_shipment_method','ID Tienda','Nombre_Tienda','DEPARTAMENTO_TIENDA',
                          'PROVINCIA_TIENDA','Dirección','Distrito','BU','Tipo Devolución','Vehiculo','Tipo Vehiculo','Placa SR',
                          'Fecha de Planifiación','Fecha de ruta','UBICACION_TIENDA']]#,'Horario Especial'

# Cambiar el nombre de la columna
ruta_PICKUP = ruta_PICKUP.rename(columns={'Proceso': 'PROCESO','Sub Proceso':'SUBPROCESO','numero_order':'RLO_ID','num_rastreo':'RASTREO',
                                        'Lpn_compra':'LPN', 'sku_simple':'SKU', 'sku_name':'NOMBRE_PRODUCTO', 'status':'STATUS',
                                        'date_status':'DATE_STATUS', 'item_shipment_method':'ORIGEN', 'ID Tienda':'ID_TIENDA',
                                        'Nombre_Tienda':'TIENDA_ORIGEN', 'Dirección':'DIRECCION_TIENDA','Distrito':'DISTRITO_TIENDA',
                                        'Tipo Devolución':'FLUJO','Vehiculo':'VEHICULO','Tipo Vehiculo':'TIPO_FLOTA','Placa SR':'PLACA_SR',
                                        'Fecha de Planifiación':'FECHA_PLANIFI','Fecha de ruta':'FECHA_RUTA'})

ruta_PICKUP = ruta_PICKUP[[ 'PROCESO','SUBPROCESO','RLO_ID','RASTREO','LPN','ORDER_NUMBER','SKU','NOMBRE_PRODUCTO',
                   'STATUS','DATE_STATUS','ORIGEN','ID_TIENDA','TIENDA_ORIGEN','DEPARTAMENTO_TIENDA',
                   'PROVINCIA_TIENDA','DIRECCION_TIENDA','DISTRITO_TIENDA','BU','FLUJO','VEHICULO',
                   'TIPO_FLOTA','PLACA_SR','FECHA_PLANIFI','FECHA_RUTA','UBICACION_TIENDA']]
# Unir los dos DataFrames
consolidado_final = pd.concat([ruta_UM_CT, ruta_PICKUP], ignore_index=True)

# Convertir la columna "numero_order" a tipo de datos str
consolidado_final["RLO_ID"] = consolidado_final["RLO_ID"].astype(str)
consolidado_final['ORDER_NUMBER'] = consolidado_final['ORDER_NUMBER'].astype(str)
consolidado_final['SKU'] = consolidado_final['SKU'].astype(str)
consolidado_final['NOMBRE_PRODUCTO'] = consolidado_final['NOMBRE_PRODUCTO'].astype(str)
consolidado_final['ORIGEN'] = consolidado_final['ORIGEN'].astype(str)
consolidado_final['STATUS'] = consolidado_final['STATUS'].astype(str)
consolidado_final['DATE_STATUS'] = consolidado_final['DATE_STATUS'].astype(str)
consolidado_final['ID_TIENDA'] = consolidado_final['ID_TIENDA'].astype(str)
consolidado_final['PLACA_SR'] = consolidado_final['PLACA_SR'].astype(str)
consolidado_final['LPN'] = consolidado_final['LPN'].fillna('').astype(str)
# Convert the column to string type
consolidado_final['TIPO_FLOTA'] = consolidado_final['TIPO_FLOTA'].astype(str)

# Assuming ruta_PICKUP is your DataFrame
consolidado_final['FECHA_PLANIFI'] = pd.to_datetime(consolidado_final['FECHA_PLANIFI']).dt.strftime('%Y-%m-%d')
consolidado_final['FECHA_RUTA'] = pd.to_datetime(consolidado_final['FECHA_RUTA']).dt.strftime('%Y-%m-%d')
consolidado_final['TIENDA_ORIGEN'] = consolidado_final['TIENDA_ORIGEN'].str.upper()


# Agregar columna 'BU_ORIGEN'
consolidado_final['BU_ORIGEN'] = np.where(consolidado_final['TIENDA_ORIGEN'].str.contains('FALABELLA', na=False), 'FALABELLA', 
                                          np.where(consolidado_final['TIENDA_ORIGEN'].str.contains('SODIMAC', na=False),'SODIMAC',
                                                    np.where(consolidado_final['TIENDA_ORIGEN'].str.contains('MAESTRO', na=False),'SODIMAC',
                                                             np.where(consolidado_final['TIENDA_ORIGEN'].str.contains('TOTTUS', na=False),'TOTTUS',
                                                                      np.where(consolidado_final['TIENDA_ORIGEN'].str.contains('FALABELLA.COM', na=False),'FALABELLA.COM',
                                                                               'OTROS')))))

# Define tu proyecto de BigQuery
project_id = "bi-local-pe"

# Define el nombre del conjunto de datos y de la tabla en BigQuery
dataset_id = "Devolucion"
table_id = "CONSOLIDADO_IL"

# Carga el DataFrame a BigQuery
consolidado_final.to_gbq(destination_table=f"{dataset_id}.{table_id}",
                   project_id=project_id,
                   if_exists="replace")  # Opciones: "append", "replace", "fail"

print("Los datos se han cargado exitosamente en BigQuery.")
