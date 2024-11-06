# Proyecto: CONSOLIDADO DE STORE PICKUP - LIMA

## Descripción
Este proyecto contiene un script de Python para consolidar datos de recolección y transporte en Lima, Perú. El código integra múltiples fuentes de datos, aplica transformaciones y sube el resultado a BigQuery.

## Estructura del Proyecto
- `ruta_base`: Rutas base de directorios para leer y consolidar archivos Excel de diferentes fuentes.
- `ruta_PICKUP`: Consolidado de datos de `STORE PICK-UP`.
- `ruta_PLAN`: Consolidado de datos de planificación.

## Requerimientos
Para ejecutar este proyecto, asegúrate de tener instaladas las siguientes librerías de Python:
- `pandas`
- `numpy`
- `google-cloud-bigquery`
- `google-cloud-storage`
- `fpdf`
- `xlsxwriter`
- `google-auth-oauthlib`
- `google-auth`
  
Instala las dependencias con:
`bash
pip install pandas numpy google-cloud-bigquery google-cloud-storage fpdf xlsxwriter google-auth-oauthlib google-auth`


# Proyecto: CONSOLIDADO PERSONAL DE TRANSPORTE

Este proyecto contiene un script de Python que consolida datos de recolección diaria de personal de transporte en Lima. Los datos se extraen de Google Sheets, se transforman, y luego se cargan en BigQuery.

## Estructura del Proyecto

- **Planificación UM CTs**: Ruta base de planificación.
- **Devolución**: Ruta base de devolución.
- **Datos_Personal_Recojo**: Hoja de Google Sheets con la información de transporte personal diario.

## Requerimientos

Para ejecutar este script, asegúrate de tener instaladas las siguientes librerías de Python:

- `pandas`
- `numpy`
- `google-cloud-bigquery`
- `google-auth-oauthlib`
- `google-auth`
- `fpdf`
- `xlsxwriter`
- `xlsx2csv`

Instala las dependencias con:

```bash
pip install pandas numpy google-cloud-bigquery google-auth-oauthlib google-auth fpdf xlsxwriter xlsx2csv
