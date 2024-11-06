# ETL_GCP

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
```bash
pip install pandas numpy google-cloud-bigquery google-cloud-storage fpdf xlsxwriter google-auth-oauthlib google-auth
