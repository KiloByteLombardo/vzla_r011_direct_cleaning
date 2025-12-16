import os
import io
import pandas as pd
from google.cloud import bigquery, storage
import gspread
from typing import Tuple


def process_excel_file(file_content: bytes, filename: str) -> bytes:
    """
    Procesa un archivo Excel: lo carga en DataFrame, procesa y devuelve como Excel.
    
    Args:
        file_content: Contenido del archivo Excel en bytes
        filename: Nombre del archivo original
        
    Returns:
        bytes: Contenido del archivo Excel procesado
    """
    try:
        # Leer el archivo Excel en un DataFrame
        print(f"Reading Excel file: {filename}")
        df = pd.read_excel(io.BytesIO(file_content))
        
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Procesar el DataFrame
        df_processed = process_dataframe(df)
        
        # Convertir el DataFrame procesado de vuelta a Excel
        output = io.BytesIO()
        df_processed.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        
        print("Excel file processed successfully")
        return output.getvalue()
        
    except Exception as e:
        print(f"Error processing Excel file: {str(e)}")
        raise


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa el DataFrame según la lógica de negocio.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame procesado
    """
    # Crear una copia para no modificar el original
    df_processed = df.copy()
    
    # Ejemplo de procesamiento: eliminar filas completamente vacías
    df_processed = df_processed.dropna(how='all')
    
    # Aquí puedes agregar más lógica de procesamiento específica
    # Por ejemplo:
    # - Limpieza de datos
    # - Transformaciones de columnas
    # - Filtros
    # - Agregaciones
    # etc.
    
    return df_processed


def upload_to_bigquery(df: pd.DataFrame, credentials, project_id: str, 
                       dataset_id: str, table_id: str, 
                       write_disposition: str = 'WRITE_TRUNCATE') -> bool:
    """
    Sube un DataFrame a BigQuery.
    
    Args:
        df: DataFrame a subir
        credentials: Credenciales de GCP
        project_id: ID del proyecto de GCP
        dataset_id: ID del dataset en BigQuery
        table_id: ID de la tabla en BigQuery
        write_disposition: Modo de escritura ('WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY')
        
    Returns:
        bool: True si fue exitoso, False en caso contrario
    """
    try:
        bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True
        )
        
        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Esperar a que termine el job
        
        print(f"DataFrame uploaded to BigQuery: {dataset_id}.{table_id}")
        return True
        
    except Exception as e:
        print(f"Error uploading to BigQuery: {str(e)}")
        return False


def upload_to_storage(file_content: bytes, credentials, project_id: str,
                     bucket_name: str, blob_name: str) -> bool:
    """
    Sube un archivo a Cloud Storage.
    
    Args:
        file_content: Contenido del archivo en bytes
        credentials: Credenciales de GCP
        project_id: ID del proyecto de GCP
        bucket_name: Nombre del bucket
        blob_name: Nombre del blob (archivo) en el bucket
        
    Returns:
        bool: True si fue exitoso, False en caso contrario
    """
    try:
        storage_client = storage.Client(credentials=credentials, project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            file_content, 
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        print(f"File uploaded to Cloud Storage: gs://{bucket_name}/{blob_name}")
        return True
        
    except Exception as e:
        print(f"Error uploading to Cloud Storage: {str(e)}")
        return False


def upload_to_sheets(df: pd.DataFrame, credentials, spreadsheet_id: str, 
                    worksheet_name: str = 'Sheet1', clear: bool = True) -> bool:
    """
    Sube un DataFrame a Google Sheets.
    
    Args:
        df: DataFrame a subir
        credentials: Credenciales de GCP
        spreadsheet_id: ID de la hoja de cálculo
        worksheet_name: Nombre de la hoja de trabajo
        clear: Si True, limpia la hoja antes de escribir
        
    Returns:
        bool: True si fue exitoso, False en caso contrario
    """
    try:
        gspread_client = gspread.authorize(credentials)
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
        
        if clear:
            worksheet.clear()
        
        # Actualizar la hoja con los datos del DataFrame
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        
        print(f"DataFrame uploaded to Google Sheets: {spreadsheet_id}/{worksheet_name}")
        return True
        
    except Exception as e:
        print(f"Error uploading to Google Sheets: {str(e)}")
        return False
