import os
import sys
import io
import pandas as pd
from google.cloud import bigquery, storage
from google.auth import default, load_credentials_from_file
from google.oauth2 import service_account
import gspread
from typing import Tuple, Optional
from datetime import datetime, date


def load_env_file(env_path: str = '.env'):
    """
    Carga variables de entorno desde un archivo .env de forma simple.
    
    Args:
        env_path: Ruta al archivo .env (default: '.env')
    """
    if not os.path.exists(env_path):
        return
    
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Ignorar comentarios y líneas vacías
                if not line or line.startswith('#'):
                    continue
                
                # Separar clave y valor
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Remover comillas si las tiene
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # Solo establecer si no existe ya en el entorno
                    if key and not os.getenv(key):
                        os.environ[key] = value
    except Exception as e:
        print(f"[VENZUELA] Warning: Could not load .env file: {str(e)}")
        sys.stdout.flush()


def get_provider_mapping(credentials, spreadsheet_id: str, worksheet_name: str = 'Maestro RMS') -> dict:
    """
    Lee un Google Sheets y crea un diccionario de pareo entre NOMBRE PROVEEDOR y UNIDAD DE NEGOCIO.
    
    Args:
        credentials: Credenciales de GCP
        spreadsheet_id: ID del Google Sheets
        worksheet_name: Nombre de la hoja de trabajo 
        
    Returns:
        dict: Diccionario con NOMBRE PROVEEDOR como clave y UNIDAD DE NEGOCIO como valor
    """
    try:
        print(f"[VENZUELA] Reading provider mapping from Google Sheets: {spreadsheet_id}/{worksheet_name}")
        sys.stdout.flush()
        
        # Asegurar que las credenciales tengan los scopes necesarios para Google Sheets
        # Si las credenciales son de tipo service_account, agregar los scopes necesarios
        if hasattr(credentials, 'with_scopes'):
            # Si las credenciales soportan with_scopes, agregar el scope de Google Sheets
            sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            credentials_with_scope = credentials.with_scopes(sheets_scopes)
        elif isinstance(credentials, service_account.Credentials):
            # Si ya es service_account pero no tiene scopes, crear uno nuevo con scopes
            sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            credentials_with_scope = credentials.with_scopes(sheets_scopes)
        else:
            # Si no se puede modificar, usar las credenciales tal cual
            credentials_with_scope = credentials
        
        gspread_client = gspread.authorize(credentials_with_scope)
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        # Obtener todos los valores de la hoja
        all_values = worksheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            print(f"[VENZUELA] Warning: Google Sheets is empty or has no data rows")
            sys.stdout.flush()
            return {}
        
        # La primera fila son los encabezados
        headers = [h.strip().upper() for h in all_values[0]]
        
        # Buscar los índices de las columnas
        if 'NOMBRE PROVEEDOR' not in headers:
            print(f"[VENZUELA] Error: Column 'NOMBRE PROVEEDOR' not found in Google Sheets. Headers: {headers}")
            sys.stdout.flush()
            return {}
        
        if 'UNIDAD DE NEGOCIO' not in headers:
            print(f"[VENZUELA] Error: Column 'UNIDAD DE NEGOCIO' not found in Google Sheets. Headers: {headers}")
            sys.stdout.flush()
            return {}
        
        provider_idx = headers.index('NOMBRE PROVEEDOR')
        unidad_idx = headers.index('UNIDAD DE NEGOCIO')
        
        # Crear el diccionario de pareo (normalizar eliminando todos los espacios)
        mapping = {}
        for row in all_values[1:]:  # Saltar la fila de encabezados
            if len(row) > max(provider_idx, unidad_idx):
                provider = str(row[provider_idx]).strip() if row[provider_idx] else ''
                unidad = str(row[unidad_idx]).strip() if row[unidad_idx] else ''
                if provider:  # Solo agregar si hay un nombre de proveedor
                    # Normalizar el proveedor eliminando todos los espacios para el pareo
                    provider_normalized = provider.replace(' ', '').replace('\t', '').replace('\n', '')
                    # Guardar tanto la versión normalizada como la original para referencia
                    mapping[provider_normalized] = unidad
        
        print(f"[VENZUELA] Loaded {len(mapping)} provider mappings from Google Sheets")
        sys.stdout.flush()
        return mapping
        
    except Exception as e:
        print(f"[VENZUELA] Error reading provider mapping from Google Sheets: {str(e)}")
        sys.stdout.flush()
        return {}


def detect_headers(file_content: bytes, expected_headers: list = None) -> Optional[int]:
    """
    Detecta automáticamente la fila donde empiezan los cabezales del archivo Excel.
    
    Args:
        file_content: Contenido del archivo Excel en bytes
        expected_headers: Lista de nombres de columnas esperados. Si es None, usa los del archivo R011.
        
    Returns:
        int: Número de fila donde empiezan los cabezales (0-indexed), o None si no se encuentra
    """
    if expected_headers is None:
        # Cabezales esperados para el archivo R011
        expected_headers = [
            'Fecha Recepción', 'Centro de Costo', 'Tienda', 'Proveedor', 'Sucursal',
            'Número Factura', 'Tipo Documento', 'Estado', 'Orden Compra', 'Fecha Factura',
            'SubTotal', 'Valor Impuesto', 'Total con Impuesto', 'Costo Recepcion',
            'Diferencia', 'Unidades por Factura', 'Unidades Recibidas', 'Diferencias',
            'Factura Con Faltante', 'Término de Pago', 'Fecha Vencimiento', 'Indicador RTV',
            'OrdenRTV', 'Consignación', 'Origen Documento', 'Razón REIM', 'Fecha Creación',
            'Fecha Modificación', 'Fecha Aprobación', 'Fecha Publicación', 'Creado Por', 'Modificado Por'
        ]
    
    try:
        # Leer el archivo sin especificar header para buscar manualmente
        print(f"[VENZUELA] Detecting headers...")
        sys.stdout.flush()
        
        # Leer las primeras filas del archivo
        df_temp = pd.read_excel(io.BytesIO(file_content), header=None, nrows=20)
        
        # Normalizar los headers esperados (eliminar espacios, convertir a string)
        expected_normalized = [str(h).strip().lower() for h in expected_headers]
        
        # Buscar en cada fila si contiene los headers esperados
        best_match_row = None
        best_match_count = 0
        
        for row_idx in range(min(20, len(df_temp))):
            # Obtener los valores de la fila como strings
            row_values = [str(val).strip().lower() if pd.notna(val) else '' for val in df_temp.iloc[row_idx].values]
            
            # Contar cuántos headers esperados se encuentran en esta fila
            match_count = 0
            for expected in expected_normalized:
                if expected in row_values:
                    match_count += 1
            
            # Si encontramos al menos 5 headers coincidentes, consideramos que es la fila de headers
            if match_count >= 5 and match_count > best_match_count:
                best_match_count = match_count
                best_match_row = row_idx
        
        if best_match_row is not None:
            print(f"[VENZUELA] Headers detected at row {best_match_row + 1} (matched {best_match_count} out of {len(expected_headers)} expected columns)")
            sys.stdout.flush()
            return best_match_row
        else:
            print(f"[VENZUELA] Warning: Could not detect headers automatically. Using first row as header.")
            sys.stdout.flush()
            return None
            
    except Exception as e:
        print(f"[VENZUELA] Error detecting headers: {str(e)}. Using first row as header.")
        sys.stdout.flush()
        return None


def process_excel_file(file_content: bytes, filename: str, credentials=None) -> bytes:
    """
    Procesa un archivo Excel: lo carga en DataFrame, procesa y devuelve como Excel.
    Detecta automáticamente los cabezales del archivo.
    
    Args:
        file_content: Contenido del archivo Excel en bytes
        filename: Nombre del archivo original
        credentials: Credenciales de GCP (opcional, necesario para pareo con Google Sheets)
        
    Returns:
        bytes: Contenido del archivo Excel procesado
    """
    try:
        # Detectar automáticamente dónde empiezan los cabezales
        header_row = detect_headers(file_content)
        
        # Leer el archivo Excel en un DataFrame
        print(f"[VENZUELA] Reading Excel file: {filename}")
        sys.stdout.flush()
        
        if header_row is not None:
            # Leer el archivo usando la fila detectada como header
            df = pd.read_excel(io.BytesIO(file_content), header=header_row)
            print(f"[VENZUELA] Using row {header_row + 1} as headers")
        else:
            # Leer el archivo usando la primera fila como header (comportamiento por defecto)
            df = pd.read_excel(io.BytesIO(file_content))
            print(f"[VENZUELA] Using first row as headers")
        
        sys.stdout.flush()
        
        # Limpiar nombres de columnas (eliminar espacios extra, etc.)
        df.columns = [str(col).strip() for col in df.columns]
        
        print(f"[VENZUELA] DataFrame shape: {df.shape}")
        print(f"[VENZUELA] Columns: {list(df.columns)}")
        sys.stdout.flush()
        
        # Procesar el DataFrame
        print(f"[VENZUELA] Processing dataframe...")
        sys.stdout.flush()
        df_processed = process_dataframe(df, credentials)
        
        # Convertir el DataFrame procesado de vuelta a Excel
        print(f"[VENZUELA] Converting to Excel...")
        sys.stdout.flush()
        output = io.BytesIO()
        df_processed.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        
        print(f"[VENZUELA] Excel file processed successfully. Output shape: {df_processed.shape}")
        sys.stdout.flush()
        return output.getvalue()
        
    except Exception as e:
        print(f"[VENZUELA] Error processing Excel file: {str(e)}")
        sys.stdout.flush()
        raise


def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas completamente vacías del DataFrame.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame sin filas vacías
    """
    initial_rows = len(df)
    df_processed = df.dropna(how='all')
    removed = initial_rows - len(df_processed)
    
    print(f"[VENZUELA] Removed {removed} empty rows (from {initial_rows} to {len(df_processed)})")
    sys.stdout.flush()
    
    return df_processed


def remove_ndint_invoices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas donde "Número Factura" tenga el prefijo "NDINT".
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame sin filas con prefijo NDINT
    """
    if 'Número Factura' not in df.columns:
        print(f"[VENZUELA] Warning: Column 'Número Factura' not found. Available columns: {list(df.columns)}")
        sys.stdout.flush()
        return df
    
    initial_rows = len(df)
    
    # Convertir la columna a string para poder hacer el filtro, manejando NaN
    df_processed = df.copy()
    df_processed['Número Factura'] = df_processed['Número Factura'].astype(str)
    
    # Contar cuántas filas tienen el prefijo NDINT antes de eliminarlas
    rows_with_ndint = df_processed['Número Factura'].str.startswith('NDINT', na=False).sum()
    
    # Filtrar: mantener solo las filas que NO empiezan con NDINT
    df_processed = df_processed[~df_processed['Número Factura'].str.startswith('NDINT', na=False)]
    
    removed = initial_rows - len(df_processed)
    print(f"[VENZUELA] Removed {removed} rows with NDINT prefix (from {initial_rows} to {len(df_processed)})")
    sys.stdout.flush()
    
    return df_processed


def add_unidad_negocio_column(df: pd.DataFrame, credentials=None) -> pd.DataFrame:
    """
    Crea la columna "Unidad de Negocio" haciendo pareo con Google Sheets.
    Hace pareo entre la columna "Sucursal" y "NOMBRE PROVEEDOR" del Google Sheets.
    
    Args:
        df: DataFrame original
        credentials: Credenciales de GCP (opcional, necesario para pareo con Google Sheets)
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Unidad de Negocio" agregada
    """
    df_processed = df.copy()
    
    # Verificar que exista la columna Sucursal
    if 'Sucursal' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Sucursal' not found. Cannot create 'Unidad de Negocio' column")
        sys.stdout.flush()
        df_processed['Unidad de Negocio'] = ''
        return df_processed
    
    # Verificar que se proporcionen credenciales
    if not credentials:
        print(f"[VENZUELA] Warning: No credentials provided. Cannot create 'Unidad de Negocio' column")
        sys.stdout.flush()
        df_processed['Unidad de Negocio'] = ''
        return df_processed
    
    # Cargar variables de entorno desde .env si existe
    load_env_file()
    
    # Obtener el ID del Google Sheets desde variables de entorno
    spreadsheet_id = os.getenv('SHEETS_PROVIDER_MAPPING_ID')
    if not spreadsheet_id:
        print(f"[VENZUELA] Warning: SHEETS_PROVIDER_MAPPING_ID not found in environment variables")
        print(f"[VENZUELA] Debug: Current working directory: {os.getcwd()}")
        print(f"[VENZUELA] Debug: .env file exists: {os.path.exists('.env')}")
        # Mostrar todas las variables que empiezan con SHEETS para debug
        sheets_vars = {k: v for k, v in os.environ.items() if 'SHEETS' in k.upper()}
        if sheets_vars:
            print(f"[VENZUELA] Debug: Found SHEETS variables: {list(sheets_vars.keys())}")
        sys.stdout.flush()
        df_processed['Unidad de Negocio'] = ''
        return df_processed
    
    print(f"[VENZUELA] Creating 'Unidad de Negocio' column using Google Sheets mapping...")
    sys.stdout.flush()
    
    # Obtener el mapeo de proveedores desde Google Sheets
    provider_mapping = get_provider_mapping(credentials, spreadsheet_id)
    
    if not provider_mapping:
        print(f"[VENZUELA] Warning: Could not load provider mapping from Google Sheets")
        sys.stdout.flush()
        df_processed['Unidad de Negocio'] = ''
        return df_processed
    
    # Inicializar la nueva columna con valores vacíos
    df_processed['Unidad de Negocio'] = ''
    
    # Convertir Sucursal a string para hacer el pareo
    df_processed['Sucursal'] = df_processed['Sucursal'].astype(str)
    
    # Hacer el pareo: buscar cada valor de Sucursal en el diccionario
    # Normalizar eliminando todos los espacios para hacer el pareo más efectivo
    matched_count = 0
    for idx, sucursal in df_processed['Sucursal'].items():
        # Normalizar el valor de Sucursal eliminando todos los espacios (incluyendo tabs y newlines)
        sucursal_normalized = str(sucursal).strip().replace(' ', '').replace('\t', '').replace('\n', '')
        # Buscar coincidencia con la versión normalizada
        if sucursal_normalized in provider_mapping:
            df_processed.at[idx, 'Unidad de Negocio'] = provider_mapping[sucursal_normalized]
            matched_count += 1
    
    print(f"[VENZUELA] Matched {matched_count} out of {len(df_processed)} rows with provider mapping")
    if matched_count < len(df_processed):
        unmatched = len(df_processed) - matched_count
        print(f"[VENZUELA] Warning: {unmatched} rows could not be matched with provider mapping")
    sys.stdout.flush()
    
    return df_processed


def add_tipo_proveedor_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Tipo de Proveedor" basándose en las columnas "Sucursal" y "Tienda".
    
    Lógica:
    1. Si "Sucursal" termina con "PPV", "PPV2" o "PPV3", poner "PPV"
    2. Si "Tienda" dice "CENDIS", poner "CENDIS"
    3. Lo demás, poner "Directo"
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Tipo de Proveedor" agregada
    """
    df_processed = df.copy()
    
    # Inicializar la nueva columna con "Directo" por defecto
    df_processed['Tipo de Proveedor'] = 'Directo'
    
    # Verificar que existan las columnas necesarias
    has_sucursal = 'Sucursal' in df_processed.columns
    has_tienda = 'Tienda' in df_processed.columns
    
    if not has_sucursal and not has_tienda:
        print(f"[VENZUELA] Warning: Columns 'Sucursal' and 'Tienda' not found. Cannot create 'Tipo de Proveedor' column")
        sys.stdout.flush()
        return df_processed
    
    # Convertir columnas a string para procesamiento
    if has_sucursal:
        df_processed['Sucursal'] = df_processed['Sucursal'].astype(str)
    if has_tienda:
        df_processed['Tienda'] = df_processed['Tienda'].astype(str)
    
    # Contadores para estadísticas
    ppv_count = 0
    cendis_count = 0
    directo_count = 0
    
    # Aplicar la lógica fila por fila
    for idx in df_processed.index:
        tipo_proveedor = 'Directo'  # Valor por defecto
        
        # 1. Primero verificar si Sucursal termina con "PPV", "PPV2" o "PPV3" (case-insensitive)
        # Si es PPV/PPV2/PPV3, no se verifica Tienda
        if has_sucursal:
            sucursal = str(df_processed.at[idx, 'Sucursal']).strip().upper()
            # Verificar si termina con PPV, PPV2 o PPV3
            if sucursal.endswith('PPV') or sucursal.endswith('PPV2') or sucursal.endswith('PPV3'):
                tipo_proveedor = 'PPV'
                ppv_count += 1
            # Si no es PPV/PPV2/PPV3, verificar Tienda
            else:
                if has_tienda:
                    tienda = str(df_processed.at[idx, 'Tienda']).strip().upper()
                    if tienda == 'CENDIS':
                        tipo_proveedor = 'CENDIS'
                        cendis_count += 1
                    else:
                        directo_count += 1
                else:
                    directo_count += 1
        # Si no hay Sucursal, solo verificar Tienda
        elif has_tienda:
            tienda = str(df_processed.at[idx, 'Tienda']).strip().upper()
            if tienda == 'CENDIS':
                tipo_proveedor = 'CENDIS'
                cendis_count += 1
            else:
                directo_count += 1
        else:
            directo_count += 1
        
        df_processed.at[idx, 'Tipo de Proveedor'] = tipo_proveedor
    
    print(f"[VENZUELA] Tipo de Proveedor column created: PPV={ppv_count}, CENDIS={cendis_count}, Directo={directo_count}")
    sys.stdout.flush()
    
    return df_processed


def add_motivo_retencion_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Motivo de Retención" basándose en las columnas "Estado" y "Tipo de Proveedor".
    
    Lógica inicial:
    1. Si "Estado" es "DISCREPANCIA DE IMPUESTO", poner "Discrepancia en Impuesto"
    2. Si "Tipo de Proveedor" es "PPV", poner "Discrepancia en Costos"
    3. Lo demás, dejar vacío (se rellenará después)
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Motivo de Retención" agregada
    """
    df_processed = df.copy()
    
    # Inicializar la nueva columna vacía
    df_processed['Motivo de Retención'] = ''
    
    # Verificar que existan las columnas necesarias
    has_estado = 'Estado' in df_processed.columns
    has_tipo_proveedor = 'Tipo de Proveedor' in df_processed.columns
    
    if not has_estado and not has_tipo_proveedor:
        print(f"[VENZUELA] Warning: Columns 'Estado' and 'Tipo de Proveedor' not found. Cannot create 'Motivo de Retención' column")
        sys.stdout.flush()
        return df_processed
    
    # Convertir columnas a string para procesamiento
    if has_estado:
        df_processed['Estado'] = df_processed['Estado'].astype(str)
    if has_tipo_proveedor:
        df_processed['Tipo de Proveedor'] = df_processed['Tipo de Proveedor'].astype(str)
    
    # Contadores para estadísticas
    discrepancia_impuesto_count = 0
    discrepancia_costos_count = 0
    empty_count = 0
    
    # Aplicar la lógica fila por fila
    for idx in df_processed.index:
        motivo_retencion = ''  # Valor por defecto (vacío)
        
        # 1. Primero verificar si Estado es "DISCREPANCIA DE IMPUESTO"
        if has_estado:
            estado = str(df_processed.at[idx, 'Estado']).strip()
            if estado == 'DISCREPANCIA DE IMPUESTO':
                motivo_retencion = 'Discrepancia en Impuesto'
                discrepancia_impuesto_count += 1
            # Si no es Discrepancia en Impuesto, verificar Tipo de Proveedor
            elif has_tipo_proveedor:
                tipo_proveedor = str(df_processed.at[idx, 'Tipo de Proveedor']).strip()
                if tipo_proveedor == 'PPV':
                    motivo_retencion = 'Discrepancia en Costos'
                    discrepancia_costos_count += 1
                else:
                    empty_count += 1
            else:
                empty_count += 1
        # Si no hay Estado, solo verificar Tipo de Proveedor
        elif has_tipo_proveedor:
            tipo_proveedor = str(df_processed.at[idx, 'Tipo de Proveedor']).strip()
            if tipo_proveedor == 'PPV':
                motivo_retencion = 'Discrepancia en Costos'
                discrepancia_costos_count += 1
            else:
                empty_count += 1
        else:
            empty_count += 1
        
        df_processed.at[idx, 'Motivo de Retención'] = motivo_retencion
    
    print(f"[VENZUELA] Motivo de Retención column created: Discrepancia en Impuesto={discrepancia_impuesto_count}, Discrepancia en Costos={discrepancia_costos_count}, Empty={empty_count}")
    sys.stdout.flush()
    
    return df_processed


def add_validacion_oc_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Validacion de OC" contando cuántas veces aparece cada orden de compra.
    Equivalente a CONTAR.SI en Excel.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Validacion de OC" agregada
    """
    df_processed = df.copy()
    
    # Verificar que exista la columna Orden Compra
    if 'Orden Compra' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Orden Compra' not found. Cannot create 'Validacion de OC' column")
        sys.stdout.flush()
        df_processed['Validacion de OC'] = 0
        return df_processed
    
    # Convertir a string para procesamiento
    df_processed['Orden Compra'] = df_processed['Orden Compra'].astype(str)
    
    # Contar cuántas veces aparece cada orden de compra
    # Esto es equivalente a CONTAR.SI(I:I;I2) en Excel
    orden_compra_counts = df_processed['Orden Compra'].value_counts().to_dict()
    
    # Crear la columna Validacion de OC con el conteo para cada fila
    df_processed['Validacion de OC'] = df_processed['Orden Compra'].map(orden_compra_counts)
    
    # Convertir a int para que sea consistente
    df_processed['Validacion de OC'] = df_processed['Validacion de OC'].astype(int)
    
    print(f"[VENZUELA] Validacion de OC column created. Unique orders: {len(orden_compra_counts)}, Total rows: {len(df_processed)}")
    sys.stdout.flush()
    
    return df_processed


def add_diferencia_real_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Diferencia Real" basándose en la columna "Validacion de OC".
    
    Lógica:
    - Si "Validacion de OC" > 1, poner "No Aplica"
    - Si "Validacion de OC" <= 1, poner "Revisar"
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Diferencia Real" agregada
    """
    df_processed = df.copy()
    
    # Verificar que exista la columna Validacion de OC
    if 'Validacion de OC' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Validacion de OC' not found. Cannot create 'Diferencia Real' column")
        sys.stdout.flush()
        df_processed['Diferencia Real'] = 'Revisar'
        return df_processed
    
    # Convertir a numérico para comparación
    df_processed['Validacion de OC'] = pd.to_numeric(df_processed['Validacion de OC'], errors='coerce').fillna(0)
    
    # Aplicar la lógica: si Validacion de OC > 1, "No Aplica", sino "Revisar"
    df_processed['Diferencia Real'] = df_processed['Validacion de OC'].apply(
        lambda x: 'No Aplica' if x <= 1 else 'Revisar'
    )
    
    # Contadores para estadísticas
    no_aplica_count = (df_processed['Diferencia Real'] == 'No Aplica').sum()
    revisar_count = (df_processed['Diferencia Real'] == 'Revisar').sum()
    
    print(f"[VENZUELA] Diferencia Real column created: No Aplica={no_aplica_count}, Revisar={revisar_count}")
    sys.stdout.flush()
    
    return df_processed


def add_valor_real_unidades_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Valor Real de Unidades" sumando las unidades por factura para cada orden de compra.
    Equivalente a SUMAR.SI en Excel.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Valor Real de Unidades" agregada
    """
    df_processed = df.copy()
    
    # Verificar que existan las columnas necesarias
    if 'Orden Compra' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Orden Compra' not found. Cannot create 'Valor Real de Unidades' column")
        sys.stdout.flush()
        df_processed['Valor Real de Unidades'] = 0
        return df_processed
    
    if 'Unidades por Factura' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Unidades por Factura' not found. Cannot create 'Valor Real de Unidades' column")
        sys.stdout.flush()
        df_processed['Valor Real de Unidades'] = 0
        return df_processed
    
    # Convertir a tipos apropiados
    df_processed['Orden Compra'] = df_processed['Orden Compra'].astype(str)
    # Convertir Unidades por Factura a numérico, manejando errores
    df_processed['Unidades por Factura'] = pd.to_numeric(df_processed['Unidades por Factura'], errors='coerce').fillna(0)
    
    # Crear un diccionario con la suma de unidades por cada orden de compra
    # Esto es equivalente a SUMAR.SI(I:I;I2;P:P) en Excel
    suma_por_orden = df_processed.groupby('Orden Compra')['Unidades por Factura'].sum().to_dict()
    
    # Crear la columna Valor Real de Unidades con la suma para cada fila
    df_processed['Valor Real de Unidades'] = df_processed['Orden Compra'].map(suma_por_orden)
    
    # Convertir a numérico
    df_processed['Valor Real de Unidades'] = pd.to_numeric(df_processed['Valor Real de Unidades'], errors='coerce').fillna(0)
    
    print(f"[VENZUELA] Valor Real de Unidades column created. Total sum calculated for {len(suma_por_orden)} unique orders")
    sys.stdout.flush()
    
    return df_processed


def add_diferencia_unidades_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Diferencia Unidades" restando "Valor Real de Unidades" menos "Unidades Recibidas".
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Diferencia Unidades" agregada
    """
    df_processed = df.copy()
    
    # Verificar que existan las columnas necesarias
    if 'Valor Real de Unidades' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Valor Real de Unidades' not found. Cannot create 'Diferencia Unidades' column")
        sys.stdout.flush()
        df_processed['Diferencia Unidades'] = 0
        return df_processed
    
    if 'Unidades Recibidas' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Unidades Recibidas' not found. Cannot create 'Diferencia Unidades' column")
        sys.stdout.flush()
        df_processed['Diferencia Unidades'] = 0
        return df_processed
    
    # Convertir a numérico, manejando errores
    df_processed['Valor Real de Unidades'] = pd.to_numeric(df_processed['Valor Real de Unidades'], errors='coerce').fillna(0)
    df_processed['Unidades Recibidas'] = pd.to_numeric(df_processed['Unidades Recibidas'], errors='coerce').fillna(0)
    
    # Calcular la diferencia: Valor Real de Unidades - Unidades Recibidas
    df_processed['Diferencia Unidades'] = df_processed['Valor Real de Unidades'] - df_processed['Unidades Recibidas']
    
    print(f"[VENZUELA] Diferencia Unidades column created")
    sys.stdout.flush()
    
    return df_processed


def add_valor_real_subtotal_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Valor Real de Subtotal" sumando los subtotales por orden de compra.
    Equivalente a SUMAR.SI(I:I;I2;K:K) en Excel, donde I es Orden Compra y K es SubTotal.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Valor Real de Subtotal" agregada
    """
    df_processed = df.copy()
    
    # Verificar que existan las columnas necesarias
    if 'Orden Compra' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Orden Compra' not found. Cannot create 'Valor Real de Subtotal' column")
        sys.stdout.flush()
        df_processed['Valor Real de Subtotal'] = 0
        return df_processed
    
    if 'SubTotal' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'SubTotal' not found. Cannot create 'Valor Real de Subtotal' column")
        sys.stdout.flush()
        df_processed['Valor Real de Subtotal'] = 0
        return df_processed
    
    # Convertir a tipos apropiados
    df_processed['Orden Compra'] = df_processed['Orden Compra'].astype(str)
    # Convertir SubTotal a numérico, manejando errores
    df_processed['SubTotal'] = pd.to_numeric(df_processed['SubTotal'], errors='coerce').fillna(0)
    
    # Crear un diccionario con la suma de subtotales por cada orden de compra
    # Esto es equivalente a SUMAR.SI(I:I;I2;K:K) en Excel
    suma_subtotal_por_orden = df_processed.groupby('Orden Compra')['SubTotal'].sum().to_dict()
    
    # Crear la columna Valor Real de Subtotal con la suma para cada fila
    df_processed['Valor Real de Subtotal'] = df_processed['Orden Compra'].map(suma_subtotal_por_orden)
    
    # Convertir a numérico
    df_processed['Valor Real de Subtotal'] = pd.to_numeric(df_processed['Valor Real de Subtotal'], errors='coerce').fillna(0)
    
    print(f"[VENZUELA] Valor Real de Subtotal column created. Total sum calculated for {len(suma_subtotal_por_orden)} unique orders")
    sys.stdout.flush()
    
    return df_processed


def add_diferencia_costo_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Diferencia Costo" restando "Valor Real de Subtotal" menos "Costo Recepcion".
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Diferencia Costo" agregada
    """
    df_processed = df.copy()
    
    # Verificar que existan las columnas necesarias
    if 'Valor Real de Subtotal' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Valor Real de Subtotal' not found. Cannot create 'Diferencia Costo' column")
        sys.stdout.flush()
        df_processed['Diferencia Costo'] = 0
        return df_processed
    
    if 'Costo Recepcion' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Costo Recepcion' not found. Cannot create 'Diferencia Costo' column")
        sys.stdout.flush()
        df_processed['Diferencia Costo'] = 0
        return df_processed
    
    # Convertir a numérico, manejando errores
    df_processed['Valor Real de Subtotal'] = pd.to_numeric(df_processed['Valor Real de Subtotal'], errors='coerce').fillna(0)
    df_processed['Costo Recepcion'] = pd.to_numeric(df_processed['Costo Recepcion'], errors='coerce').fillna(0)
    
    # Calcular la diferencia: Valor Real de Subtotal - Costo Recepcion
    df_processed['Diferencia Costo'] = df_processed['Valor Real de Subtotal'] - df_processed['Costo Recepcion']
    
    print(f"[VENZUELA] Diferencia Costo column created")
    sys.stdout.flush()
    
    return df_processed


def update_motivo_retencion_after_diferencia_unidades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Actualiza la columna "Motivo de Retención" después de crear "Diferencia Unidades".
    
    Lógica:
    - Filtrar filas donde "Diferencia Unidades" sea 0 o null (-)
    - De esas filas, filtrar solo las que tienen "Discrepancia en Costos" o vacío en "Motivo de Retención"
    - Colocar "Discrepancia en Costos" a todas esas filas
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con "Motivo de Retención" actualizado
    """
    df_processed = df.copy()
    
    # Verificar que existan las columnas necesarias
    if 'Diferencia Unidades' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Diferencia Unidades' not found. Cannot update 'Motivo de Retención'")
        sys.stdout.flush()
        return df_processed
    
    if 'Motivo de Retención' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Motivo de Retención' not found. Cannot update it")
        sys.stdout.flush()
        return df_processed
    
    # Convertir Diferencia Unidades a numérico, manejando errores y valores como "-"
    df_processed['Diferencia Unidades'] = pd.to_numeric(df_processed['Diferencia Unidades'], errors='coerce')
    
    # Convertir Motivo de Retención a string
    df_processed['Motivo de Retención'] = df_processed['Motivo de Retención'].astype(str)
    
    # Contador para estadísticas
    updated_count = 0
    
    # Aplicar la lógica fila por fila
    for idx in df_processed.index:
        diferencia_unidades = df_processed.at[idx, 'Diferencia Unidades']
        motivo_retencion = str(df_processed.at[idx, 'Motivo de Retención']).strip()
        
        # Verificar si Diferencia Unidades es 0 o null (NaN)
        is_zero_or_null = (pd.isna(diferencia_unidades) or diferencia_unidades == 0)
        
        # Si Diferencia Unidades es 0 o null Y Motivo de Retención es "Discrepancia en Costos" o vacío
        if is_zero_or_null:
            if motivo_retencion == 'Discrepancia en Costos' or motivo_retencion == '' or motivo_retencion == 'nan':
                df_processed.at[idx, 'Motivo de Retención'] = 'Discrepancia en Costos'
                updated_count += 1
    
    print(f"[VENZUELA] Motivo de Retención updated after Diferencia Unidades: {updated_count} rows changed to 'Discrepancia en Costos'")
    sys.stdout.flush()
    
    return df_processed


def fill_motivo_retencion_unidades(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rellena la columna "Motivo de Retención" con "Discrepancia en Unidades" para los valores vacíos.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con "Motivo de Retención" rellenado
    """
    df_processed = df.copy()
    
    # Verificar que exista la columna Motivo de Retención
    if 'Motivo de Retención' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Motivo de Retención' not found. Cannot fill it")
        sys.stdout.flush()
        return df_processed
    
    # Convertir a string
    df_processed['Motivo de Retención'] = df_processed['Motivo de Retención'].astype(str)
    
    # Contador para estadísticas
    filled_count = 0
    
    # Rellenar valores vacíos con "Discrepancia en Unidades"
    for idx in df_processed.index:
        motivo_retencion = str(df_processed.at[idx, 'Motivo de Retención']).strip()
        if motivo_retencion == '' or motivo_retencion == 'nan':
            df_processed.at[idx, 'Motivo de Retención'] = 'Discrepancia en Unidades'
            filled_count += 1
    
    print(f"[VENZUELA] Motivo de Retención filled with 'Discrepancia en Unidades': {filled_count} rows filled")
    sys.stdout.flush()
    
    return df_processed


def get_tienda_mapping(credentials, spreadsheet_id: str, worksheet_gid: int = 1531818168, worksheet_name: str = 'Matriz Tienda') -> Tuple[dict, dict]:
    """
    Lee un Google Sheets y crea diccionarios de pareo entre Tienda y Área, y Tienda y Gte Área.
    
    Args:
        credentials: Credenciales de GCP
        spreadsheet_id: ID del Google Sheets
        worksheet_gid: GID de la hoja de trabajo (default: 1531818168)
        worksheet_name: Nombre de la hoja de trabajo (default: 'Matriz Tienda')
        
    Returns:
        tuple: (dict_area, dict_gerente) donde:
            - dict_area: Diccionario con Tienda como clave y Área como valor
            - dict_gerente: Diccionario con Tienda como clave y Gte Área como valor
    """
    try:
        print(f"[VENZUELA] Reading tienda mapping from Google Sheets: {spreadsheet_id}/{worksheet_name} (GID: {worksheet_gid})")
        sys.stdout.flush()
        
        # Asegurar que las credenciales tengan los scopes necesarios para Google Sheets
        if hasattr(credentials, 'with_scopes'):
            sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            credentials_with_scope = credentials.with_scopes(sheets_scopes)
        elif isinstance(credentials, service_account.Credentials):
            sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            credentials_with_scope = credentials.with_scopes(sheets_scopes)
        else:
            credentials_with_scope = credentials
        
        gspread_client = gspread.authorize(credentials_with_scope)
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        
        # Intentar obtener la hoja por nombre
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # Si no se encuentra por nombre, listar todas las hojas disponibles para debug
            try:
                all_worksheets = spreadsheet.worksheets()
                available_sheets = [ws.title for ws in all_worksheets]
                print(f"[VENZUELA] Error: Worksheet '{worksheet_name}' (GID: {worksheet_gid}) not found")
                print(f"[VENZUELA] Available worksheets: {available_sheets}")
                sys.stdout.flush()
            except Exception as e:
                print(f"[VENZUELA] Error listing worksheets: {str(e)}")
                sys.stdout.flush()
            return {}, {}
        except Exception as e:
            print(f"[VENZUELA] Error accessing worksheet: {str(e)}")
            sys.stdout.flush()
            return {}, {}
        
        # Obtener todos los valores de la hoja
        all_values = worksheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            print(f"[VENZUELA] Warning: Google Sheets is empty or has no data rows")
            sys.stdout.flush()
            return {}, {}
        
        # La primera fila son los encabezados
        headers = [h.strip() for h in all_values[0]]
        
        # Buscar los índices de las columnas
        # Los cabezales son: Estatus, Tienda, Código, Tienda, Tipo, Área, Gte Área
        # Usar la primera columna "Tienda" para el match
        if 'Tienda' not in headers:
            print(f"[VENZUELA] Error: Column 'Tienda' not found in Google Sheets. Headers: {headers}")
            sys.stdout.flush()
            return {}, {}
        
        # Buscar la primera ocurrencia de "Tienda"
        tienda_idx = headers.index('Tienda')
        
        # Buscar "Área" y "Gte Área"
        area_idx = None
        gerente_idx = None
        
        # Buscar "Área" (puede estar con o sin tilde)
        for i, h in enumerate(headers):
            if h.strip().upper() in ['ÁREA', 'AREA']:
                area_idx = i
                break
        
        # Buscar "Gte Área" o "Gerente de Area" o variaciones
        for i, h in enumerate(headers):
            h_upper = h.strip().upper()
            if 'GTE' in h_upper and 'ÁREA' in h_upper or 'GTE' in h_upper and 'AREA' in h_upper:
                gerente_idx = i
                break
            elif 'GERENTE' in h_upper and 'ÁREA' in h_upper or 'GERENTE' in h_upper and 'AREA' in h_upper:
                gerente_idx = i
                break
        
        if area_idx is None:
            print(f"[VENZUELA] Error: Column 'Área' not found in Google Sheets. Headers: {headers}")
            sys.stdout.flush()
            return {}, {}
        
        if gerente_idx is None:
            print(f"[VENZUELA] Warning: Column 'Gte Área' not found in Google Sheets. Headers: {headers}")
            sys.stdout.flush()
            gerente_idx = -1  # Usar -1 como indicador de que no existe
        
        # Crear los diccionarios de pareo
        mapping_area = {}
        mapping_gerente = {}
        
        for row in all_values[1:]:  # Saltar la fila de encabezados
            if len(row) > max(tienda_idx, area_idx, gerente_idx if gerente_idx >= 0 else 0):
                tienda = str(row[tienda_idx]).strip() if row[tienda_idx] else ''
                area = str(row[area_idx]).strip() if row[area_idx] else ''
                gerente = str(row[gerente_idx]).strip() if gerente_idx >= 0 and row[gerente_idx] else ''
                
                if tienda:  # Solo agregar si hay un nombre de tienda
                    # Normalizar la tienda eliminando espacios para el pareo
                    tienda_normalized = tienda.replace(' ', '').replace('\t', '').replace('\n', '').upper()
                    mapping_area[tienda_normalized] = area
                    if gerente_idx >= 0:
                        mapping_gerente[tienda_normalized] = gerente
        
        print(f"[VENZUELA] Loaded {len(mapping_area)} tienda mappings from Google Sheets (Area: {len(mapping_area)}, Gerente: {len(mapping_gerente)})")
        sys.stdout.flush()
        return mapping_area, mapping_gerente
        
    except Exception as e:
        print(f"[VENZUELA] Error reading tienda mapping from Google Sheets: {str(e)}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        return {}, {}


def add_area_column(df: pd.DataFrame, credentials=None) -> pd.DataFrame:
    """
    Crea la columna "Area" haciendo pareo con Google Sheets.
    Hace pareo entre la columna "Tienda" y "Tienda" del Google Sheets.
    
    Args:
        df: DataFrame original
        credentials: Credenciales de GCP (opcional, necesario para pareo con Google Sheets)
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Area" agregada
    """
    df_processed = df.copy()
    
    # Verificar que exista la columna Tienda
    if 'Tienda' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Tienda' not found. Cannot create 'Area' column")
        sys.stdout.flush()
        df_processed['Area'] = ''
        return df_processed
    
    # Verificar que se proporcionen credenciales
    if not credentials:
        print(f"[VENZUELA] Warning: No credentials provided. Cannot create 'Area' column")
        sys.stdout.flush()
        df_processed['Area'] = ''
        return df_processed
    
    # Cargar variables de entorno desde .env si existe
    load_env_file()
    
    # Obtener el ID del Google Sheets desde variables de entorno
    spreadsheet_id = os.getenv('SHEETS_PROVIDER_MAPPING_ID')
    if not spreadsheet_id:
        print(f"[VENZUELA] Warning: SHEETS_PROVIDER_MAPPING_ID not found in environment variables")
        sys.stdout.flush()
        df_processed['Area'] = ''
        return df_processed
    
    print(f"[VENZUELA] Creating 'Area' column using Google Sheets mapping...")
    sys.stdout.flush()
    
    # Obtener el mapeo de tiendas desde Google Sheets
    tienda_area_mapping, _ = get_tienda_mapping(credentials, spreadsheet_id)
    
    if not tienda_area_mapping:
        print(f"[VENZUELA] Warning: Could not load tienda mapping from Google Sheets")
        sys.stdout.flush()
        df_processed['Area'] = ''
        return df_processed
    
    # Inicializar la nueva columna con valores vacíos
    df_processed['Area'] = ''
    
    # Convertir Tienda a string para hacer el pareo
    df_processed['Tienda'] = df_processed['Tienda'].astype(str)
    
    # Hacer el pareo: buscar cada valor de Tienda en el diccionario
    # Normalizar eliminando todos los espacios para hacer el pareo más efectivo
    matched_count = 0
    for idx, tienda in df_processed['Tienda'].items():
        # Normalizar el valor de Tienda eliminando todos los espacios (incluyendo tabs y newlines)
        tienda_normalized = str(tienda).strip().replace(' ', '').replace('\t', '').replace('\n', '').upper()
        # Buscar coincidencia con la versión normalizada
        if tienda_normalized in tienda_area_mapping:
            df_processed.at[idx, 'Area'] = tienda_area_mapping[tienda_normalized]
            matched_count += 1
    
    print(f"[VENZUELA] Matched {matched_count} out of {len(df_processed)} rows with tienda-area mapping")
    if matched_count < len(df_processed):
        unmatched = len(df_processed) - matched_count
        print(f"[VENZUELA] Warning: {unmatched} rows could not be matched with tienda-area mapping")
    sys.stdout.flush()
    
    return df_processed


def add_gerente_area_column(df: pd.DataFrame, credentials=None) -> pd.DataFrame:
    """
    Crea la columna "Gerente de Area" haciendo pareo con Google Sheets.
    Hace pareo entre la columna "Tienda" y "Tienda" del Google Sheets.
    
    Args:
        df: DataFrame original
        credentials: Credenciales de GCP (opcional, necesario para pareo con Google Sheets)
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Gerente de Area" agregada
    """
    df_processed = df.copy()
    
    # Verificar que exista la columna Tienda
    if 'Tienda' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Tienda' not found. Cannot create 'Gerente de Area' column")
        sys.stdout.flush()
        df_processed['Gerente de Area'] = ''
        return df_processed
    
    # Verificar que se proporcionen credenciales
    if not credentials:
        print(f"[VENZUELA] Warning: No credentials provided. Cannot create 'Gerente de Area' column")
        sys.stdout.flush()
        df_processed['Gerente de Area'] = ''
        return df_processed
    
    # Cargar variables de entorno desde .env si existe
    load_env_file()
    
    # Obtener el ID del Google Sheets desde variables de entorno
    spreadsheet_id = os.getenv('SHEETS_PROVIDER_MAPPING_ID')
    if not spreadsheet_id:
        print(f"[VENZUELA] Warning: SHEETS_PROVIDER_MAPPING_ID not found in environment variables")
        sys.stdout.flush()
        df_processed['Gerente de Area'] = ''
        return df_processed
    
    print(f"[VENZUELA] Creating 'Gerente de Area' column using Google Sheets mapping...")
    sys.stdout.flush()
    
    # Obtener el mapeo de tiendas desde Google Sheets
    _, tienda_gerente_mapping = get_tienda_mapping(credentials, spreadsheet_id)
    
    if not tienda_gerente_mapping:
        print(f"[VENZUELA] Warning: Could not load tienda-gerente mapping from Google Sheets")
        sys.stdout.flush()
        df_processed['Gerente de Area'] = ''
        return df_processed
    
    # Inicializar la nueva columna con valores vacíos
    df_processed['Gerente de Area'] = ''
    
    # Convertir Tienda a string para hacer el pareo
    df_processed['Tienda'] = df_processed['Tienda'].astype(str)
    
    # Hacer el pareo: buscar cada valor de Tienda en el diccionario
    # Normalizar eliminando todos los espacios para hacer el pareo más efectivo
    matched_count = 0
    for idx, tienda in df_processed['Tienda'].items():
        # Normalizar el valor de Tienda eliminando todos los espacios (incluyendo tabs y newlines)
        tienda_normalized = str(tienda).strip().replace(' ', '').replace('\t', '').replace('\n', '').upper()
        # Buscar coincidencia con la versión normalizada
        if tienda_normalized in tienda_gerente_mapping:
            df_processed.at[idx, 'Gerente de Area'] = tienda_gerente_mapping[tienda_normalized]
            matched_count += 1
    
    print(f"[VENZUELA] Matched {matched_count} out of {len(df_processed)} rows with tienda-gerente mapping")
    if matched_count < len(df_processed):
        unmatched = len(df_processed) - matched_count
        print(f"[VENZUELA] Warning: {unmatched} rows could not be matched with tienda-gerente mapping")
    sys.stdout.flush()
    
    return df_processed


def validate_cendis_area(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida y corrige las columnas "Area" y "Gerente de Area" para tiendas CENDIS.
    
    Lógica:
    - Si "Tienda" es "CENDIS", entonces "Area" debe ser "CENDIS" y "Gerente de Area" debe estar vacío.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con validación aplicada
    """
    df_processed = df.copy()
    
    # Verificar que existan las columnas necesarias
    if 'Tienda' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Tienda' not found. Cannot validate CENDIS")
        sys.stdout.flush()
        return df_processed
    
    if 'Area' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Area' not found. Cannot validate CENDIS")
        sys.stdout.flush()
        return df_processed
    
    if 'Gerente de Area' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Gerente de Area' not found. Cannot validate CENDIS")
        sys.stdout.flush()
        return df_processed
    
    # Convertir columnas a string
    df_processed['Tienda'] = df_processed['Tienda'].astype(str)
    df_processed['Area'] = df_processed['Area'].astype(str)
    df_processed['Gerente de Area'] = df_processed['Gerente de Area'].astype(str)
    
    # Contador para estadísticas
    corrected_count = 0
    
    # Aplicar la validación fila por fila
    for idx in df_processed.index:
        tienda = str(df_processed.at[idx, 'Tienda']).strip().upper()
        
        # Si la tienda es CENDIS, validar y corregir
        if tienda == 'CENDIS':
            # Corregir Area
            df_processed.at[idx, 'Area'] = 'CENDIS'
            # Corregir Gerente de Area (debe estar vacío)
            df_processed.at[idx, 'Gerente de Area'] = ''
            corrected_count += 1
    
    print(f"[VENZUELA] CENDIS validation applied: {corrected_count} rows corrected (Area='CENDIS', Gerente de Area='')")
    sys.stdout.flush()
    
    return df_processed


def add_rango_fecha_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "Rango de fecha" calculando la diferencia en días entre la fecha de hoy y "Fecha Recepción".
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "Rango de fecha" agregada
    """
    df_processed = df.copy()
    
    # Verificar que exista la columna Fecha Recepción
    if 'Fecha Recepción' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Fecha Recepción' not found. Cannot create 'Rango de fecha' column")
        sys.stdout.flush()
        df_processed['Rango de fecha'] = 0
        return df_processed
    
    print(f"[VENZUELA] Creating 'Rango de fecha' column...")
    sys.stdout.flush()
    
    # Obtener la fecha de hoy
    fecha_hoy = date.today()
    
    # Convertir "Fecha Recepción" a datetime si no lo es
    df_processed['Fecha Recepción'] = pd.to_datetime(df_processed['Fecha Recepción'], errors='coerce')
    
    # Calcular la diferencia en días
    df_processed['Rango de fecha'] = (fecha_hoy - df_processed['Fecha Recepción'].dt.date).dt.days
    
    # Reemplazar valores NaN con 0
    df_processed['Rango de fecha'] = df_processed['Rango de fecha'].fillna(0).astype(int)
    
    # Contador para estadísticas
    valid_dates = df_processed['Rango de fecha'].notna().sum()
    print(f"[VENZUELA] Rango de fecha column created. Valid dates: {valid_dates} out of {len(df_processed)}")
    sys.stdout.flush()
    
    return df_processed


def add_rango_0_30_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "0-30" que indica si el rango de fecha está entre 0 y 30 días.
    Si está en el rango, pone 1, sino 0.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "0-30" agregada
    """
    df_processed = df.copy()
    
    if 'Rango de fecha' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Rango de fecha' not found. Cannot create '0-30' column")
        sys.stdout.flush()
        df_processed['0-30'] = 0
        return df_processed
    
    print(f"[VENZUELA] Creating '0-30' column...")
    sys.stdout.flush()
    
    # Convertir a numérico
    df_processed['Rango de fecha'] = pd.to_numeric(df_processed['Rango de fecha'], errors='coerce').fillna(0)
    
    # Aplicar la lógica: si está entre 0 y 30 (inclusive), poner 1, sino 0
    df_processed['0-30'] = ((df_processed['Rango de fecha'] >= 0) & (df_processed['Rango de fecha'] <= 30)).astype(int)
    
    count = df_processed['0-30'].sum()
    print(f"[VENZUELA] 0-30 column created. {count} rows in range 0-30")
    sys.stdout.flush()
    
    return df_processed


def add_rango_30_60_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "30-60" que indica si el rango de fecha está entre 30 y 60 días.
    Si está en el rango, pone 1, sino 0.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "30-60" agregada
    """
    df_processed = df.copy()
    
    if 'Rango de fecha' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Rango de fecha' not found. Cannot create '30-60' column")
        sys.stdout.flush()
        df_processed['30-60'] = 0
        return df_processed
    
    print(f"[VENZUELA] Creating '30-60' column...")
    sys.stdout.flush()
    
    # Convertir a numérico
    df_processed['Rango de fecha'] = pd.to_numeric(df_processed['Rango de fecha'], errors='coerce').fillna(0)
    
    # Aplicar la lógica: si está entre 30 y 60 (excluyendo 30, incluyendo 60), poner 1, sino 0
    df_processed['30-60'] = ((df_processed['Rango de fecha'] > 30) & (df_processed['Rango de fecha'] <= 60)).astype(int)
    
    count = df_processed['30-60'].sum()
    print(f"[VENZUELA] 30-60 column created. {count} rows in range 30-60")
    sys.stdout.flush()
    
    return df_processed


def add_rango_60_90_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "60-90" que indica si el rango de fecha está entre 60 y 90 días.
    Si está en el rango, pone 1, sino 0.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "60-90" agregada
    """
    df_processed = df.copy()
    
    if 'Rango de fecha' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Rango de fecha' not found. Cannot create '60-90' column")
        sys.stdout.flush()
        df_processed['60-90'] = 0
        return df_processed
    
    print(f"[VENZUELA] Creating '60-90' column...")
    sys.stdout.flush()
    
    # Convertir a numérico
    df_processed['Rango de fecha'] = pd.to_numeric(df_processed['Rango de fecha'], errors='coerce').fillna(0)
    
    # Aplicar la lógica: si está entre 60 y 90 (excluyendo 60, incluyendo 90), poner 1, sino 0
    df_processed['60-90'] = ((df_processed['Rango de fecha'] > 60) & (df_processed['Rango de fecha'] <= 90)).astype(int)
    
    count = df_processed['60-90'].sum()
    print(f"[VENZUELA] 60-90 column created. {count} rows in range 60-90")
    sys.stdout.flush()
    
    return df_processed


def add_rango_90_120_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "90-120" que indica si el rango de fecha está entre 90 y 120 días.
    Si está en el rango, pone 1, sino 0.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "90-120" agregada
    """
    df_processed = df.copy()
    
    if 'Rango de fecha' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Rango de fecha' not found. Cannot create '90-120' column")
        sys.stdout.flush()
        df_processed['90-120'] = 0
        return df_processed
    
    print(f"[VENZUELA] Creating '90-120' column...")
    sys.stdout.flush()
    
    # Convertir a numérico
    df_processed['Rango de fecha'] = pd.to_numeric(df_processed['Rango de fecha'], errors='coerce').fillna(0)
    
    # Aplicar la lógica: si está entre 90 y 120 (excluyendo 90, incluyendo 120), poner 1, sino 0
    df_processed['90-120'] = ((df_processed['Rango de fecha'] > 90) & (df_processed['Rango de fecha'] <= 120)).astype(int)
    
    count = df_processed['90-120'].sum()
    print(f"[VENZUELA] 90-120 column created. {count} rows in range 90-120")
    sys.stdout.flush()
    
    return df_processed


def add_rango_120_plus_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna "+120" que indica si el rango de fecha es mayor a 120 días.
    Si es mayor a 120, pone 1, sino 0.
    
    Args:
        df: DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame con la columna "+120" agregada
    """
    df_processed = df.copy()
    
    if 'Rango de fecha' not in df_processed.columns:
        print(f"[VENZUELA] Warning: Column 'Rango de fecha' not found. Cannot create '+120' column")
        sys.stdout.flush()
        df_processed['+120'] = 0
        return df_processed
    
    print(f"[VENZUELA] Creating '+120' column...")
    sys.stdout.flush()
    
    # Convertir a numérico
    df_processed['Rango de fecha'] = pd.to_numeric(df_processed['Rango de fecha'], errors='coerce').fillna(0)
    
    # Aplicar la lógica: si es mayor a 120, poner 1, sino 0
    df_processed['+120'] = (df_processed['Rango de fecha'] > 120).astype(int)
    
    count = df_processed['+120'].sum()
    print(f"[VENZUELA] +120 column created. {count} rows with range > 120")
    sys.stdout.flush()
    
    return df_processed


def process_dataframe(df: pd.DataFrame, credentials=None) -> pd.DataFrame:
    """
    Procesa el DataFrame según la lógica de negocio para archivos R011.
    Aplica los filtros y transformaciones en orden.
    
    Limpiezas aplicadas (en orden):
    1. Eliminar filas completamente vacías
    2. Eliminar filas donde "Número Factura" tenga el prefijo "NDINT"
    3. Crear columna "Unidad de Negocio" haciendo pareo con Google Sheets
    4. Crear columna "Tipo de Proveedor" basándose en "Sucursal" y "Tienda"
    5. Crear columna "Motivo de Retención" basándose en "Estado" y "Tipo de Proveedor"
    6. Crear columna "Validacion de OC" contando órdenes de compra repetidas
    7. Crear columna "Diferencia Real" basándose en "Validacion de OC"
    8. Crear columna "Valor Real de Unidades" sumando unidades por orden de compra
    9. Crear columna "Diferencia Unidades" restando "Valor Real de Unidades" menos "Unidades Recibidas"
    10. Actualizar "Motivo de Retención" basándose en "Diferencia Unidades"
    11. Crear columna "Valor Real de Subtotal" sumando subtotales por orden de compra
    12. Crear columna "Diferencia Costo" restando "Valor Real de Subtotal" menos "Costo Recepcion"
    13. Rellenar "Motivo de Retención" con "Discrepancia en Unidades" para valores vacíos
    14. Crear columna "Area" haciendo pareo con Google Sheets (Matriz Tienda)
    15. Crear columna "Gerente de Area" haciendo pareo con Google Sheets (Matriz Tienda)
    16. Validar y corregir "Area" y "Gerente de Area" para tiendas CENDIS
    17. Crear columna "Rango de fecha" calculando diferencia entre hoy y "Fecha Recepción"
    18. Crear columna "0-30" indicando si el rango está entre 0 y 30 días
    19. Crear columna "30-60" indicando si el rango está entre 30 y 60 días
    20. Crear columna "60-90" indicando si el rango está entre 60 y 90 días
    21. Crear columna "90-120" indicando si el rango está entre 90 y 120 días
    22. Crear columna "+120" indicando si el rango es mayor a 120 días
    
    Args:
        df: DataFrame original
        credentials: Credenciales de GCP (opcional, necesario para pareo con Google Sheets)
        
    Returns:
        pd.DataFrame: DataFrame procesado
    """
    # Crear una copia para no modificar el original
    df_processed = df.copy()
    
    initial_rows = len(df_processed)
    print(f"[VENZUELA] Starting dataframe processing. Initial rows: {initial_rows}")
    sys.stdout.flush()
    
    # Aplicar filtros en orden
    # 1. Eliminar filas completamente vacías
    df_processed = remove_empty_rows(df_processed)
    
    # 2. Eliminar filas donde "Número Factura" tenga el prefijo "NDINT"
    df_processed = remove_ndint_invoices(df_processed)
    
    # 3. Crear columna "Unidad de Negocio" haciendo pareo con Google Sheets
    df_processed = add_unidad_negocio_column(df_processed, credentials)
    
    # 4. Crear columna "Tipo de Proveedor" basándose en "Sucursal" y "Tienda"
    df_processed = add_tipo_proveedor_column(df_processed)
    
    # 5. Crear columna "Motivo de Retención" basándose en "Estado" y "Tipo de Proveedor"
    df_processed = add_motivo_retencion_column(df_processed)
    
    # 6. Crear columna "Validacion de OC" contando órdenes de compra repetidas
    df_processed = add_validacion_oc_column(df_processed)
    
    # 7. Crear columna "Diferencia Real" basándose en "Validacion de OC"
    df_processed = add_diferencia_real_column(df_processed)
    
    # 8. Crear columna "Valor Real de Unidades" sumando unidades por orden de compra
    df_processed = add_valor_real_unidades_column(df_processed)
    
    # 9. Crear columna "Diferencia Unidades" restando "Valor Real de Unidades" menos "Unidades Recibidas"
    df_processed = add_diferencia_unidades_column(df_processed)
    
    # 10. Actualizar "Motivo de Retención" basándose en "Diferencia Unidades"
    df_processed = update_motivo_retencion_after_diferencia_unidades(df_processed)
    
    # 11. Crear columna "Valor Real de Subtotal" sumando subtotales por orden de compra
    df_processed = add_valor_real_subtotal_column(df_processed)
    
    # 12. Crear columna "Diferencia Costo" restando "Valor Real de Subtotal" menos "Costo Recepcion"
    df_processed = add_diferencia_costo_column(df_processed)
    
    # 13. Rellenar "Motivo de Retención" con "Discrepancia en Unidades" para valores vacíos
    df_processed = fill_motivo_retencion_unidades(df_processed)
    
    # 14. Crear columna "Area" haciendo pareo con Google Sheets (Matriz Tienda)
    df_processed = add_area_column(df_processed, credentials)
    
    # 15. Crear columna "Gerente de Area" haciendo pareo con Google Sheets (Matriz Tienda)
    df_processed = add_gerente_area_column(df_processed, credentials)
    
    # 16. Validar y corregir "Area" y "Gerente de Area" para tiendas CENDIS
    df_processed = validate_cendis_area(df_processed)
    
    # 17. Crear columna "Rango de fecha" calculando diferencia entre hoy y "Fecha Recepción"
    df_processed = add_rango_fecha_column(df_processed)
    
    # 18. Crear columna "0-30" indicando si el rango está entre 0 y 30 días
    df_processed = add_rango_0_30_column(df_processed)
    
    # 19. Crear columna "30-60" indicando si el rango está entre 30 y 60 días
    df_processed = add_rango_30_60_column(df_processed)
    
    # 20. Crear columna "60-90" indicando si el rango está entre 60 y 90 días
    df_processed = add_rango_60_90_column(df_processed)
    
    # 21. Crear columna "90-120" indicando si el rango está entre 90 y 120 días
    df_processed = add_rango_90_120_column(df_processed)
    
    # 22. Crear columna "+120" indicando si el rango es mayor a 120 días
    df_processed = add_rango_120_plus_column(df_processed)
    
    final_rows = len(df_processed)
    print(f"[VENZUELA] Processing completed. Final rows: {final_rows} (removed {initial_rows - final_rows} total)")
    sys.stdout.flush()
    
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
        
        print(f"[VENZUELA] Uploading {len(df)} rows to BigQuery: {dataset_id}.{table_id}")
        sys.stdout.flush()
        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Esperar a que termine el job
        
        print(f"[VENZUELA] DataFrame uploaded to BigQuery: {dataset_id}.{table_id}")
        sys.stdout.flush()
        return True
        
    except Exception as e:
        print(f"[VENZUELA] Error uploading to BigQuery: {str(e)}")
        sys.stdout.flush()
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
        print(f"[VENZUELA] Uploading file to Cloud Storage: gs://{bucket_name}/{blob_name}")
        sys.stdout.flush()
        blob.upload_from_string(
            file_content, 
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        print(f"[VENZUELA] File uploaded to Cloud Storage: gs://{bucket_name}/{blob_name}")
        sys.stdout.flush()
        return True
        
    except Exception as e:
        print(f"[VENZUELA] Error uploading to Cloud Storage: {str(e)}")
        sys.stdout.flush()
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
        # Asegurar que las credenciales tengan los scopes necesarios para Google Sheets
        if hasattr(credentials, 'with_scopes'):
            sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
            credentials_with_scope = credentials.with_scopes(sheets_scopes)
        elif isinstance(credentials, service_account.Credentials):
            sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
            credentials_with_scope = credentials.with_scopes(sheets_scopes)
        else:
            credentials_with_scope = credentials
        
        gspread_client = gspread.authorize(credentials_with_scope)
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=26)
        
        if clear:
            worksheet.clear()
        
        # Actualizar la hoja con los datos del DataFrame
        print(f"[VENZUELA] Uploading {len(df)} rows to Google Sheets: {spreadsheet_id}/{worksheet_name}")
        sys.stdout.flush()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        
        print(f"[VENZUELA] DataFrame uploaded to Google Sheets: {spreadsheet_id}/{worksheet_name}")
        sys.stdout.flush()
        return True
        
    except Exception as e:
        print(f"[VENZUELA] Error uploading to Google Sheets: {str(e)}")
        sys.stdout.flush()
        return False


def get_credentials_local():
    """
    Obtiene credenciales de GCP para uso local, primero intenta desde credentials.json,
    si no está disponible, usa ADC (Application Default Credentials).
    Las credenciales incluyen los scopes necesarios para Google Sheets.
    
    Returns:
        tuple: (credentials, project_id)
    """
    # Para uso local, buscar credentials.json en la raíz del proyecto
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')
    
    # Scopes necesarios para Google Sheets
    sheets_scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    
    # Intentar cargar desde credentials.json
    if os.path.exists(credentials_path):
        try:
            print(f"[MAIN] Loading credentials from {credentials_path}")
            sys.stdout.flush()
            credentials, project = load_credentials_from_file(credentials_path)
            
            # Si las credenciales soportan with_scopes, asegurar que tengan los scopes
            if hasattr(credentials, 'with_scopes'):
                credentials = credentials.with_scopes(sheets_scopes)
            
            return credentials, project
        except Exception as e:
            print(f"[MAIN] Warning: Could not load credentials from {credentials_path}: {str(e)}")
            print("[MAIN] Falling back to Application Default Credentials (ADC)")
            sys.stdout.flush()
    
    # Fallback a ADC
    print("[MAIN] Using Application Default Credentials (ADC)")
    sys.stdout.flush()
    credentials, project = default()
    
    # Si las credenciales soportan with_scopes, asegurar que tengan los scopes
    if hasattr(credentials, 'with_scopes'):
        credentials = credentials.with_scopes(sheets_scopes)
    
    return credentials, project


def main():
    """
    Función main para testeo local.
    Procesa un archivo Excel y guarda el resultado en la carpeta resultados.
    
    Uso:
        python src/venezuela.py <ruta_al_archivo_excel>
        
    O modifica la variable INPUT_FILE en el código.
    """
    # Cargar variables de entorno desde .env al inicio
    load_env_file()
    
    # Configurar el archivo de entrada
    # Opción 1: Usar argumento de línea de comandos
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        # Opción 2: Modificar esta ruta para tu archivo de prueba
        input_file = "D:/Users/andres.moreno/Documents/archivos Informe de Retenidas DIRECTO/minutasesinautomatizacindeinformederetenidasprim/R011-Finanzas-Informe de Documentos ReIM_ftd_Reporte_Documentos_ReIM - 2025-12-11T091613.648.xlsx"
    # Verificar que el archivo existe
    if not os.path.exists(input_file):
        print(f"[MAIN] Error: File not found: {input_file}")
        print(f"[MAIN] Usage: python src/venezuela.py <ruta_al_archivo_excel>")
        sys.exit(1)
    
    print("=" * 60)
    print("[MAIN] Starting local test processing")
    print(f"[MAIN] Input file: {input_file}")
    print("=" * 60)
    sys.stdout.flush()
    
    try:
        # Leer el archivo
        print(f"[MAIN] Reading file: {input_file}")
        sys.stdout.flush()
        with open(input_file, 'rb') as f:
            file_content = f.read()
        
        filename = os.path.basename(input_file)
        print(f"[MAIN] File size: {len(file_content)} bytes")
        sys.stdout.flush()
        
        # Obtener credenciales
        print(f"[MAIN] Getting credentials...")
        sys.stdout.flush()
        credentials, project_id = get_credentials_local()
        print(f"[MAIN] Credentials loaded. Project: {project_id}")
        sys.stdout.flush()
        
        # Procesar el archivo
        print(f"[MAIN] Processing file...")
        sys.stdout.flush()
        processed_content = process_excel_file(file_content, filename, credentials)
        
        # Crear carpeta resultados si no existe
        resultados_dir = "resultados"
        if not os.path.exists(resultados_dir):
            os.makedirs(resultados_dir)
            print(f"[MAIN] Created directory: {resultados_dir}")
            sys.stdout.flush()
        
        # Generar nombre del archivo de salida con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"Informe_R011_{timestamp}.xlsx"
        output_path = os.path.join(resultados_dir, output_filename)
        
        # Guardar el archivo procesado
        print(f"[MAIN] Saving processed file to: {output_path}")
        sys.stdout.flush()
        with open(output_path, 'wb') as f:
            f.write(processed_content)
        
        print("=" * 60)
        print(f"[MAIN] Processing completed successfully!")
        print(f"[MAIN] Output file: {output_path}")
        print(f"[MAIN] Output size: {len(processed_content)} bytes")
        print("=" * 60)
        sys.stdout.flush()
        
    except Exception as e:
        print("=" * 60)
        print(f"[MAIN] Error during processing: {str(e)}")
        print("=" * 60)
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
