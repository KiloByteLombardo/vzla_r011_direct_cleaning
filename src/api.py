import os
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from google.auth import default, load_credentials_from_file
from google.cloud import bigquery, storage
import gspread
from werkzeug.utils import secure_filename
import io
import venezuela

app = Flask(__name__)
# Configurar CORS para permitir todos los orígenes
CORS(app, resources={r"/*": {"origins": "*"}})


def get_credentials():
    """
    Obtiene credenciales de GCP, primero intenta desde credentials.json,
    si no está disponible, usa ADC (Application Default Credentials).
    
    Returns:
        tuple: (credentials, project_id)
    """
    # Obtener la ruta desde variable de entorno o usar la ruta por defecto
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/app/credentials.json')
    
    # Intentar cargar desde credentials.json
    if os.path.exists(credentials_path):
        try:
            print(f"Loading credentials from {credentials_path}")
            credentials, project = load_credentials_from_file(credentials_path)
            return credentials, project
        except Exception as e:
            print(f"Warning: Could not load credentials from {credentials_path}: {str(e)}")
            print("Falling back to Application Default Credentials (ADC)")
    
    # Fallback a ADC
    print("Using Application Default Credentials (ADC)")
    credentials, project = default()
    return credentials, project


def test_bigquery_connection(credentials, project_id: str) -> tuple:
    """
    Prueba la conexión a BigQuery.
    
    Args:
        credentials: Credenciales de GCP
        project_id: ID del proyecto de GCP
    
    Returns:
        tuple: (success, message)
    """
    try:
        bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
        # Intentar listar datasets del proyecto
        datasets = list(bigquery_client.list_datasets())
        return True, f"Successfully connected to BigQuery. Project: {project_id}, Datasets found: {len(datasets)}"
    except Exception as e:
        return False, f"Error connecting to BigQuery: {str(e)}"


def test_storage_connection(credentials, project_id: str) -> tuple:
    """
    Prueba la conexión a Cloud Storage.
    
    Args:
        credentials: Credenciales de GCP
        project_id: ID del proyecto de GCP
    
    Returns:
        tuple: (success, message)
    """
    try:
        storage_client = storage.Client(credentials=credentials, project=project_id)
        # Intentar listar buckets del proyecto
        buckets = list(storage_client.list_buckets())
        return True, f"Successfully connected to Cloud Storage. Project: {project_id}, Buckets found: {len(buckets)}"
    except Exception as e:
        return False, f"Error connecting to Cloud Storage: {str(e)}"


@app.route('/health', methods=['GET'])
def health():
    """
    Endpoint de health check.
    
    Returns:
        JSON con el estado del servicio
    """
    return jsonify({
        'status': 'healthy',
        'service': 'vzla-r011-direct-cleaning',
        'message': 'Service is running'
    }), 200


@app.route('/test/bigquery', methods=['GET'])
def test_bigquery_endpoint():
    """
    Endpoint para probar la conexión a BigQuery.
    
    Returns:
        JSON con el resultado de la prueba de conexión
    """
    try:
        credentials, project_id = get_credentials()
        success, message = test_bigquery_connection(credentials, project_id)
        status_code = 200 if success else 500
        return jsonify({
            'success': success,
            'message': message
        }), status_code
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error testing BigQuery connection: {str(e)}'
        }), 500


@app.route('/test/storage', methods=['GET'])
def test_storage_endpoint():
    """
    Endpoint para probar la conexión a Cloud Storage.
    
    Returns:
        JSON con el resultado de la prueba de conexión
    """
    try:
        credentials, project_id = get_credentials()
        success, message = test_storage_connection(credentials, project_id)
        status_code = 200 if success else 500
        return jsonify({
            'success': success,
            'message': message
        }), status_code
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error testing Cloud Storage connection: {str(e)}'
        }), 500


@app.route('/process', methods=['POST'])
def process_file():
    """
    Endpoint para procesar un archivo Excel.
    Recibe un archivo Excel, lo procesa y devuelve el resultado.
    
    Query parameters opcionales:
        - upload_bigquery: Si está presente, sube el resultado a BigQuery
        - dataset_id: ID del dataset de BigQuery
        - table_id: ID de la tabla de BigQuery
        - upload_storage: Si está presente, sube el resultado a Cloud Storage
        - bucket_name: Nombre del bucket de Cloud Storage
        - blob_name: Nombre del blob en Cloud Storage
        - upload_sheets: Si está presente, sube el resultado a Google Sheets
        - spreadsheet_id: ID de la hoja de cálculo
        - worksheet_name: Nombre de la hoja de trabajo
    
    Returns:
        Archivo Excel procesado o JSON con información del procesamiento
    """
    try:
        # Verificar que se haya enviado un archivo
        if 'file' not in request.files:
            return jsonify({
                'error': 'No file provided',
                'message': 'Please provide an Excel file in the "file" field'
            }), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({
                'error': 'No file selected',
                'message': 'Please select a file to upload'
            }), 400
        
        # Verificar que sea un archivo Excel
        if not (file.filename.endswith('.xlsx') or file.filename.endswith('.xls')):
            return jsonify({
                'error': 'Invalid file type',
                'message': 'Please upload an Excel file (.xlsx or .xls)'
            }), 400
        
        # Leer el contenido del archivo
        file_content = file.read()
        filename = secure_filename(file.filename)
        
        # Procesar el archivo
        processed_content = venezuela.process_excel_file(file_content, filename)
        
        # Obtener credenciales para los uploads
        credentials, project_id = get_credentials()
        
        # Obtener parámetros opcionales
        upload_bigquery = request.args.get('upload_bigquery', 'false').lower() == 'true'
        upload_storage = request.args.get('upload_storage', 'false').lower() == 'true'
        upload_sheets = request.args.get('upload_sheets', 'false').lower() == 'true'
        
        response_data = {
            'success': True,
            'message': 'File processed successfully',
            'filename': filename,
            'uploads': {}
        }
        
        # Subir a BigQuery si se solicita
        if upload_bigquery:
            dataset_id = request.args.get('dataset_id')
            table_id = request.args.get('table_id')
            if dataset_id and table_id:
                import pandas as pd
                df = pd.read_excel(io.BytesIO(processed_content))
                success = venezuela.upload_to_bigquery(
                    df, credentials, project_id, dataset_id, table_id
                )
                response_data['uploads']['bigquery'] = {
                    'success': success,
                    'dataset': dataset_id,
                    'table': table_id
                }
            else:
                response_data['uploads']['bigquery'] = {
                    'success': False,
                    'message': 'dataset_id and table_id are required'
                }
        
        # Subir a Cloud Storage si se solicita
        if upload_storage:
            bucket_name = request.args.get('bucket_name')
            blob_name = request.args.get('blob_name', filename)
            if bucket_name:
                success = venezuela.upload_to_storage(
                    processed_content, credentials, project_id, bucket_name, blob_name
                )
                response_data['uploads']['storage'] = {
                    'success': success,
                    'bucket': bucket_name,
                    'blob': blob_name
                }
            else:
                response_data['uploads']['storage'] = {
                    'success': False,
                    'message': 'bucket_name is required'
                }
        
        # Subir a Google Sheets si se solicita
        if upload_sheets:
            spreadsheet_id = request.args.get('spreadsheet_id')
            worksheet_name = request.args.get('worksheet_name', 'Sheet1')
            if spreadsheet_id:
                import pandas as pd
                df = pd.read_excel(io.BytesIO(processed_content))
                success = venezuela.upload_to_sheets(
                    df, credentials, spreadsheet_id, worksheet_name
                )
                response_data['uploads']['sheets'] = {
                    'success': success,
                    'spreadsheet_id': spreadsheet_id,
                    'worksheet': worksheet_name
                }
            else:
                response_data['uploads']['sheets'] = {
                    'success': False,
                    'message': 'spreadsheet_id is required'
                }
        
        # Si no se solicita ninguna subida, devolver el archivo procesado
        if not (upload_bigquery or upload_storage or upload_sheets):
            output_filename = f"processed_{filename}"
            return send_file(
                io.BytesIO(processed_content),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=output_filename
            )
        
        # Si se solicitó alguna subida, devolver JSON con la información
        return jsonify(response_data), 200
        
    except Exception as e:
        return jsonify({
            'error': 'Processing failed',
            'message': str(e)
        }), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8750))
    app.run(host='0.0.0.0', port=port, debug=True)
