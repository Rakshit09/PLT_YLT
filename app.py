import os
import io
import logging
import zipfile
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for, Response
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import create_engine, text, URL
from dotenv import load_dotenv
from sqlalchemy.exc import ProgrammingError
import base64
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-here-change-in-production')
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024 * 1024  # 5GB max file size
app.config['SESSION_TYPE'] = 'filesystem'

DATABRIDGE = '103db9bcc5307a1d669c5f0946a36dfc.databridge.rms-pe.com'
EDM_SERVERS = ('GREAZUK1DB051P', 'GREAZUK1DB101P', 'GREAZUK1DB181P', 'GREAZUK1DB201P', 'GREAZUK1DB251P', 'DATABRIDGE')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_engine(server: str, database: str, username: str, password: str, domain: str = None):
    try:
        if server == 'DATABRIDGE':
            server = DATABRIDGE


        # domain authentication
        if domain and domain.strip():
            username = f"{domain}\\{username}"

        host = server
        port = None
        if ',' in server:
            host, port_str = server.split(',', 1)
            try:
                port = int(port_str)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse port from server string: {server}")
                port = None

        connection_url = URL.create(
            "mssql+pymssql",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
            query={"timeout": "30"}
        )
        
        engine = sa.create_engine(
            connection_url,
            pool_size=20,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
            pool_timeout=300,
            echo_pool=False,
            execution_options={
                "isolation_level": "AUTOCOMMIT",
                "stream_results": True,
            },
            future=True,
        )
        
        # test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info(f"Successfully created engine for {server}/{database}")
        return engine
        
    except Exception as exc:
        logger.error(f"Failed to create engine: {exc}")
        raise

def convert_csv_plt_to_ylt(df):
    try:
    
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        period_col = None
        event_col = None
        loss_col = None
        
        #  period column
        for pattern in ['periodid', 'period_id', 'period']:
            if pattern in df_columns_lower:
                period_col = df_columns_lower[pattern]
                break
        
        #  event column
        for pattern in ['eventid', 'event_id', 'event']:
            if pattern in df_columns_lower:
                event_col = df_columns_lower[pattern]
                break
        
        #  loss column
        for pattern in ['loss', 'losses', 'ground_up_loss']:
            if pattern in df_columns_lower:
                loss_col = df_columns_lower[pattern]
                break
        
        if not all([period_col, event_col, loss_col]):
            # try positional 
            if len(df.columns) >= 3:
                period_col = df.columns[0]
                event_col = df.columns[1]
                loss_col = df.columns[2]
                logger.warning(f"Using positional columns: {period_col}, {event_col}, {loss_col}")
            else:
                raise ValueError(f"Cannot identify required columns. Found columns: {df.columns.tolist()}")
        
        logger.info(f"Using columns - Period: {period_col}, Event: {event_col}, Loss: {loss_col}")
        
        # YLT DataFrame in IFM format
        ylt = pd.DataFrame()
        ylt['intYear'] = df[period_col]
        ylt['dblLoss'] = df[loss_col]
        ylt['CAT'] = 'CAT'
        ylt['zero'] = 0
        ylt['rate'] = 1
        ylt['intEvent'] = df[event_col]
        

        
        #  to string, remove trailing comma
        output = io.StringIO()
        ylt.to_csv(output, index=False, header=False)
        csv_string = output.getvalue()
        lines = csv_string.splitlines()
        if len(lines) > 0 and lines[0] == ",,,,,":
            lines[0] = ""
        
        # clean string
        return pd.read_csv(io.StringIO("\n".join(lines)), header=None, names=ylt.columns)
    
    except Exception as e:
        logger.error(f"Error converting CSV PLT to YLT: {e}")
        raise

def convert_sql_plt_to_ylt(engine, database, server, anlsid=None, perspcode=None):

    schemas_to_try = ['plt', 'dbo']
    df = None
    successful_query = None
    
    for schema in schemas_to_try:
        try:
            if server == 'DATABRIDGE':
                query = f"SELECT * FROM [{database}].[plt].[rdm_port]"
            else:
                query = f"SELECT * FROM [{database}].[{schema}].[rdm_port]"

            conditions = []
            
            if anlsid and str(anlsid).strip():
                conditions.append(f"ANLSID = {anlsid}")
            
            if perspcode and perspcode.strip():
                conditions.append(f"PERSPCODE = '{perspcode}'")
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            logger.info(f"Attempting to execute query with schema '{schema}': {query}")
            
            chunks = []
            for chunk in pd.read_sql_query(text(query), engine, chunksize=250000):
                chunks.append(chunk)
            
            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
            
            successful_query = query
            logger.info(f"Successfully executed query using schema '{schema}'.")
            break 

        except ProgrammingError as e:

            if 'invalid object name' in str(e).lower():
                logger.warning(f"Table not found in schema '{schema}'. Trying next schema...")
                continue 
            else:
                logger.error(f"An unexpected SQL error occurred with schema '{schema}': {e}")
                raise
        except Exception as e:
            logger.error(f"A non-SQL error occurred while querying schema '{schema}': {e}")
            raise
            
    if df is None:
        raise ValueError(f"Could not find the 'rdm_port' table in any of the attempted schemas: {schemas_to_try}")

    
    if df.empty:
        raise ValueError(f"Query returned no data. Check your parameters (ANLSID, PERSPCODE) and table contents. Query: {successful_query}")
    
    logger.info(f"Retrieved {len(df)} rows from database")
    logger.info(f"Columns found: {df.columns.tolist()}")
    
    ylt = pd.DataFrame()
    
    # Map columns 
    df_columns_lower = {col.lower(): col for col in df.columns}
    
    # Find required columns
    period_col, event_col, loss_col, eventdate_col = None, None, None, None
    
    for pattern in ['periodid', 'period_id', 'period']:
        if pattern in df_columns_lower:
            period_col = df_columns_lower[pattern]
            break
    
    for pattern in ['eventid', 'event_id', 'event']:
        if pattern in df_columns_lower:
            event_col = df_columns_lower[pattern]
            break
    
    for pattern in ['loss', 'losses']:
        if pattern in df_columns_lower:
            loss_col = df_columns_lower[pattern]
            break
            
    for pattern in ['eventdate', 'event_date']:
        if pattern in df_columns_lower:
            eventdate_col = df_columns_lower[pattern]
            break
    
    if not all([period_col, event_col, loss_col]):
        logger.error(f"Required columns not found. Available columns: {df.columns.tolist()}")
        raise ValueError(f"Required columns (periodID, eventID, loss) not found in table")
    
    # Create YLT structure
    ylt['intYear'] = df[period_col]
    ylt['Loss'] = df[loss_col]
    ylt['LossType'] = 'CAT'
    ylt['SD'] = 0
    
    if eventdate_col:
        df[eventdate_col] = pd.to_datetime(df[eventdate_col])
        year_start = df[eventdate_col].apply(lambda x: datetime(x.year, 1, 1))
        ylt['Day'] = ((df[eventdate_col] - year_start).dt.days / 365.0).round(6)
    else:
        ylt['Day'] = 1
    
    ylt['eventid'] = df[event_col]
    
    # Convert to IFM format
    ylt_ifm = pd.DataFrame()
    ylt_ifm['intYear'] = ylt['intYear']
    ylt_ifm['dblLoss'] = ylt['Loss']
    ylt_ifm['CAT'] = ylt['LossType']
    ylt_ifm['zero'] = ylt['SD']
    ylt_ifm['rate'] = ylt['Day']
    ylt_ifm['intEvent'] = ylt['eventid']
    
    return ylt_ifm

def get_credentials_for_server(server):
    if server == 'DATABRIDGE' and 'databridge_credentials' in session:
        logger.info("Using DATABRIDGE specific credentials.")
        creds = session.get('databridge_credentials', {})
        return creds.get('username'), creds.get('password'), None
    else:
        logger.info("Using standard credentials.")
        creds = session.get('credentials', {})
        return creds.get('username'), creds.get('password'), creds.get('domain')


@app.route('/')
def index():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.json
        session['credentials'] = {
            'username': data.get('username'),
            'password': data.get('password'),
            'domain': data.get('domain')
        }
        if data.get('use_databridge_creds'):
            session['databridge_credentials'] = {
                'username': data.get('databridge_username'),
                'password': data.get('databridge_password')
            }
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/dashboard')
def dashboard():
    if 'credentials' not in session:
        return redirect(url_for('index'))
    return render_template('dashboard.html', edm_servers=EDM_SERVERS)

@app.route('/convert_sql', methods=['POST'])
def convert_sql():
    try:
        data = request.json
        creds = session.get('credentials', {})
        
        #  connection parameters
        server = data.get('server')
        database = data.get('database')
        anlsid = data.get('anlsid')
        perspcode = data.get('perspcode')
        
        if not all([server, database]):
            return jsonify({'error': 'Server and Database are required'}), 400
        
        #  credentials
        username, password, domain = get_credentials_for_server(server)
        
        if not username or not password:
            return jsonify({'error': 'Missing credentials. Please login again.'}), 401
        
        #  engine 
        engine = get_engine(server, database, username, password, domain)
        ylt_df = convert_sql_plt_to_ylt(engine, database, server, anlsid, perspcode)
        
        # calculate AAL dynamically 
        numeric_rows = ylt_df[pd.to_numeric(ylt_df['intYear'], errors='coerce').notna()].copy()
        
        #  metadata header
        name = 'N/A'
        curr = 'N/A'
        if anlsid:
            anlsids_dict = session.get('anlsids', {})
            anlsid_info = anlsids_dict.get(str(anlsid))
            if anlsid_info:
                name = anlsid_info.get('name', 'N/A')
                curr = anlsid_info.get('curr', 'N/A')

        metadata_lines = [
            f"// Server: {server}",
            f"// Database: {database}",
            f"// Analysis ID: {anlsid or 'All'}",
            f"// Name: {name if anlsid else 'All'}",
            f"// Currency: {curr if anlsid else 'All'}"
        ]
        
        if len(numeric_rows) > 0:
            #  columns are numeric 
            numeric_rows['dblLoss'] = pd.to_numeric(numeric_rows['dblLoss'], errors='coerce')
            numeric_rows['intYear'] = pd.to_numeric(numeric_rows['intYear'], errors='coerce')
            
            total_loss = numeric_rows['dblLoss'].sum()
            
            # calculate AAL based on the number of simulation years (max period)
            num_years = numeric_rows['intYear'].max()
            aal = total_loss / num_years if num_years > 0 else 0
        else:
            aal = 0
        
        #  to CSV string
        output = io.StringIO()
        ylt_df.to_csv(output, index=False, header=False)
        data_csv_content = output.getvalue()

        #  prepend metadata 
        metadata_header = "\n".join(metadata_lines) + "\n"
        csv_content = metadata_header + data_csv_content
        
        #  filename
        filename_parts = ['YLT']
        if anlsid:
            filename_parts.append(f'ANLSID{anlsid}')
        if perspcode:
            filename_parts.append(perspcode)
        filename_parts.append('IFM.csv')
        output_filename = '_'.join(filename_parts)
        
        return jsonify({
            'success': True,
            'filename': output_filename,
            'data': csv_content,
            'rows': len(numeric_rows),
            'aal': aal,
            'query_info': f"Database: {database}, ANLSID: {anlsid or 'All'}, Name: {name if anlsid else 'All'}, Currency: {curr if anlsid else 'All'}, PERSPCODE: {perspcode or 'All'}"
        })
        
    except Exception as e:
        logger.error(f"SQL conversion error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/convert_batch', methods=['POST'])
def convert_batch():
    try:
        jobs = request.json.get('jobs', [])
        if not jobs:
            return jsonify({'error': 'No batch jobs provided'}), 400

        zip_buffer = io.BytesIO()
        summaries = []

        with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED) as zip_file:
            for job in jobs:
                server = job.get('server')
                database = job.get('database')
                anlsid = job.get('anlsid')
                perspcode = job.get('perspcode')

                if not all([server, database]):
                    logger.warning(f"Skipping invalid batch job: {job}")
                    continue

                output_filename = "error.txt"
                try:
                    #  credentials and engine
                    username, password, domain = get_credentials_for_server(server)
                    if not username or not password:
                        raise Exception(f"Missing credentials for server {server}")
                    
                    engine = get_engine(server, database, username, password, domain)

                    # convert to YLT
                    ylt_df = convert_sql_plt_to_ylt(engine, database, server, anlsid, perspcode)

                    # calculate stats
                    numeric_rows = ylt_df[pd.to_numeric(ylt_df['intYear'], errors='coerce').notna()].copy()
                    aal = 0
                    if len(numeric_rows) > 0:
                        numeric_rows['dblLoss'] = pd.to_numeric(numeric_rows['dblLoss'], errors='coerce')
                        numeric_rows['intYear'] = pd.to_numeric(numeric_rows['intYear'], errors='coerce')
                        total_loss = numeric_rows['dblLoss'].sum()
                        num_years = numeric_rows['intYear'].max()
                        aal = total_loss / num_years if num_years > 0 else 0

                    # add metadata
                    name = 'N/A'
                    curr = 'N/A'
                    if anlsid:
                        anlsids_dict = session.get('anlsids', {})
                        anlsid_info = anlsids_dict.get(str(anlsid))
                        if anlsid_info:
                            name = anlsid_info.get('name', 'N/A')
                            curr = anlsid_info.get('curr', 'N/A')
                    
                    metadata_lines = [
                        f"\\\\ Server: {server}",
                        f"\\\\ Database: {database}",
                        f"\\\\ Analysis ID: {anlsid or 'All'}",
                        f"\\\\ Name: {name if anlsid else 'All'}",
                        f"\\\\ Currency: {curr if anlsid else 'All'}"
                    ]

                    #  CSV content
                    output = io.StringIO()
                    ylt_df.to_csv(output, index=False, header=False)
                    data_csv_content = output.getvalue()
                    
                    metadata_header = "\n".join(metadata_lines) + "\n"
                    csv_content = metadata_header + data_csv_content

                    #  filename
                    filename_parts = ['YLT']
                    if anlsid:
                        filename_parts.append(f'ANLSID{anlsid}')
                    if perspcode:
                        filename_parts.append(perspcode)
                    filename_parts.append(f'{database}_IFM.csv')
                    output_filename = '_'.join(filter(None, filename_parts))

                    # add file to zip
                    zip_file.writestr(output_filename, csv_content)
                    logger.info(f"Added {output_filename} to batch zip.")

                    # add  summary
                    summaries.append({
                        'filename': output_filename,
                        'rows': len(numeric_rows),
                        'aal': aal,
                        'query_info': f"DB: {database}, ANLSID: {anlsid or 'All'}, PERSPCODE: {perspcode or 'All'}"
                    })

                except Exception as e:
                    logger.error(f"Failed to process batch job {job}: {e}", exc_info=True)
                    error_filename = f"ERROR_ANLSID{anlsid or 'All'}_{database}.txt"
                    error_content = f"Failed to process job for:\nServer: {server}\nDatabase: {database}\nANLSID: {anlsid or 'All'}\nPERSPCODE: {perspcode or 'All'}\n\nError: {str(e)}"
                    zip_file.writestr(error_filename, error_content)
                    summaries.append({
                        'filename': error_filename,
                        'error': str(e)
                    })
        
        zip_buffer.seek(0)
        zip_base64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')

        return jsonify({
            'success': True,
            'summaries': summaries,
            'zip_data': zip_base64
        })

    except Exception as e:
        logger.error(f"Batch conversion error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/get_databases')
def get_databases():
    server = request.args.get('server')
    logger.info(f"Received request for databases from server: {server}")
    
    if not server:
        logger.error("No server provided in request")
        return Response('<option value="">Please select a server</option>', mimetype='text/html')
    
    try:
        username, password, domain = get_credentials_for_server(server)
        if not username or not password:
            logger.error("No database credentials found in session for the selected server type")
            return Response('<option value="">Authentication error: No credentials</option>', mimetype='text/html', status=401)
        
        # Connect and get DBs
        engine = get_engine(server, 'master', username, password, domain)
        if engine is None:
            raise Exception("Failed to get database engine for server discovery.")

        with engine.connect() as conn:
            result = conn.execute(text("SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name"))
            databases = [row[0] for row in result]
        
        options = ['<option value="">-- Select Database --</option>']
        options.extend([f'<option value="{db}">{db}</option>' for db in databases])
        
        logger.info(f"Successfully fetched {len(databases)} databases")
        return Response('\n'.join(options), mimetype='text/html')
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Database error in get_databases: {error_msg}", exc_info=True)
        
        if "Login failed for user" in error_msg:
            return Response('<option value="">Authentication failed</option>', mimetype='text/html', status=401)
        elif "connection failed" in error_msg or "Unable to connect" in error_msg:
            return Response('<option value="">Connection failed</option>', mimetype='text/html', status=503)
        else:
            return Response(f'<option value="">Error fetching databases</option>', mimetype='text/html', status=500)

@app.route('/get_anlsids')
def get_anlsids():
    server = request.args.get('server')
    database = request.args.get('database')
    
    if not server or not database:
        return Response('<option value="">Select server and database</option>', mimetype='text/html')
    
    try:
        username, password, domain = get_credentials_for_server(server)
        if not username or not password:
             return Response('<option value="">Authentication error</option>', mimetype='text/html', status=401)

        engine = get_engine(server, database, username, password, domain)
        
        anlsids = None

        with engine.connect() as conn:
            try:
                query = text(f"SELECT DISTINCT ID, NAME, CURR, PERIL FROM [{database}].[dbo].[rdm_analysis] ORDER BY ID")
                result = conn.execute(query)
                anlsids = [(row[0], row[1], row[2], row[3]) for row in result]
                logger.info("Found ANLSIDs with full details from 'dbo.rdm_analysis'")
            except Exception as e:
                logger.warning(f"Could not get full details from 'rdm_analysis': {e}. Falling back to ANLSID from rdm_port.")
                
        
        if anlsids is None or not anlsids:
            return Response('<option value="">No ANLSIDs found</option>', mimetype='text/html')

        options = ['<option value="">-- All ANLSIDs (optional) --</option>']
        
        # Store in session 
        anlsids_dict = {str(id): {'name': name, 'curr': curr, 'peril': peril} for id, name, curr, peril in anlsids}
        session['anlsids'] = anlsids_dict

        options.extend([f'<option value="{id}" data-curr="{curr}" data-peril="{peril}">{id}    [{name}]</option>' for id, name, curr, peril in anlsids])

        return Response('\n'.join(options), mimetype='text/html')

    except Exception as e:
        logger.error(f"Error fetching ANLSIDs: {e}")
        return Response(f'<option value="">Error loading ANLSIDs</option>', status=500, mimetype='text/html')

@app.route('/get_perspcodes')
def get_perspcodes():
    server = request.args.get('server')
    database = request.args.get('database')
    anlsid = request.args.get('anlsid')

    if not all([server, database, anlsid]):
        return Response('<option value="">-- All PERSPCODEs (optional) --</option>', mimetype='text/html')

    try:
        username, password, domain = get_credentials_for_server(server)
        if not username or not password:
             return Response('<option value="">Authentication error</option>', mimetype='text/html', status=401)

        engine = get_engine(server, database, username, password, domain)
        
        perspcodes = None
        schemas_to_try = ['dbo', 'plt']

        with engine.connect() as conn:
            for schema in schemas_to_try:
                try:
                    if server == 'DATABRIDGE':
                        query = text(f"SELECT DISTINCT PERSPCODE FROM [{database}].[plt].[rdm_port] WHERE ANLSID = :anlsid ORDER BY PERSPCODE")
                    else:
                        query = text(f"SELECT DISTINCT PERSPCODE FROM [{database}].[{schema}].[rdm_anlspersp] WHERE ANLSID = :anlsid ORDER BY PERSPCODE")
                    result = conn.execute(query, {'anlsid': anlsid})
                    perspcodes = [row[0] for row in result]
                    logger.info(f"Found PERSPCODEs in schema '{schema}' for ANLSID {anlsid}")
                    break
                except ProgrammingError as e:
                    if 'invalid object name' in str(e).lower():
                        logger.warning(f"Table 'rdm_anlspersp' not found in schema '{schema}'. Trying next.")
                        continue
                    else:
                        raise

        if perspcodes is None:
             return Response('<option value="">No PERSPCODEs found</option>', mimetype='text/html')

        options = ['<option value="">-- All PERSPCODEs (optional) --</option>']
        options.extend([f'<option value="{p}">{p}</option>' for p in perspcodes])
        return Response('\n'.join(options), mimetype='text/html')

    except Exception as e:
        logger.error(f"Error fetching PERSPCODEs: {e}")
        return Response(f'<option value="">Error loading PERSPCODEs</option>', status=500, mimetype='text/html')


@app.route('/convert_csv', methods=['POST'])
def convert_csv():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Please upload a CSV file'}), 400
        
        logger.info(f"Processing file: {file.filename}")
        
        #  read CSV file
        df = pd.read_csv(file)
        logger.info(f"CSV loaded with shape: {df.shape}, columns: {df.columns.tolist()}")
        
        # Convert to YLT
        ylt_df = convert_csv_plt_to_ylt(df)
        
        # calculate AAL
        numeric_rows = ylt_df[pd.to_numeric(ylt_df.iloc[:, 0], errors='coerce').notna()].copy()
        if len(numeric_rows) > 0:
            numeric_rows.columns = ['intYear', 'dblLoss', 'CAT', 'zero', 'rate', 'intEvent']
            numeric_rows['dblLoss'] = pd.to_numeric(numeric_rows['dblLoss'], errors='coerce')
            numeric_rows['intYear'] = pd.to_numeric(numeric_rows['intYear'], errors='coerce')
            max_year = numeric_rows['intYear'].max()
            total_loss = numeric_rows['dblLoss'].sum()
            aal = total_loss / max_year if max_year > 0 else 0
        else:
            aal = 0
        
        #  to CSV string
        output = io.StringIO()
        ylt_df.to_csv(output, index=False, header=False)
        csv_content = output.getvalue()
        
        # filename
        output_filename = file.filename.replace('PLT', 'YLT').replace('.csv', '_IFM.csv')
        if 'YLT' not in output_filename:
            output_filename = output_filename.replace('.csv', '_YLT_IFM.csv')
        
        return jsonify({
            'success': True,
            'filename': output_filename,
            'data': csv_content,
            'rows': len(numeric_rows),
            'aal': aal
        })
        
    except Exception as e:
        logger.error(f"CSV conversion error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0',port=5000)