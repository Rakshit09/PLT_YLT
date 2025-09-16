import os
import io
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for, Response
import pandas as pd
import numpy as np
import sqlalchemy as sa
from sqlalchemy import create_engine, text, URL
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-here-change-in-production')
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024 * 1024  # 5GB max file size
app.config['SESSION_TYPE'] = 'filesystem'

EDM_SERVERS = ('GREAZUK1DB051P', 'GREAZUK1DB101P', 'GREAZUK1DB181P', 'GREAZUK1DB201P', 'GREAZUK1DB251P', '103db9bcc5307a1d669c5f0946a36dfc.databridge.rms-pe.com,1333')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_engine(server: str, database: str, username: str, password: str, domain: str = None):
    """Create SQL Server connection engine"""
    try:
        # Handle domain authentication
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
            query={"timeout": "0", "login_timeout": "300", "charset": "utf8"}
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
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info(f"Successfully created engine for {server}/{database}")
        return engine
        
    except Exception as exc:
        logger.error(f"Failed to create engine: {exc}")
        raise

def convert_csv_plt_to_ylt(df):
    """Convert PLT DataFrame from CSV to YLT IFM format"""
    try:
        # Ensure we have the required columns (case-insensitive search)
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        period_col = None
        event_col = None
        loss_col = None
        
        # Find period column
        for pattern in ['periodid', 'period_id', 'period']:
            if pattern in df_columns_lower:
                period_col = df_columns_lower[pattern]
                break
        
        # Find event column
        for pattern in ['eventid', 'event_id', 'event']:
            if pattern in df_columns_lower:
                event_col = df_columns_lower[pattern]
                break
        
        # Find loss column
        for pattern in ['loss', 'losses', 'ground_up_loss']:
            if pattern in df_columns_lower:
                loss_col = df_columns_lower[pattern]
                break
        
        if not all([period_col, event_col, loss_col]):
            # Try positional if named columns not found
            if len(df.columns) >= 3:
                period_col = df.columns[0]
                event_col = df.columns[1]
                loss_col = df.columns[2]
                logger.warning(f"Using positional columns: {period_col}, {event_col}, {loss_col}")
            else:
                raise ValueError(f"Cannot identify required columns. Found columns: {df.columns.tolist()}")
        
        logger.info(f"Using columns - Period: {period_col}, Event: {event_col}, Loss: {loss_col}")
        
        # Create YLT DataFrame in IFM format
        ylt = pd.DataFrame()
        ylt['intYear'] = df[period_col]
        ylt['dblLoss'] = df[loss_col]
        ylt['CAT'] = 'CAT'
        ylt['zero'] = 0
        ylt['rate'] = 1
        ylt['intEvent'] = df[event_col]
        
        # Add escape-delay header
        header_row = pd.DataFrame({
            'intYear': ['// escape-delay'],
            'dblLoss': [''],
            'CAT': [''],
            'zero': [''],
            'rate': [''],
            'intEvent': ['']
        })
        
        ylt = pd.concat([header_row, ylt], ignore_index=True)
        
        return ylt
    
    except Exception as e:
        logger.error(f"Error converting CSV PLT to YLT: {e}")
        raise

def convert_sql_plt_to_ylt(engine, database, anlsid=None, perspcode=None):
    """Convert PLT from SQL to YLT IFM format from plt.rdm_port table"""
    try:
        # Build query for plt.rdm_port table
        query = f"SELECT * FROM [{database}].[plt].[rdm_port]"
        conditions = []
        
        if anlsid and anlsid.strip():
            conditions.append(f"ANLSID = {anlsid}")
        
        if perspcode and perspcode.strip():
            conditions.append(f"PERSPCODE = '{perspcode}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        logger.info(f"Executing query: {query}")
        
        # **FIX**: Read data from SQL in chunks to prevent timeouts on large datasets
        chunks = []
        for chunk in pd.read_sql_query(text(query), engine, chunksize=250000):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        
        if df.empty:
            raise ValueError(f"Query returned no data. Check your parameters (ANLSID, PERSPCODE) and table contents. Query: {query}")
        
        logger.info(f"Retrieved {len(df)} rows from database")
        logger.info(f"Columns found: {df.columns.tolist()}")
        
        # Create YLT format based on your SQL script structure
        ylt = pd.DataFrame()
        
        # Map columns - these are the expected columns from RMS PLT
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        # Find the required columns
        period_col = None
        event_col = None
        loss_col = None
        eventdate_col = None
        
        # Common RMS column names
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
        
        # Create YLT structure as per your SQL script
        ylt['intYear'] = df[period_col]
        ylt['Loss'] = df[loss_col]
        ylt['LossType'] = 'CAT'
        ylt['SD'] = 0
        
        # Calculate Day if EventDate exists (as per your SQL script)
        if eventdate_col:
            df[eventdate_col] = pd.to_datetime(df[eventdate_col])
            year_start = df[eventdate_col].apply(lambda x: datetime(x.year, 1, 1))
            ylt['Day'] = ((df[eventdate_col] - year_start).dt.days / 365.0).round(6)
        else:
            ylt['Day'] = 1
        
        ylt['eventid'] = df[event_col]
        
        # Convert to IFM format (matching your R script output)
        ylt_ifm = pd.DataFrame()
        ylt_ifm['intYear'] = ylt['intYear']
        ylt_ifm['dblLoss'] = ylt['Loss']
        ylt_ifm['CAT'] = ylt['LossType']
        ylt_ifm['zero'] = ylt['SD']
        ylt_ifm['rate'] = ylt['Day']
        ylt_ifm['intEvent'] = ylt['eventid']
        
        # Add escape-delay header
        header_row = pd.DataFrame({
            'intYear': ['// escape-delay'],
            'dblLoss': [''],
            'CAT': [''],
            'zero': [''],
            'rate': [''],
            'intEvent': ['']
        })
        
        ylt_ifm = pd.concat([header_row, ylt_ifm], ignore_index=True)
        
        return ylt_ifm
    
    except Exception as e:
        logger.error(f"Error converting SQL PLT to YLT: {e}")
        raise

@app.route('/')
def index():
    return render_template('login.html')

@app.route('/login', methods=['POST'])
def login():
    """Store credentials in session and redirect to dashboard"""
    try:
        data = request.json
        session['credentials'] = {
            'username': data.get('username'),
            'password': data.get('password'),
            'domain': data.get('domain')
        }
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/dashboard')
def dashboard():
    """Main dashboard with SQL and CSV tabs"""
    if 'credentials' not in session:
        return redirect(url_for('index'))
    return render_template('dashboard.html', edm_servers=EDM_SERVERS)

@app.route('/convert_sql', methods=['POST'])
def convert_sql():
    """Convert PLT from SQL Server to YLT"""
    try:
        data = request.json
        creds = session.get('credentials', {})
        
        # Get connection parameters
        server = data.get('server')
        database = data.get('database')
        anlsid = data.get('anlsid')
        perspcode = data.get('perspcode')
        
        if not all([server, database]):
            return jsonify({'error': 'Server and Database are required'}), 400
        
        # Use stored credentials
        username = creds.get('username')
        password = creds.get('password')
        domain = creds.get('domain')
        
        if not username or not password:
            return jsonify({'error': 'Missing credentials. Please login again.'}), 401
        
        # Create engine and convert
        engine = get_engine(server, database, username, password, domain)
        ylt_df = convert_sql_plt_to_ylt(engine, database, anlsid, perspcode)
        
        # **IMPROVEMENT**: Calculate AAL dynamically instead of using a hardcoded value
        numeric_rows = ylt_df[ylt_df['intYear'] != '// escape-delay'].copy()
        if len(numeric_rows) > 0:
            # Ensure columns are numeric for calculations
            numeric_rows['dblLoss'] = pd.to_numeric(numeric_rows['dblLoss'], errors='coerce')
            numeric_rows['intYear'] = pd.to_numeric(numeric_rows['intYear'], errors='coerce')
            
            total_loss = numeric_rows['dblLoss'].sum()
            
            # Calculate AAL based on the number of simulation years (max period)
            num_years = numeric_rows['intYear'].max()
            aal = total_loss / num_years if num_years > 0 else 0
        else:
            aal = 0
        
        # Convert to CSV string
        output = io.StringIO()
        ylt_df.to_csv(output, index=False, header=False)
        csv_content = output.getvalue()
        
        # Generate filename
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
            'query_info': f"Database: {database}, ANLSID: {anlsid or 'All'}, PERSPCODE: {perspcode or 'All'}"
        })
        
    except Exception as e:
        logger.error(f"SQL conversion error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/get_databases')
def get_databases():
    server = request.args.get('server')
    logger.info(f"Received request for databases from server: {server}")
    
    if not server:
        logger.error("No server provided in request")
        return Response('<option value="">Please select a server</option>', mimetype='text/html')
    
    try:
        creds = session.get('credentials')
        if not creds or not creds.get('username') or not creds.get('password'):
            logger.error("No database credentials found in session")
            return Response('<option value="">Authentication error: No credentials</option>', mimetype='text/html', status=401)
        
        # Connect to master db to get list of other DBs
        engine = get_engine(server, 'master', creds.get('username'), creds.get('password'), creds.get('domain'))
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

@app.route('/convert_csv', methods=['POST'])
def convert_csv():
    """Convert uploaded CSV PLT file to YLT"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'Please upload a CSV file'}), 400
        
        logger.info(f"Processing file: {file.filename}")
        
        # Read CSV file
        df = pd.read_csv(file)
        logger.info(f"CSV loaded with shape: {df.shape}, columns: {df.columns.tolist()}")
        
        # Convert to YLT
        ylt_df = convert_csv_plt_to_ylt(df)
        
        # Calculate AAL
        numeric_rows = ylt_df[ylt_df['intYear'] != '// escape-delay'].copy()
        if len(numeric_rows) > 0:
            numeric_rows['dblLoss'] = pd.to_numeric(numeric_rows['dblLoss'], errors='coerce')
            numeric_rows['intYear'] = pd.to_numeric(numeric_rows['intYear'], errors='coerce')
            max_year = numeric_rows['intYear'].max()
            total_loss = numeric_rows['dblLoss'].sum()
            aal = total_loss / max_year if max_year > 0 else 0
        else:
            aal = 0
        
        # Convert to CSV string
        output = io.StringIO()
        ylt_df.to_csv(output, index=False, header=False)
        csv_content = output.getvalue()
        
        # Generate filename
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
    app.run(debug=True, port=5000)