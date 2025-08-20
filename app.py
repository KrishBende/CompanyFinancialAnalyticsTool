from flask import Flask, render_template, request, send_from_directory, jsonify
import os
import sys
import uuid
import time

# Add the current directory to the Python path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from query_engine import QueryEngine

# Create Flask app with static folder configuration
app = Flask(__name__, 
            static_url_path='/static',
            static_folder='static')

# Create a directory for charts if it doesn't exist
CHARTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'charts')
if not os.path.exists(CHARTS_DIR):
    os.makedirs(CHARTS_DIR)

# Initialize the query engine with the charts directory
query_engine = QueryEngine("superstore.csv", CHARTS_DIR)

# Store for query results (in a real app, you'd use a database)
query_results = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def process_query():
    query = request.form['query']
    if query:
        # Record the time before executing the query
        start_time = time.time()
        
        # Execute the query and capture output
        import io
        import contextlib
        
        # Capture stdout to get print statements from the query engine
        captured_output = io.StringIO()
        with contextlib.redirect_stdout(captured_output):
            query_engine.execute_query(query)
        
        output = captured_output.getvalue()
        
        # Get the list of generated chart files (only new ones)
        chart_files = []
        
        # Check for PNG files in the charts directory that were created after start_time
        for f in os.listdir(CHARTS_DIR):
            if f.endswith('.png'):
                file_path = os.path.join(CHARTS_DIR, f)
                if os.path.getmtime(file_path) > start_time:
                    chart_files.append(f)
        
        # If no new files found, check for any recent files
        if not chart_files:
            for f in os.listdir(CHARTS_DIR):
                if f.endswith('.png'):
                    file_path = os.path.join(CHARTS_DIR, f)
                    # Files created in last 2 minutes
                    if os.path.getmtime(file_path) > (time.time() - 120):
                        chart_files.append(f)
        
        # Generate a unique ID for this query
        query_id = str(uuid.uuid4())
        
        # Store results
        query_results[query_id] = {
            'query': query,
            'output': output,
            'chart_files': chart_files
        }
        
        return render_template('results.html', query=query, query_id=query_id, chart_files=chart_files, output=output)
    
    return render_template('index.html', error="Please enter a query")

@app.route('/charts/<filename>')
def serve_chart(filename):
    return send_from_directory(CHARTS_DIR, filename)

@app.route('/api/results/<query_id>')
def get_results(query_id):
    if query_id in query_results:
        return jsonify(query_results[query_id])
    else:
        return jsonify({'error': 'Query not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)