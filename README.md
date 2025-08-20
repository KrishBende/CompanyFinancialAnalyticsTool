# Project Summary: Company Internal Financial Analytics Tool

This project implements a Python-based tool for analyzing internal company financial data using natural language queries. It demonstrates skills in data processing with PySpark, natural language processing, data visualization, and machine learning.

## Components Implemented

1.  **Data Acquisition & Setup**:
    *   Set up a virtual environment (`venv`) with all necessary dependencies.
    *   Downloaded and extracted a relevant financial dataset ("Global Superstore Dataset") from Kaggle.

2.  **Efficient Data Processing (PySpark)**:
    *   Created `data_processor.py` with a `FinancialDataProcessor` class.
    *   This class loads the CSV data into a PySpark DataFrame with a defined schema for performance and type safety.
    *   Column names with dots (from the original CSV) are cleaned to use underscores for easier handling in Spark SQL.

3.  **Natural Language Processing (NLP) Interface**:
    *   Created `advanced_nlp_ml.py` with an `AdvancedNLPProcessor` class that uses spaCy for sophisticated natural language understanding.
    *   This processor can identify actions (show, plot, average, trend, predict, compare), metrics (sales, profit, quantity, discount), dimensions (category, region, segment, time), filters, and time ranges.
    *   It leverages spaCy's named entity recognition to identify geopolitical entities (for region filtering) and dates.

4.  **Query Execution Engine**:
    *   Updated `query_engine.py` with a `QueryEngine` class that orchestrates the entire process.
    *   Supports various operations like showing totals, calculating averages, identifying trends, and making predictions.
    *   Applies filters based on parsed query components (region, category, year).
    *   Implements a comparison functionality (simplified).

5.  **Machine Learning Algorithms**:
    *   Created `advanced_nlp_ml.py` with an `MLProcessor` class.
    *   This processor implements a linear regression model for predicting future sales.
    *   It calculates model metrics (RMSE) and provides both historical data and predictions.

6.  **Visualization & Output**:
    *   Created `visualizer.py` with a `DataVisualizer` class.
    *   This class generates bar charts, line charts, and pie charts using Matplotlib and Seaborn.
    *   Visualizations are saved as PNG files in a dedicated `charts/` directory.

7.  **Web Application Interface**:
    *   Created `app.py` with a Flask web application for a user-friendly interface.
    *   Implemented responsive HTML templates with modern CSS styling.
    *   Added client-side JavaScript for enhanced user interactions.
    *   Users can enter natural language queries through a web form and view results with visualizations.

8.  **User Interaction**:
    *   The `QueryEngine` supports both command-line argument queries and an interactive session.
    *   Users can enter natural language queries like "Show me total sales by category" or "Predict sales for next year".
    *   The web interface provides an intuitive way to interact with the analytics engine.

## How to Run

### Command Line Interface

1.  Ensure you have Python 3.x installed.
2.  Create and activate the virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3.  Install dependencies:
    ```bash
    pip install --upgrade pip
    pip install kaggle pyspark pandas matplotlib seaborn scikit-learn spacy flask
    ```
4.  Download the spaCy English language model:
    ```bash
    python -m spacy download en_core_web_sm
    ```
5.  Set up Kaggle API credentials (if you want to download the dataset directly):
    *   Place your `kaggle.json` file in `~/.kaggle/` with appropriate permissions (600).
    *   Or manually download the "Global Superstore Dataset" from Kaggle and place `superstore.csv` in the project directory.
6.  Run a single query:
    ```bash
    python query_engine.py "Predict sales for next year"
    ```
7.  Run in interactive mode:
    ```bash
    python query_engine.py
    ```
    Then enter queries at the prompt.

### Web Application Interface

1.  Ensure you have Python 3.x installed and the virtual environment set up as described above.
2.  Run the Flask web application:
    ```bash
    python app.py
    ```
3.  Open your web browser and navigate to:
    ```
    http://localhost:5000
    ```

## Example Queries

*   `Show me total sales by category`
*   `Plot the trend of profit over time`
*   `What is the average discount by segment?`
*   `Show me total sales in the West region for 2012`
*   `What was the profit trend from 2011 to 2013?`
*   `Predict sales for next year`
*   `Compare sales in the United States`

## Future Enhancements

*   Integrate more sophisticated NLP libraries like Hugging Face `transformers` for even better query understanding.
*   Implement more machine learning algorithms using `scikit-learn` for different types of predictions and classifications.
*   Add deep learning models using `PyTorch` or `TensorFlow` for more complex pattern recognition tasks.
*   Enhance the web-based UI with more advanced features like dashboards and user accounts.
*   Enhance the comparison functionality to handle more complex comparison queries.
*   Implement anomaly detection algorithms to identify unusual patterns in the financial data.
*   Add export functionality for reports and charts.
*   Implement real-time data updates and streaming analytics.