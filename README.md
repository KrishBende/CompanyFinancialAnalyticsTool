# Project Summary: Company Internal Financial Analytics Tool

This project implements a Python-based tool for analyzing internal company financial data using natural language queries. It demonstrates skills in data processing with PySpark, natural language processing, data visualization, and basic machine learning concepts.

## Components Implemented

1.  **Data Acquisition & Setup**:
    *   Set up a virtual environment (`venv`) with all necessary dependencies.
    *   Downloaded and extracted a relevant financial dataset ("Global Superstore Dataset") from Kaggle.

2.  **Efficient Data Processing (PySpark)**:
    *   Created `data_processor.py` with a `FinancialDataProcessor` class.
    *   This class loads the CSV data into a PySpark DataFrame with a defined schema for performance and type safety.
    *   Column names with dots (from the original CSV) are cleaned to use underscores for easier handling in Spark SQL.

3.  **Natural Language Processing (NLP) Interface**:
    *   Created `nlp_parser.py` with an `NLPQueryParser` class.
    *   This parser uses regex and keyword matching to interpret user queries.
    *   It identifies actions (show, plot, average, trend), metrics (sales, profit, quantity, discount), dimensions (category, region, segment, time), filters, and time ranges.

4.  **Query Execution Engine**:
    *   Created `query_engine.py` with a `QueryEngine` class.
    *   This engine orchestrates the entire process: loading data, parsing queries, and executing operations.
    *   It supports various operations like showing totals, calculating averages, and identifying trends.
    *   It applies filters based on parsed query components (region, category, year).

5.  **Visualization & Output**:
    *   Created `visualizer.py` with a `DataVisualizer` class.
    *   This class generates bar charts, line charts, and pie charts using Matplotlib and Seaborn.
    *   Visualizations are created from Spark DataFrames and returned as base64 encoded strings (ready for display in web applications or Jupyter notebooks).

6.  **User Interaction**:
    *   The `QueryEngine` supports both command-line argument queries and an interactive session.
    *   Users can enter natural language queries like "Show me total sales by category" or "Plot the trend of profit over time".

## How to Run

1.  Ensure you have Python 3.x installed.
2.  Create and activate the virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3.  Install dependencies:
    ```bash
    pip install --upgrade pip
    pip install kaggle pyspark pandas matplotlib seaborn scikit-learn
    ```
4.  Set up Kaggle API credentials (if you want to download the dataset directly):
    *   Place your `kaggle.json` file in `~/.kaggle/` with appropriate permissions (600).
    *   Or manually download the "Global Superstore Dataset" from Kaggle and place `superstore.csv` in the project directory.
5.  Run a single query:
    ```bash
    python query_engine.py "Show me total sales by category"
    ```
6.  Run in interactive mode:
    ```bash
    python query_engine.py
    ```
    Then enter queries at the prompt.

## Example Queries

*   `Show me total sales by category`
*   `Plot the trend of profit over time`
*   `What is the average discount by segment?`
*   `Show me total sales in the West region for 2012`
*   `What was the profit trend from 2011 to 2013?`

## Future Enhancements (as per original plan)

*   Integrate more sophisticated NLP libraries like `spaCy` or Hugging Face `transformers` for better query understanding.
*   Implement machine learning algorithms using `scikit-learn` for predictive analytics (e.g., sales forecasting).
*   Add deep learning models using `PyTorch` or `TensorFlow` for more complex pattern recognition tasks.
*   Develop a web-based UI (e.g., with Flask or Streamlit) for a more user-friendly interface.