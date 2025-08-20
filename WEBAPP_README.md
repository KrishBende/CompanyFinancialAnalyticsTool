# Company Internal Financial Analytics Web Application

This is a web-based interface for the Company Internal Financial Analytics tool, which allows users to analyze financial data using natural language queries.

## Features

- Natural language processing for financial queries
- Data visualization with automatic chart generation
- Predictive analytics using machine learning
- Responsive web interface with modern UI design
- Example queries for quick start

## Prerequisites

Before running the web application, ensure you have:

1. Python 3.7 or higher
2. All dependencies installed (see main project README)
3. The virtual environment set up and activated

## Running the Web Application

1. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Run the Flask application:
   ```bash
   python app.py
   ```

3. Open your web browser and navigate to:
   ```
   http://localhost:5000
   ```

## Using the Application

1. Enter a natural language query in the input field (e.g., "Show me total sales by category")
2. Click "Analyze Data" or press Enter
3. View the results, including any generated charts
4. Use the "Back to Query Interface" link to return to the main page

### Example Queries

- "Show me total sales by category"
- "Plot the trend of profit over time"
- "What is the average discount by segment?"
- "Show me total sales in the West region for 2012"
- "Predict sales for next year"

## Project Structure

```
CompanyInternalFinancials/
├── app.py                 # Flask web application
├── query_engine.py        # Main query processing engine
├── data_processor.py      # Data loading and processing
├── visualizer.py          # Data visualization
├── advanced_nlp_ml.py     # NLP and ML processing
├── templates/             # HTML templates
│   ├── index.html         # Main page
│   └── results.html       # Results page
├── static/                # Static assets
│   ├── style.css          # Main stylesheet
│   ├── app.css            # JavaScript-enhanced styles
│   └── app.js             # Client-side JavaScript
├── charts/                # Generated charts (created at runtime)
└── venv/                  # Python virtual environment
```

## How It Works

1. The web application uses Flask to serve HTML pages and handle user requests
2. When a query is submitted, it's processed by the `QueryEngine`
3. The `QueryEngine` uses NLP to parse the query and determine what action to take
4. Data is processed using PySpark for efficient handling of large datasets
5. Results are visualized using Matplotlib and Seaborn
6. Charts are saved to the `charts/` directory and served to the user

## Troubleshooting

### Charts Not Displaying

If charts are not displaying:

1. Check that the `charts/` directory exists and is writable
2. Ensure matplotlib and seaborn are properly installed
3. Verify that the query actually requests a visualization (contains "plot", "chart", etc.)

### Common Issues

- **Module not found errors**: Make sure the virtual environment is activated
- **Permission errors**: Ensure the application has write permissions to the `charts/` directory
- **Slow performance**: Large datasets may take time to process; be patient during analysis

## Customization

You can customize the web interface by modifying:

- `templates/index.html` - Main page layout and content
- `templates/results.html` - Results page layout and content
- `static/style.css` - Main styling
- `static/app.js` - Client-side JavaScript interactions

## Future Enhancements

- Add user authentication and personalized dashboards
- Implement query history and favorites
- Add more advanced visualization options
- Include real-time data updates
- Add export functionality for reports and charts