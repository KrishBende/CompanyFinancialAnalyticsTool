from data_processor import FinancialDataProcessor
from nlp_parser import NLPQueryParser
from visualizer import DataVisualizer
from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg, max as spark_max, min as spark_min
import sys

class QueryEngine:
    """
    The main query engine that orchestrates parsing, data processing, and visualization.
    """
    
    def __init__(self, data_file_path):
        """
        Initializes the query engine with the path to the data file.
        
        Args:
            data_file_path (str): Path to the 'superstore.csv' file.
        """
        self.data_file_path = data_file_path
        self.processor = FinancialDataProcessor(self.data_file_path)
        self.parser = NLPQueryParser()
        self.visualizer = None
        self.spark = None
        self.df = None
        
        # Load data
        self._load_data()
        
    def _load_data(self):
        """
        Loads the data using the processor.
        """
        print("Loading financial data...")
        self.processor.load_data()
        self.spark = self.processor.get_spark_session()
        self.df = self.processor.get_dataframe()
        self.visualizer = DataVisualizer(self.spark)
        print("Data loaded and ready for queries.")
        
    def execute_query(self, query):
        """
        Executes a natural language query.
        
        Args:
            query (str): The user's natural language query.
        """
        print(f"\n--- Executing Query: {query} ---")
        
        # Parse the query
        parsed_query = self.parser.parse_query(query)
        print(f"Parsed query: {parsed_query}")
        
        # Handle different actions
        action = parsed_query.get('action')
        metric = parsed_query.get('metric')
        dimension = parsed_query.get('dimension')
        filters = parsed_query.get('filters', {})
        time_range = parsed_query.get('time_range')
        
        if not action or not metric:
            print("Could not understand the query. Please try rephrasing.")
            return
            
        # Apply filters to the dataframe
        filtered_df = self.df
        if 'region' in filters:
            filtered_df = filtered_df.filter(col("Region") == filters['region'])
        if 'category' in filters:
            filtered_df = filtered_df.filter(col("Category") == filters['category'])
        if time_range:
            if 'year' in time_range:
                filtered_df = filtered_df.filter(col("Year") == time_range['year'])
            elif 'start_year' in time_range and 'end_year' in time_range:
                filtered_df = filtered_df.filter(
                    (col("Year") >= time_range['start_year']) & 
                    (col("Year") <= time_range['end_year'])
                )
                
        # Map metric names to column names
        metric_column_map = {
            'sales': 'Sales',
            'profit': 'Profit',
            'quantity': 'Quantity',
            'discount': 'Discount'
        }
        
        actual_metric_column = metric_column_map.get(metric)
        if not actual_metric_column:
            print(f"Unknown metric: {metric}")
            return
            
        # Map action to operation
        if action in ['show', 'total']:
            # Aggregate by dimension if specified, otherwise overall total
            if dimension:
                dimension_column_map = {
                    'category': 'Category',
                    'region': 'Region',
                    'segment': 'Segment',
                    'time': 'Year'
                }
                actual_dimension_column = dimension_column_map.get(dimension)
                if not actual_dimension_column:
                    print(f"Unknown dimension: {dimension}")
                    return
                    
                result_df = filtered_df.groupBy(actual_dimension_column).agg(
                    spark_sum(actual_metric_column).alias(f"Total{actual_metric_column.capitalize()}")
                )
                result_df.show()
                
                # If action is 'plot', create a visualization
                if action == 'plot':
                    title = f"Total {actual_metric_column.capitalize()} by {actual_dimension_column}"
                    xlabel = actual_dimension_column
                    ylabel = f"Total {actual_metric_column.capitalize()}"
                    
                    if dimension == 'time':
                        # Line chart for time series
                        filename = self.visualizer.plot_line_chart(
                            result_df, 
                            actual_dimension_column, 
                            f"Total{actual_metric_column.capitalize()}", 
                            title, 
                            xlabel, 
                            ylabel
                        )
                    else:
                        # Bar chart for categories/regions/segments
                        filename = self.visualizer.plot_bar_chart(
                            result_df, 
                            actual_dimension_column, 
                            f"Total{actual_metric_column.capitalize()}", 
                            title, 
                            xlabel, 
                            ylabel
                        )
                        
                    print(f"[Visualization] Chart saved as: {filename}")
                    
            else:
                # Overall total
                result = filtered_df.agg(spark_sum(actual_metric_column).alias(f"Total{actual_metric_column.capitalize()}")).collect()[0]
                print(f"Total {actual_metric_column.capitalize()}: {result[f'Total{actual_metric_column.capitalize()}']}")
                
        elif action == 'average':
            # Average by dimension if specified, otherwise overall average
            if dimension:
                dimension_column_map = {
                    'category': 'Category',
                    'region': 'Region',
                    'segment': 'Segment',
                    'time': 'Year'
                }
                actual_dimension_column = dimension_column_map.get(dimension)
                if not actual_dimension_column:
                    print(f"Unknown dimension: {dimension}")
                    return
                    
                result_df = filtered_df.groupBy(actual_dimension_column).agg(
                    spark_avg(actual_metric_column).alias(f"Average{actual_metric_column.capitalize()}")
                )
                result_df.show()
            else:
                # Overall average
                result = filtered_df.agg(spark_avg(actual_metric_column).alias(f"Average{actual_metric_column.capitalize()}")).collect()[0]
                print(f"Average {actual_metric_column.capitalize()}: {result[f'Average{actual_metric_column.capitalize()}']}")
                
        elif action == 'plot':
            # This is handled in the 'show'/'total' section when dimension is specified
            # If we get here, it means plot was specified without a clear metric or dimension
            # We can default to plotting a simple bar chart of top categories by sales
            if metric and not dimension:
                result_df = filtered_df.groupBy("Category").agg(
                    spark_sum(actual_metric_column).alias(f"Total{actual_metric_column.capitalize()}")
                ).orderBy(col(f"Total{actual_metric_column.capitalize()}").desc()).limit(10)
                
                title = f"Top 10 Categories by {actual_metric_column.capitalize()}"
                xlabel = "Category"
                ylabel = f"Total {actual_metric_column.capitalize()}"
                
                filename = self.visualizer.plot_bar_chart(
                    result_df, 
                    "Category", 
                    f"Total{actual_metric_column.capitalize()}", 
                    title, 
                    xlabel, 
                    ylabel
                )
                
                print(f"[Visualization] Chart saved as: {filename}")
                
        elif action == 'trend':
            # Trend over time
            if metric:
                result_df = filtered_df.groupBy("Year").agg(
                    spark_sum(actual_metric_column).alias(f"Total{actual_metric_column.capitalize()}")
                ).orderBy("Year")
                
                result_df.show()
                
                # Create line chart
                title = f"{actual_metric_column.capitalize()} Trend Over Time"
                xlabel = "Year"
                ylabel = f"Total {actual_metric_column.capitalize()}"
                
                filename = self.visualizer.plot_line_chart(
                    result_df, 
                    "Year", 
                    f"Total{actual_metric_column.capitalize()}", 
                    title, 
                    xlabel, 
                    ylabel
                )
                
                print(f"[Visualization] Chart saved as: {filename}")
                
        else:
            print(f"Action '{action}' not yet implemented.")
            
    def interactive_session(self):
        """
        Starts an interactive session to accept queries from the user.
        """
        print("\n--- Financial Data Analysis Engine ---")
        print("Enter your queries in natural language or type 'exit' to quit.")
        print("Example queries:")
        print("  - Show me total sales by category")
        print("  - Plot the trend of profit over time")
        print("  - What is the average discount by segment?")
        print("  - Show me total sales in the West region for 2012")
        print("---------------------------------------")
        
        while True:
            try:
                query = input("\nEnter your query: ").strip()
                if query.lower() in ['exit', 'quit']:
                    print("Goodbye!")
                    break
                if query:
                    self.execute_query(query)
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"An error occurred: {e}")

# Example usage
if __name__ == "__main__":
    # Initialize the query engine
    engine = QueryEngine("superstore.csv")
    
    # If command line arguments are provided, execute them as queries
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        engine.execute_query(query)
    else:
        # Otherwise, start an interactive session
        engine.interactive_session()