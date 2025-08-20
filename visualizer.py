import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg, year as spark_year
import io
import base64
import os

class DataVisualizer:
    """
    A class to handle creating visualizations from Spark DataFrames.
    """
    
    def __init__(self, spark_session):
        """
        Initializes the visualizer with a Spark session.
        
        Args:
            spark_session (SparkSession): An active Spark session.
        """
        self.spark = spark_session
        # Set styling for plots
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
    def plot_bar_chart(self, df, x_col, y_col, title="Bar Chart", xlabel=None, ylabel=None):
        """
        Creates a bar chart from a Spark DataFrame and saves it to a file.
        
        Args:
            df (pyspark.sql.DataFrame): The Spark DataFrame containing data.
            x_col (str): The column name for the x-axis.
            y_col (str): The column name for the y-axis.
            title (str): The title of the chart.
            xlabel (str): Label for the x-axis.
            ylabel (str): Label for the y-axis.
            
        Returns:
            str: The filename of the saved plot.
        """
        # Convert to Pandas for plotting
        pandas_df = df.toPandas()
        
        # Create the plot
        plt.figure(figsize=(10, 6))
        ax = sns.barplot(data=pandas_df, x=x_col, y=y_col)
        
        # Set labels and title
        ax.set_title(title, fontsize=16)
        ax.set_xlabel(xlabel or x_col, fontsize=12)
        ax.set_ylabel(ylabel or y_col, fontsize=12)
        
        # Rotate x-axis labels if they are long
        if pandas_df[x_col].dtype == 'object' and pandas_df[x_col].str.len().max() > 10:
            plt.xticks(rotation=45, ha='right')
            
        plt.tight_layout()
        
        # Save to a file
        filename = f"{title.replace(' ', '_').lower()}.png"
        # Ensure filename is filesystem-safe
        filename = "".join(c for c in filename if c.isalnum() or c in "_-.") or "chart.png"
        plt.savefig(filename)
        
        # Clear the figure to free memory
        plt.clf()
        plt.close()
        
        return filename
    
    def plot_line_chart(self, df, x_col, y_col, title="Line Chart", xlabel=None, ylabel=None):
        """
        Creates a line chart from a Spark DataFrame and saves it to a file.
        
        Args:
            df (pyspark.sql.DataFrame): The Spark DataFrame containing data.
            x_col (str): The column name for the x-axis.
            y_col (str): The column name for the y-axis.
            title (str): The title of the chart.
            xlabel (str): Label for the x-axis.
            ylabel (str): Label for the y-axis.
            
        Returns:
            str: The filename of the saved plot.
        """
        # Convert to Pandas for plotting
        pandas_df = df.toPandas()
        
        # Sort by x_col if it's a time column
        if 'year' in x_col.lower() or 'date' in x_col.lower():
            pandas_df = pandas_df.sort_values(by=x_col)
        
        # Create the plot
        plt.figure(figsize=(10, 6))
        ax = sns.lineplot(data=pandas_df, x=x_col, y=y_col, marker='o')
        
        # Set labels and title
        ax.set_title(title, fontsize=16)
        ax.set_xlabel(xlabel or x_col, fontsize=12)
        ax.set_ylabel(ylabel or y_col, fontsize=12)
        
        plt.tight_layout()
        
        # Save to a file
        filename = f"{title.replace(' ', '_').lower()}.png"
        # Ensure filename is filesystem-safe
        filename = "".join(c for c in filename if c.isalnum() or c in "_-.") or "chart.png"
        plt.savefig(filename)
        
        # Clear the figure to free memory
        plt.clf()
        plt.close()
        
        return filename
    
    def plot_pie_chart(self, df, label_col, value_col, title="Pie Chart"):
        """
        Creates a pie chart from a Spark DataFrame and saves it to a file.
        
        Args:
            df (pyspark.sql.DataFrame): The Spark DataFrame containing data.
            label_col (str): The column name for the pie slice labels.
            value_col (str): The column name for the pie slice values.
            title (str): The title of the chart.
            
        Returns:
            str: The filename of the saved plot.
        """
        # Convert to Pandas for plotting
        pandas_df = df.toPandas()
        
        # Create the plot
        plt.figure(figsize=(8, 8))
        plt.pie(pandas_df[value_col], labels=pandas_df[label_col], autopct='%1.1f%%', startangle=140)
        plt.title(title, fontsize=16)
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        
        plt.tight_layout()
        
        # Save to a file
        filename = f"{title.replace(' ', '_').lower()}.png"
        # Ensure filename is filesystem-safe
        filename = "".join(c for c in filename if c.isalnum() or c in "_-.") or "chart.png"
        plt.savefig(filename)
        
        # Clear the figure to free memory
        plt.clf()
        plt.close()
        
        return filename

# Example usage (for testing)
if __name__ == "__main__":
    # This would normally be done in the context of the full application
    # For testing, we'll create a simple Spark session and dummy data
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    
    spark = SparkSession.builder.appName("VisualizerTest").getOrCreate()
    
    # Create dummy data for testing
    schema = StructType([
        StructField("Category", StringType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Profit", DoubleType(), True),
        StructField("Year", IntegerType(), True),
    ])
    
    test_data = [
        ("Furniture", 1000.0, 200.0, 2011),
        ("Office Supplies", 1500.0, 300.0, 2011),
        ("Technology", 2000.0, 500.0, 2011),
        ("Furniture", 1200.0, 250.0, 2012),
        ("Office Supplies", 1800.0, 350.0, 2012),
        ("Technology", 2500.0, 600.0, 2012)
    ]
    
    df = spark.createDataFrame(test_data, schema)
    
    # Test the visualizer
    visualizer = DataVisualizer(spark)
    
    # Test bar chart
    bar_chart_df = df.groupBy("Category").agg(spark_sum("Sales").alias("TotalSales"))
    bar_filename = visualizer.plot_bar_chart(bar_chart_df, "Category", "TotalSales", "Total Sales by Category")
    print(f"Bar chart saved as: {bar_filename}")
    
    # Test line chart
    line_chart_df = df.groupBy("Year").agg(spark_sum("Profit").alias("TotalProfit"))
    line_filename = visualizer.plot_line_chart(line_chart_df, "Year", "TotalProfit", "Profit Trend Over Years")
    print(f"Line chart saved as: {line_filename}")
    
    # Test pie chart
    pie_chart_df = df.groupBy("Category").agg(spark_sum("Sales").alias("TotalSales"))
    pie_filename = visualizer.plot_pie_chart(pie_chart_df, "Category", "TotalSales", "Sales Distribution")
    print(f"Pie chart saved as: {pie_filename}")
    
    spark.stop()