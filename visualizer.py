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
    
    def __init__(self, spark_session, charts_dir=None):
        """
        Initializes the visualizer with a Spark session.
        
        Args:
            spark_session (SparkSession): An active Spark session.
            charts_dir (str): Directory to save charts. If None, uses current directory.
        """
        self.spark = spark_session
        self.charts_dir = charts_dir or os.getcwd()
        # Set styling for plots
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
    def _save_chart(self, title):
        """
        Helper method to save chart to the designated directory.
        
        Args:
            title (str): The title of the chart.
            
        Returns:
            str: The full path to the saved plot.
        """
        # Create filename from title
        filename = f"{title.replace(' ', '_').lower()}.png"
        # Ensure filename is filesystem-safe
        filename = "".join(c for c in filename if c.isalnum() or c in "_-.") or "chart.png"
        
        # Full path to save the file
        full_path = os.path.join(self.charts_dir, filename)
        
        # Ensure the charts directory exists
        os.makedirs(self.charts_dir, exist_ok=True)
        
        # Save the figure
        plt.savefig(full_path, dpi=300, bbox_inches='tight')
        
        # Clear the figure to free memory
        plt.clf()
        plt.close()
        
        # Return just the filename (not the full path)
        return filename
        
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
        plt.figure(figsize=(12, 7))
        ax = sns.barplot(data=pandas_df, x=x_col, y=y_col, palette="viridis")
        
        # Set labels and title
        ax.set_title(title, fontsize=18, pad=20, fontweight='bold')
        ax.set_xlabel(xlabel or x_col, fontsize=14, fontweight='medium')
        ax.set_ylabel(ylabel or y_col, fontsize=14, fontweight='medium')
        
        # Rotate x-axis labels if they are long
        if pandas_df[x_col].dtype == 'object' and pandas_df[x_col].str.len().max() > 10:
            plt.xticks(rotation=45, ha='right')
        elif pandas_df[x_col].dtype == 'object':
            plt.xticks(rotation=30, ha='right')
            
        # Add value labels on bars
        for p in ax.patches:
            ax.annotate(f'{p.get_height():.0f}', 
                        (p.get_x() + p.get_width() / 2., p.get_height()), 
                        ha='center', va='center', fontsize=10, color='black', 
                        xytext=(0, 10), textcoords='offset points')
            
        plt.tight_layout()
        
        # Save to a file
        return self._save_chart(title)
    
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
        plt.figure(figsize=(12, 7))
        ax = sns.lineplot(data=pandas_df, x=x_col, y=y_col, marker='o', linewidth=3, markersize=8, color='#2c3e50')
        
        # Set labels and title
        ax.set_title(title, fontsize=18, pad=20, fontweight='bold')
        ax.set_xlabel(xlabel or x_col, fontsize=14, fontweight='medium')
        ax.set_ylabel(ylabel or y_col, fontsize=14, fontweight='medium')
        
        # Add grid for better readability
        ax.grid(True, linestyle='--', alpha=0.7)
        
        # Format y-axis to show values in a more readable format
        ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        plt.tight_layout()
        
        # Save to a file
        return self._save_chart(title)
    
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
        plt.figure(figsize=(10, 10))
        colors = sns.color_palette("husl", len(pandas_df))
        wedges, texts, autotexts = plt.pie(
            pandas_df[value_col], 
            labels=pandas_df[label_col], 
            autopct='%1.1f%%', 
            startangle=140,
            colors=colors,
            explode=[0.05] * len(pandas_df)  # Slightly separate slices
        )
        
        # Style the text
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            
        plt.title(title, fontsize=18, pad=20, fontweight='bold')
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        
        plt.tight_layout()
        
        # Save to a file
        return self._save_chart(title)

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