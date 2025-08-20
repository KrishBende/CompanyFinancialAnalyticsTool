import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import col

class FinancialDataProcessor:
    """
    A class to handle loading and basic processing of the superstore financial data.
    """
    
    def __init__(self, file_path):
        """
        Initializes the processor with the path to the CSV file.
        
        Args:
            file_path (str): Path to the 'superstore.csv' file.
        """
        self.file_path = file_path
        self.spark = None
        self.df = None
        
    def load_data(self):
        """
        Loads the superstore CSV data into a PySpark DataFrame with cleaned column names.
        """
        # Initialize Spark Session
        self.spark = (
            SparkSession.builder
            .appName("CompanyInternalFinancials")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        
        # Define schema for better performance and type safety
        schema = StructType([
            StructField("Category", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Customer.ID", StringType(), True),
            StructField("Customer.Name", StringType(), True),
            StructField("Discount", DoubleType(), True),
            StructField("Market", StringType(), True),
            StructField("记录数", IntegerType(), True), # 'Record Count' in Chinese
            StructField("Order.Date", DateType(), True),
            StructField("Order.ID", StringType(), True),
            StructField("Order.Priority", StringType(), True),
            StructField("Product.ID", StringType(), True),
            StructField("Product.Name", StringType(), True),
            StructField("Profit", DoubleType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Region", StringType(), True),
            StructField("Row.ID", IntegerType(), True),
            StructField("Sales", DoubleType(), True),
            StructField("Segment", StringType(), True),
            StructField("Ship.Date", DateType(), True),
            StructField("Ship.Mode", StringType(), True),
            StructField("Shipping.Cost", DoubleType(), True),
            StructField("State", StringType(), True),
            StructField("Sub.Category", StringType(), True),
            StructField("Year", IntegerType(), True),
            StructField("Market2", StringType(), True),
            StructField("weeknum", IntegerType(), True)
        ])
        
        # Load data with defined schema
        raw_df = self.spark.read.csv(
            self.file_path, 
            header=True, 
            schema=schema, 
            dateFormat="yyyy-MM-dd"
        )
        
        # Clean column names by replacing dots with underscores for easier handling in Spark SQL
        cleaned_columns = [col_name.replace('.', '_') for col_name in raw_df.columns]
        self.df = raw_df.toDF(*cleaned_columns)
        
        print("Data loaded successfully.")
        self.df.printSchema()
        self.df.show(5)
        
    def get_spark_session(self):
        """
        Returns the active Spark session.
        
        Returns:
            SparkSession: The active Spark session.
        """
        return self.spark
        
    def get_dataframe(self):
        """
        Returns the loaded and processed Spark DataFrame.
        
        Returns:
            pyspark.sql.DataFrame: The processed DataFrame.
        """
        return self.df

# Example usage (for testing)
if __name__ == "__main__":
    processor = FinancialDataProcessor("superstore.csv")
    processor.load_data()
    # The session and dataframe are now available via processor.get_spark_session() and processor.get_dataframe()