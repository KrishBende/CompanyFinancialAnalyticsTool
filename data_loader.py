import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import col

def load_data_to_spark(file_path):
    """
    Loads the superstore CSV data into a PySpark DataFrame.
    
    Args:
        file_path (str): Path to the 'superstore.csv' file.
        
    Returns:
        tuple: A tuple containing (spark_session, spark_dataframe).
    """
    # Initialize Spark Session
    spark = (
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
    spark_df = spark.read.csv(
        file_path, 
        header=True, 
        schema=schema, 
        dateFormat="yyyy-MM-dd"
    )
    
    # Clean column names by replacing dots with underscores for easier handling in Spark SQL
    # This creates a new DataFrame with cleaned column names
    cleaned_columns = [col_name.replace('.', '_') for col_name in spark_df.columns]
    cleaned_df = spark_df.toDF(*cleaned_columns)
    
    return spark, cleaned_df

# Example usage (for testing)
if __name__ == "__main__":
    spark_session, df = load_data_to_spark("superstore.csv")
    df.printSchema()
    df.show(5)
    # Stop the session when done (in a real app, you might keep it alive longer)
    # spark_session.stop()