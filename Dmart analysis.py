# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import col

# Task 1: Establish PySpark Connection
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Sales Data Pipeline") \
    .getOrCreate()

print("PySpark Session Established")

# Task 2: Load Data into PySpark DataFrames
# Replace 'path/to/...' with the actual paths to your CSV files
products_df = spark.read.csv("C:/Users/AJAY/Desktop/Guvi/Dmart/Product.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("C:/Users/AJAY/Desktop/Guvi/Dmart/Sales.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("C:/Users/AJAY/Desktop/Guvi/Dmart/Customer.csv", header=True, inferSchema=True)

# Display the schemas of loaded DataFrames
print("Products Schema:")
products_df.printSchema()

print("Sales Schema:")
sales_df.printSchema()

print("Customers Schema:")
customers_df.printSchema()

# Task 3: Data Transformation and Cleaning

# Rename columns for consistency
products_df = products_df.withColumnRenamed("ProductID", "product_id")
customers_df = customers_df.withColumnRenamed("CustomerID", "customer_id")
sales_df = sales_df.withColumnRenamed("ProductID", "product_id") \
                   .withColumnRenamed("CustomerID", "customer_id")

# Handle missing values
products_df = products_df.fillna({"price": 0.0})
sales_df = sales_df.dropna(subset=["product_id", "customer_id"])
customers_df = customers_df.fillna({"age": 0, "location": "Unknown"})

# Ensure correct data types
products_df = products_df.withColumn("price", products_df["price"].cast(FloatType()))
customers_df = customers_df.withColumn("age", customers_df["age"].cast(IntegerType()))

# Join the DataFrames on relevant keys
# Step 1: Join sales_df with products_df
sales_products_df = sales_df.join(products_df, on="product_id", how="inner")

# Step 2: Join the resulting DataFrame with customers_df
final_df = sales_products_df.join(customers_df, on="customer_id", how="inner")

# Show the final DataFrame
print("Final Joined DataFrame:")
final_df.show()

# Optional: Save the final DataFrame to a CSV file
final_df.write.csv("C:/Users/AJAY/Desktop/Guvi/Final_file/final_data.csv", header=True)
