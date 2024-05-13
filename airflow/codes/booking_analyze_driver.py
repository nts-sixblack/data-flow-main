import logging
import os
import subprocess

import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, monotonically_increasing_id
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


booking_directory = "hdfs://namenode:8020/user/root/logistic/booking_data/Booking_data_Feb2024.xlsx"
driver_directory = "hdfs://namenode:8020/user/root/logistic/driver_report/Driver_Report_Feb2024.xlsx"
jdbc_url = "jdbc:mysql://mysql_db:3306/logistic"

# Execute the 'hadoop classpath --glob' command
classpath = subprocess.check_output("hadoop classpath --glob", shell=True).decode('utf-8').strip()

# Set the CLASSPATH environment variable
os.environ['CLASSPATH'] = classpath

# Get MySQL username and password
username = "root"
password = "u123FJf5dhRMa21"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("booking_analyze_sale") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.1.jar") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.executor.heartbeatInterval", "10000ms") \
    .config("spark.network.timeout", "800s") \
    .config("spark.driver.memoryOverhead", "3g") \
    .config("spark.executor.memoryOverhead", "3g") \
    .getOrCreate()


logging.info("This is booking analyze app.................")

# Read each Excel file into a DataFrame
booking_df = spark.read.format("excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(booking_directory)

# Read each Excel file into a DataFrame
driver_df = spark.read.format("excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(driver_directory)


# DIM_LOCATION
region_df = spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'dim_location') \
    .option("user", username) \
    .option("password", password) \
    .load()


# DIM_CUSTOMER
customer_df = spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'dim_customer') \
    .option("user", username) \
    .option("password", password) \
    .load()

# DIM_VEHICLE
vehicle_df = spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'dim_vehicle') \
    .option("user", username) \
    .option("password", password) \
    .load()


# DIM TIME
time_df = spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'dim_time') \
    .option("user", username) \
    .option("password", password) \
    .load()

# DIM SUPPLIER
supplier_df = spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'dim_supplier') \
    .option("user", username) \
    .option("password", password) \
    .load()


# DIM BOOK
dim_book_df = spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'dim_booking') \
    .option("user", username) \
    .option("password", password) \
    .load()

# DIM SALE
dim_sale_df = booking_df.join(driver_df, booking_df['ID'] == driver_df['Booking ID'], 'inner')

dim_sale_df = dim_sale_df.withColumn("year", F.year(F.col("pickup Date Time UTC")))
dim_sale_df = dim_sale_df.withColumn("month", F.month(F.col("pickup Date Time UTC")))
dim_sale_df = dim_sale_df.withColumn("dayofweek", F.dayofweek(F.col("pickup Date Time UTC")))
dim_sale_df = dim_sale_df.withColumn('TimeID_str', F.concat(F.col('year'), F.lit(''), F.col('month'), F.lit(''), F.col('dayofweek')))

# Convert the string into an integer
dim_sale_df = dim_sale_df.withColumn('TimeID', dim_sale_df['TimeID_str'].cast('int'))

dim_sale_df = dim_sale_df.withColumnRenamed("Total VND", "TotalVND") \
    .withColumnRenamed("Total6", "TotalUSD") \
    .withColumnRenamed("Payment VND", "PaymentVND") \
    .withColumnRenamed("Payment21", "PaymentUSD") \
    .withColumnRenamed("Profit VND", "ProfitVND") \
    .withColumnRenamed("Profit", "ProfitUSD")

dim_sale_df.show()

dim_sale_df = dim_sale_df \
    .withColumn("SaleVND", dim_sale_df["TotalVND"]) \
    .withColumn("SaleUSD", dim_sale_df["TotalUSD"])


dim_sale_joined = dim_sale_df \
    .join(region_df, dim_sale_df['Region'] == region_df['Region'], 'inner') \
    .join(supplier_df, dim_sale_df['Supplier Name'] == supplier_df['supplierName'], 'inner') \
    .join(customer_df, dim_sale_df['Email'] == customer_df['Email'], 'inner') \
    .join(dim_book_df, dim_sale_df['ID'] == dim_book_df['BookingID'], 'inner') \
    .join(vehicle_df, dim_sale_df['Car Class'] == vehicle_df['carType'], 'inner') \
    .withColumn("SupplierID", supplier_df["supplierID"]) \
    .withColumn("VehicleID", vehicle_df["carID"]) \
    .select(dim_sale_df['TotalVND'], dim_sale_df['TotalUSD'],
            dim_sale_df['PaymentVND'], dim_sale_df['PaymentUSD'],
            dim_sale_df['ProfitVND'], dim_sale_df['ProfitUSD'],
            dim_sale_df['SaleVND'], dim_sale_df['SaleUSD'],
            region_df['RegionID'],
            'SupplierID',
            customer_df['CustomerID'],
            'TimeID',
            dim_book_df['BookingID'],
            'VehicleID'
            )

dim_sale_joined.show()

# Write time DataFrame to MySQL
dim_sale_joined.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "fact_sale") \
    .option("user", username) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# DIM Driver
dim_driver_df = driver_df.withColumn("BookingID", driver_df["Booking ID"]) \
    .withColumn("driverEventStatus", driver_df["driverEventsStatus"]) \
    .withColumn("riderScore", driver_df["rideScore"]) \
    .withColumn("incidentType", driver_df["incidentType"]) \
    .select('BookingID', 'driverEventStatus', 'riderScore', 'incidentType')

# Write time DataFrame to MySQL
dim_driver_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "driver_report") \
    .option("user", username) \
    .option("password", password) \
    .mode("overwrite") \
    .save()


# Stop SparkSession
spark.stop()
