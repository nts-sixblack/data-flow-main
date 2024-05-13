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

# Write merged DataFrame to output file
booking_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("booking_analyze_sale.csv")


# DIM_LOCATION
region_df = booking_df.select('Region') \
    .dropDuplicates(['Region']) \
    .withColumn('RegionID', monotonically_increasing_id()) \
    .select('RegionID', 'Region')

# Write location DataFrame to MySQL
region_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_location") \
    .option("user", username) \
    .option("password", password) \
    .mode("overwrite") \
    .save()


# DIM_CUSTOMER
customer_df = booking_df.dropDuplicates(['Ext ID', 'First Name', 'Phone', 'Email']) \
    .withColumnRenamed('Ext ID', 'CustomerID') \
    .withColumnRenamed('First Name', 'CustomerName') \
    .select('CustomerID', 'CustomerName', 'Phone', 'Email')

# Write location DataFrame to MySQL
customer_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_customer") \
    .option("user", username) \
    .option("password", password) \
    .mode("overwrite") \
    .save()


# DIM_VEHICLE
vehicle_df = booking_df.select('Car Class') \
    .withColumnRenamed("Car Class", "carType") \
    .select('carType') \
    .dropDuplicates(['carType']) \
    .withColumn('carID', monotonically_increasing_id()) \
    .select('carID', 'carType')

# Write vehicle DataFrame to MySQL
vehicle_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_vehicle") \
    .option("user", username) \
    .option("password", password) \
    .mode('overwrite') \
    .save()


# DIM TIME
time_df = driver_df.withColumn("year", F.year(F.col("pickup Date Time UTC")))
time_df = time_df.withColumn("month", F.month(F.col("pickup Date Time UTC")))
time_df = time_df.withColumn("dayofweek", F.dayofweek(F.col("pickup Date Time UTC")))
time_df = time_df.dropDuplicates(['year', 'month', 'dayofweek']) \
    .select('year', 'month', 'dayofweek') \
    .withColumn('TimeID_str', F.concat(F.col('year'), F.lit(''), F.col('month'), F.lit(''), F.col('dayofweek'))) \

# Convert the string into an integer
time_df = time_df.withColumn('TimeID', time_df['TimeID_str'].cast('int')) \
    .select('TimeID', 'year', 'month', 'dayofweek')

# Write time DataFrame to MySQL
time_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_time") \
    .option("user", username) \
    .option("password", password) \
    .mode("overwrite") \
    .save()


# DIM SUPPLIER
supplier_df = booking_df.dropDuplicates(['Supplier Type', 'Supplier Name']) \
    .withColumnRenamed("Supplier Type", "supplierType") \
    .withColumnRenamed("Supplier Name", "supplierName") \
    .select('supplierType', 'supplierName')

# Write time DataFrame to MySQL
supplier_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_supplier") \
    .option("user", username) \
    .option("password", password) \
    .mode('append') \
    .save()


# DIM BOOK
dim_book_df = booking_df.join(driver_df, booking_df['ID'] == driver_df['Booking ID'], 'inner') \
    .select(booking_df['ID'], booking_df['Flight Number'], booking_df['Status'], booking_df['Distance Km'],
            driver_df['pickup Date Time UTC'], driver_df['pickupDateTimeLocal'],
            driver_df['pickupLocation'], driver_df['dropoffLocation'], driver_df['price']
            ) \
    .withColumnRenamed("ID", "BookingID") \
    .withColumnRenamed("Distance Km", "drivingDistanceInKm") \
    .withColumnRenamed("pickupLocation", "pickupLocation") \
    .withColumnRenamed("dropoffLocation", "dropOffLocation") \
    .withColumnRenamed("Flight Number", "flightNumber") \
    .withColumnRenamed("Status", "bookingStatus") \
    .withColumnRenamed("price", "totalPrice")

dim_book_df = dim_book_df.filter(dim_book_df['BookingID'].isNotNull()) \
    .withColumn("pickupDateTimeUTC", dim_book_df["pickup Date Time UTC"].cast("timestamp")) \
    .withColumn("pickupDateTimeLocal", dim_book_df["pickupDateTimeLocal"].cast("timestamp")) \
    .select("BookingID", "pickupDateTimeUTC", "pickupDateTimeLocal", "drivingDistanceInKm",
            "pickupLocation", "dropOffLocation", "flightNumber", "bookingStatus", "totalPrice"
            )


dim_book_df.show()

# Write time DataFrame to MySQL
dim_book_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dim_booking") \
    .option("user", username) \
    .option("password", password) \
    .mode("overwrite") \
    .save()

# Stop SparkSession
spark.stop()
