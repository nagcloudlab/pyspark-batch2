from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

'''
Example : Working with DataFrameReader i.e source & schema
'''

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("pyspark-app") \
        .getOrCreate()
    
    # Read data from CSV/JSON file with custom schema
    
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])


    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""


    # Read data from CSV file
    flightTimeCsvDF=spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("dateFormat", "M/d/y") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .load("source/flight-time.csv") \
    
    flightTimeCsvDF.printSchema()
    flightTimeCsvDF.show(5)

    # Read data from JSON file
    flightTimeJsonDF=spark.read \
        .format("json") \
        .option("dateFormat", "M/d/y") \
        .schema(flightSchemaDDL) \
        .option("mode", "FAILFAST") \
        .load("source/flight-time.json")
    
    flightTimeJsonDF.printSchema()
    flightTimeJsonDF.show(5)

    # Read data from Parquet file
    flightTimeParquetDF=spark.read \
        .format("parquet") \
        .option("mode", "FAILFAST") \
        .load("source/flight-time.parquet")
    
    flightTimeParquetDF.printSchema()
    flightTimeParquetDF.show(5)

    spark.stop()