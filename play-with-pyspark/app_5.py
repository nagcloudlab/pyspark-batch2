from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import spark_partition_id


'''
Example : Working with DataFrameWriter i.e sink
'''

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("pyspark-app") \
        .getOrCreate()
    

    # Read 
    flightTimeParquetDF=spark.read \
        .format("parquet") \
        .option("mode", "FAILFAST") \
        .load("data/flight-time.parquet")
    
    print("Num Partitions before: ", flightTimeParquetDF.rdd.getNumPartitions())
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    
    # Transformations

    # Write
    partionedFlightTimeParquetDF=flightTimeParquetDF.repartition(1)
    print("Num Partitions after: ", partionedFlightTimeParquetDF.rdd.getNumPartitions())
    partionedFlightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("OP_CARRIER","ORIGIN") \
        .option("path", "dataSink/json/") \
        .option("maxRecordsPerFile", 10000) \
        .save()
   

    spark.stop()