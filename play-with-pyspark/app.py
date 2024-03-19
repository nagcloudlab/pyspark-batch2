from pyspark.sql import SparkSession

if __name__ == "__main__":

    # Create a SparkSession
    spark = SparkSession.builder.appName("pyspark-app").master('local[3]').getOrCreate()

    # Read
    survey_df = spark.read.csv("input/sample.csv", header=True, inferSchema=True)

    # Tranformation
    country_count_df=survey_df\
    .filter("Age<40") \
    .select("Country") \
    .groupBy("Country") \
    .count()

    # Write
    # country_count_df.show()
    # country_count_df.write.csv("output/country_count.csv", mode="overwrite", header=True)

    # Stop the SparkSession
    spark.stop()

