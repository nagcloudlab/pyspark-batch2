from pyspark.sql import SparkSession
from pyspark import SparkConf

if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("MySparkApp") \
        .setMaster("local[3]")

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    # read

    survey_df = spark.read \
        .csv("data/sample.csv", header=True,inferSchema=True)
    # .format("csv") \
    # .option("path", "data/sample.csv") \
    # .option("header", True) \
    # .option("inferSchema", True) \
    # .load()

    # show partitions of survey_df
    # print("Partition Count: ", survey_df.rdd.getNumPartitions())

    survey_df.repartition(2)

    # data - processing Req, e.g age < 40 & how many people per country? )

    young_people_df=survey_df.filter("Age < 40")

    count_by_country = young_people_df \
        .select("Age", "Country") \
        .groupBy("Country") \
        .count()

    result=count_by_country.collect()
    print(result)

    # survey_df.show()
    # survey_df.printSchema()


    input("Press Enter to continue...")


    spark.stop()
