from pyspark.sql import SparkSession

if __name__ == "__main__":
    
    HDFS_PATH = "hdfs://namenode:9000"

    spark = SparkSession.builder.appName("SaveData").getOrCreate()


    countries_df = spark.read.format("csv").option("header", "True").option("separator", ",").load("../spark/apps/Batch processing/GlobalLandTemperaturesByCountry.csv")
    countries_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/rawZone/GlobalLandTemperaturesByCountry.csv")


    cities_df = spark.read.format("csv").option("header", "True").option("separator", ",").load("../spark/apps/Batch processing/GlobalLandTemperaturesByCity.csv")
    cities_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/rawZone/GlobalLandTemperaturesByCity.csv")

    
    global_df = spark.read.format("csv").option("header", "True").option("separator", ",").load("../spark/apps/Batch processing/GlobalTemperatures.csv")
    global_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/rawZone/GlobalTemperatures.csv")
