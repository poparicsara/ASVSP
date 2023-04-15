from pyspark.sql import SparkSession

if __name__ == "__main__":
    
    HDFS_PATH = "hdfs://namenode:9000"

    spark = SparkSession.builder.appName("RawApp").getOrCreate()


    countries_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/rawZone/GlobalLandTemperaturesByCountry.csv")
    countries_df = countries_df.drop("AverageTemperatureUncertainty")
    countries_df = countries_df.filter(countries_df["AverageTemperature"].isNotNull())
    countries_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/transformationZone/GlobalLandTemperaturesByCountry.csv")


    cities_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/rawZone/GlobalLandTemperaturesByCity.csv")
    cities_df = cities_df.drop("AverageTemperatureUncertainty")
    cities_df = cities_df.filter(cities_df["AverageTemperature"].isNotNull())
    cities_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/transformationZone/GlobalLandTemperaturesByCity.csv")
    

    global_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/rawZone/GlobalTemperatures.csv")
    global_df = global_df.drop("LandAverageTemperatureUncertainty")
    global_df = global_df.drop("LandAndOceanAverageTemperatureUncertainty")
    global_df = global_df.drop("LandMaxTemperatureUncertainty")
    global_df = global_df.drop("LandMinTemperatureUncertainty")
    global_df = global_df.filter(global_df["LandAverageTemperature"].isNotNull())
    global_df = global_df.filter(global_df["LandAndOceanAverageTemperature"].isNotNull())
    global_df.write.format("csv").mode("overwrite").option("header", "True").save(HDFS_PATH + "/transformationZone/GlobalTemperatures.csv")
