from pyspark.sql import SparkSession
from pyspark.sql.functions import *

kafka_topic_name = "weather-topic"
kafka_bootstrap_servers = "kafka2:19093"


def write_result(result, epoch, tablename):

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", tablename).\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()



if __name__ == "__main__":

    print("Welcome to Spark Structured Streaming!")

    # spark = SparkSession.builder.appName("Spark streaming").\
    #     config("spark.jars", "spark-sql-kafka-0-10_2.12-3.0.1.jar, kafka-clients-3.3.1.jar, \
    #         spark-token-provider-kafka-0-10_2.12-3.0.1.jar, commons-pool2-2.11.1.jar").getOrCreate()

    spark = SparkSession.builder.appName("Spark streaming").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
                .option("subscribe", kafka_topic_name).option("startingOffsets", "latest").load()

    df.printSchema()

    df = df.selectExpr("CAST(value AS STRING)")

    weather_schema_string = "weather_dt_iso TIMESTAMP, weather_city_name STRING, weather_temp DOUBLE, "\
                         + "weather_temp_min DOUBLE, weather_temp_max DOUBLE, weather_pressure DOUBLE, "\
                         + "weather_humidity DOUBLE, weather_wind_speed DOUBLE, weather_wind_deg DOUBLE, "\
                         + "weather_rain_1h DOUBLE, weather_rain_3h DOUBLE, weather_clouds_all DOUBLE, "\
                         + "weather_id DOUBLE, weather_main STRING, weather_description STRING, weather_icon STRING"


    df = df.select(from_csv(col("value"), weather_schema_string).alias("weather"))

    df = df.select("weather.*" )
    df.printSchema()



    # Grad sa najvecim vazdusnim pritiskom u sat i po

    df = df.groupBy("weather_city_name", window(df.weather_dt_iso, "1 hour 30 minutes", "30 minutes"))\
        .agg(max("weather_pressure").alias("max_pressure"))\
    
    df = df.withColumn("window", to_csv(col("window")))
    df = df.select("window", "weather_city_name", "max_pressure")

    # query = df.writeStream.trigger(processingTime='5 seconds').outputMode("update")\
    #                         .option("truncate", "false").format("console").start()

    query=df.writeStream.outputMode("update") \
        .foreachBatch(lambda df, epoch_id: write_result(df, epoch_id, "pressure")) \
        .start()

    query.awaitTermination()

    print("Stream data processing complete")