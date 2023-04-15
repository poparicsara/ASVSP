from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

KAFKA_TOPIC_NAME = "weather-topic"
KAFKA_BOOTSTRAP_SERVERS = "kafka2:19093"


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

    spark = SparkSession.builder.appName("Spark streaming").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
                .option("subscribe", KAFKA_TOPIC_NAME).option("startingOffsets", "latest").load()

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
    

    # Trenutna prosecna brzina vetra po gradovima
    wind_speed_df = df.groupBy("weather_city_name").agg({'weather_wind_speed': 'avg'})\
                 .select("weather_city_name", col("avg(weather_wind_speed)").alias("avg_wind_speed"))
    

    # query = wind_speed_df.writeStream.trigger(processingTime='5 seconds').outputMode("update")\
    #                         .option("truncate", "false").format("console").start()

    query = wind_speed_df.writeStream.outputMode("update") \
        .foreachBatch(lambda df, epoch_id: write_result(df, epoch_id, "average_wind_speed")) \
        .start()

    query.awaitTermination()

    print("Stream data processing complete")