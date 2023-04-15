from pyspark.sql import SparkSession
from pyspark.sql.functions import year as YEAR
from pyspark.sql.functions import lit, floor, avg, count, asc, min, max, row_number, to_date, col, substring, expr
from pyspark.sql.window import Window


HDFS_PATH = "hdfs://namenode:9000/transformationZone"


def get_decade_temperature_by_country(df):

    # UPIT 1: Prosecna temperatura po deceniji u zavisnosti od drzave

    df = df.withColumn("Decade", floor(YEAR(df["dt"]) / 10) * 10)

    result = df.groupBy("Decade", "Country").agg(avg("AverageTemperature").alias("AverageTemperature"))
    result = result.orderBy(result["Country"].asc(), result["Decade"].asc())

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_decade_temperature_by_country").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_year_temperature_by_country(df):

    # UPIT 2: Prosecna temperatura po godini u zavisnosti od drzave

    df = df.withColumn("Year", YEAR(df["dt"]))

    result = df.groupBy("Year", "Country").agg(avg("AverageTemperature").alias("AverageTemperature"))
    result = result.orderBy(result["Country"].asc(), result["Year"].asc())

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_year_temperature_by_country").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_10_countries_with_highest_temp_rise(df):

    # UPIT 3: 10 drzava sa najvecim rastom temperature u poslednjih 100 godina

    df = df.withColumn("Year", YEAR(df["dt"]))

    first_50 = df.filter(df["Year"].between(1913, 1963))
    first_50 = first_50.groupBy("Country").agg(avg("AverageTemperature").alias("AverageTempInFirst50"))

    second_50 = df.filter(df["Year"].between(1963, 2013))
    second_50 = second_50.groupBy("Country").agg(avg("AverageTemperature").alias("AverageTempInSecond50"))

    last_100 = first_50.join(second_50, "Country")
    last_100 = last_100.withColumn("TemperatureDifference", (last_100["AverageTempInSecond50"] - last_100["AverageTempInFirst50"]))
    last_100 = last_100.orderBy(last_100["TemperatureDifference"].desc()).limit(10)
    last_100.show()

    last_100.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_countries_with_highest_temp_rise").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_decade_temperature_by_city(df):

    # UPIT 4: Prosecna temperatura po deceniji u zavisnosti od grada

    df = df.withColumn("Decade", floor(YEAR(df["dt"]) / 10) * 10)

    result = df.groupBy("Decade", "City").agg(avg("AverageTemperature").alias("AverageTemperature"))
    result = result.orderBy(result["City"].asc(), result["Decade"].asc())

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_decade_temperature_by_city").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_10_cities_with_biggest_temp_difference_comparing_to_country(df):

    # UPIT 5: 10 gradova koji imaju najvece odskakanje prosecne temperature od prosecne temperature drzave

    temp_by_country = df.groupBy("Country").agg(avg("AverageTemperature").alias("AverageTempByCountry"))

    temp_by_city = df.groupBy("City", "Country").agg(avg("AverageTemperature").alias("AverageTempByCity"))
    temp_by_city = temp_by_city.join(temp_by_country, ["Country"])
    temp_by_city = temp_by_city.withColumn("TempDifference", lit(temp_by_city["AverageTempByCity"] - temp_by_country["AverageTempByCountry"]))
    temp_by_city = temp_by_city.orderBy(temp_by_city["TempDifference"].desc()).limit(10)

    temp_by_city.select("City", "Country", "AverageTempByCity", "AverageTempByCountry", "TempDifference")

    temp_by_city.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_cities_with_biggest_temp_difference_compairing_to_country").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_centry_with_highest_temp_of_land_and_ocean(df):

    # UPIT 6: Vek sa najvecom prosecnom temperaturom okeana i tla

    df = df.withColumn("Centry", floor(YEAR(df["dt"]) / 100))

    result = df.groupBy("Centry").agg(avg("LandAndOceanAverageTemperature").alias("LandAndOceanAverageTemperature"))
    result = result.sort(result["LandAndOceanAverageTemperature"].desc()).limit(1)

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_centry_with_highest_temp_of_land_and_ocean").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_latitude_and_longitude_with_highest_temp(df):

    # UPIT 7: Geografska sirina i duzina sa najvecom prosecnom temperaturom

    north = df.filter(df["Latitude"].endswith("N"))
    north = north.groupBy("City", "Country", "Latitude", "Longitude").agg(avg("AverageTemperature").alias("AverageTemperature"))
    north = north.sort(north["AverageTemperature"].desc()).limit(10)
    north = north.withColumn("Lat", expr("substring(Latitude, 1, length(Latitude)-1)"))
    north = north.withColumn("Long", expr("substring(Longitude, 1, length(Longitude)-1)"))
    north = north.select("City", "Country", north["Lat"].alias("Latitude"), north["Long"].alias("Longitude"), "AverageTemperature")

    south = df.filter(df["Latitude"].endswith("S"))
    south = south.groupBy("City", "Country", "Latitude", "Longitude").agg(avg("AverageTemperature").alias("AverageTemperature"))
    south = south.sort(south["AverageTemperature"].desc()).limit(10)
    south = south.withColumn("Lat", expr("substring(Latitude, 1, length(Latitude)-1)"))
    south = south.withColumn("Long", expr("substring(Longitude, 1, length(Longitude)-1)"))
    south = south.select("City", "Country", south["Lat"].alias("Latitude"), south["Long"].alias("Longitude"), "AverageTemperature")

    result = north.union(south)
    
    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_highest_temps_by_latitude_and_longitude").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_10_years_with_highest_temp(df):

    # UPIT 8: 10 godina sa najvisom temperaturom + koja je po redosledu ta 

    df = df.withColumn("Year", YEAR(df["dt"]))

    result = df.groupBy("Year").agg(avg("AverageTemperature").alias("AverageTempByYear"))
    result = result.sort(result["AverageTempByYear"].desc()).limit(10)

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_years_with_highest_temp").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_measurement_num_by_country(df):

    # UPIT 9: Broj izvrsenih merenja u svakom gradu

    df = df.filter(YEAR(df["dt"]) > 1850)
    df = df.select("Country")

    window = Window.partitionBy("Country")

    result = df.withColumn("Measurements", count("Country").over(window))
    result = result.select("Country", "Measurements").distinct()

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_measurment_num_by_country").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()


def get_temperature_rank(df):

    # UPIT 10: Izmerena temperature i njen rang u odnosu na druga merenja u tom gradu 

    window = Window.partitionBy("City").orderBy("AverageTemperature")

    result = df.withColumn("TemperatureRank", row_number().over(window))
    result = result.select("City", "AverageTemperature", "TemperatureRank")

    result.write.format("jdbc").\
        option("url", "jdbc:postgresql://db:5432/DATABASE").\
        option("driver", "org.postgresql.Driver").\
        option("dbtable", "get_highest_and_lowest_temp").\
        option("user", "user").\
        option("password", "password").\
        mode("overwrite").save()



if __name__ == "__main__":

    spark = SparkSession.builder.appName("WeatherApp").getOrCreate()

    country_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/GlobalLandTemperaturesByCountry.csv")
    city_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/GlobalLandTemperaturesByCity.csv")
    global_df = spark.read.format("csv").option("header", "True").option("separator", ",").load(HDFS_PATH + "/GlobalTemperatures.csv")

    get_decade_temperature_by_country(country_df)

    get_year_temperature_by_country(country_df)

    get_10_countries_with_highest_temp_rise(country_df)

    get_decade_temperature_by_city(city_df)

    get_10_cities_with_biggest_temp_difference_comparing_to_country(city_df)

    get_centry_with_highest_temp_of_land_and_ocean(global_df)

    get_latitude_and_longitude_with_highest_temp(city_df)

    get_10_years_with_highest_temp(city_df)

    get_measurement_num_by_country(country_df)

    get_temperature_rank(city_df)
    

