from pyspark.sql.session import SparkSession

if __name__ == '__main__':

    HDFS_NAMENODE = "hdfs://localhost:9000"

    spark = SparkSession.builder.appName("HDFSData").getOrCreate()

    df = spark.read("../Data/Batch\ processing/GlobalLandTemperaturesByMajorCity.csv", header = True)

    df.show()