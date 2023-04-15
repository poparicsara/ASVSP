# !/bin/bash

# printf "\nSAVE FILES TO HDFS\n"
# sudo docker exec -it spark-master ./spark/bin/spark-submit  ../spark/apps/saveData.py 

# printf "\nPREPROCESSING\n"
# sudo docker exec -it spark-master ./spark/bin/spark-submit  ../spark/apps/raw.py 

# printf "\nBATCH PROCESSING\n"
# sudo docker exec -it spark-master ./spark/bin/spark-submit  --jars ../spark/apps/postgresql-42.5.1.jar ../spark/apps/transformation.py

printf "\REAL-TIME PROCESSING\n"
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/biggestHumidity.py  &
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/averageWindSpeed.py & 
docker exec spark-master ./spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ./spark/apps/postgresql-42.5.1.jar ./spark/apps/sparkStreaming.py  


