
'''
Initialize spark streaming service, which streams data from kafka topic.
Processes and trains data from the stream and saves it to a delta sink.
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, FloatType
from pyspark.sql.functions import udf, from_json, col
import findspark
findspark.init()
from pyspark.ml import PipelineModel

import re
from datetime import datetime
from pathlib import Path

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell', 'com.datastax.spark:spark-cassandra-connector_2.12:3.0.0'

SRC_DIR = Path(__file__).resolve().parent

################# Add the topic created
kafka_topic = 'test'
spark = SparkSession \
    .builder \
    .master("local[*]")\
    .appName("StreamingApp") \
    .getOrCreate()

schema = StructType(
    [StructField("created_at", StringType()),
    StructField("message", StringType()),
    StructField("City", StringType()),
    StructField("Temp", FloatType())]
    )

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("header", "true") \
    .load() \
    .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "CAST(value AS STRING) as message")

df = df \
    .withColumn("value", from_json("message", schema)) \
    .select('timestamp', 'value.*')
print(df)

# Changing datetime format
date_process = udf(
    lambda x: datetime.strftime(
        datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
        )
    )
df = df.withColumn("created_at", date_process(df.created_at))

################# Pre-processing the data
pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', \
        x.lower().strip()).split(), ArrayType(StringType())
    )
df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()
#drop the rows with null values in Temp Column 
################# Passing into ml pipeline
model_path = SRC_DIR.joinpath('models')
pipeline_model = PipelineModel.load(model_path)

prediction  = pipeline_model.transform(df)
'''
The labels are labelled with positive (4) as 0.0 
negative (0) as 1.0
'''
prediction_df = prediction \
    .select(prediction.cleaned_data, prediction.created_at, \
         prediction.timestamp, prediction.message, prediction.Temp, prediction.City, prediction.prediction)

def writeToCassandra(writeDF, epochId):
  writeDF.write \
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="sentiments_table", keyspace="tweets_sentiments")\
    .save()

prediction_df.writeStream \
    .option("spark.cassandra.connection.host","localhost:9042")\
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .option("checkpointLocation", "chk-point-dir") \
    .start()\
    .awaitTermination()

