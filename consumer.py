import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType

import findspark
findspark.init()

class Consumer:
    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("SparkConsumer") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.cassandra.connection.host", "127.0.0.1") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.streaming.stopGracefullyOnShutdown", True) \
            .getOrCreate()
        
        self.schema = StructType([
            StructField("id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("body", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("created", FloatType(), True),
            StructField("subreddit", StringType(), True),
        ])
        
    def read_stream(self, topic_name):
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic_name) \
            .option("includeHeaders", "true") \
            .load()
        
        parsed_df = df.withColumn("comment_json", from_json(df["value"].cast("string"), self.schema))

        output_df = parsed_df.select(
            "comment_json.id",
            "comment_json.author",
            "comment_json.body",
            "comment_json.score",
            "comment_json.created",
            "comment_json.subreddit",
        )

        output_df = self.clean_data(output_df)
        
        output_df.writeStream\
            .option("failOnDataLoss", "false") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="comments", keyspace="reddit") \
            .option("checkpointLocation", "./pyspark-checkpoint") \
            .start()
        
        self.spark.streams.awaitAnyTermination()
        
    def clean_data(self, df: pyspark.sql.DataFrame):
        # df.withColumn("created", to_date(col("created").cast("string"), "%Y-%m-%d %H:%M:%S"))
        return df

if __name__ == "__main__":
    consumer = Consumer()
    consumer.read_stream("AskReddit")