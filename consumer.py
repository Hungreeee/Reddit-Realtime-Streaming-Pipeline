import pyspark
import os
from time import sleep
from threading import Thread
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import findspark
findspark.init()

class Consumer:
    def __init__(self, topic_list: str):
        self.topic_list = topic_list
        
        self.cluster = Cluster(['127.0.0.1'], port=9042)

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
            StructField("flair", StringType(), True),
        ])
        
    def read_stream(self):
        df = self.spark.readStream \
            .format("kafka") \
            .option("failOnDataLoss", "false") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "comments") \
            .option("includeHeaders", "true") \
            .load()
        
        df = df.withColumn("comment_json", from_json(df["value"].cast("string"), self.schema)) \
                .select(
                    "comment_json.id",
                    "comment_json.author",
                    "comment_json.body",
                    "comment_json.score",
                    "comment_json.created",
                    "comment_json.subreddit",
                    "comment_json.flair")
        return df
    
    def sentiment_analyzer(self, df):
        analyzer = SentimentIntensityAnalyzer()
        udf_analyzer = udf(lambda text: analyzer.polarity_scores(text)["compound"], FloatType())
        df = df.withColumn("sentiment_score", udf_analyzer(col("body")))
        df = df.withColumn("sentiment_tag", when(col("sentiment_score") >= 0.05, "positive") \
                                        .otherwise(when(col("sentiment_score") <= -0.05, "negative") \
                                            .otherwise("neutral")))
        return df
    
    def nGram_analyzer(self, tokenized_df):
        unigramlist = tokenized_df.withColumn("ngram", explode("cleaned_body")) \
            .withWatermark("timestamp", "1 minute") \
            .groupBy("ngram", "timestamp") \
            .agg(mean("sentiment_score").alias("mean_sentiment"),
                count("ngram").alias("frequency"))

        bigramlist = tokenized_df.withColumn("ngram", explode("ng2_body")) \
            .withWatermark("timestamp", "1 minute") \
            .groupBy("ngram", "timestamp") \
            .agg(mean("sentiment_score").alias("mean_sentiment"),
                count("ngram").alias("frequency"))

        freqlist = bigramlist.union(unigramlist) 
        freqlist = freqlist.filter((col("ngram").isNotNull()) & (col("timestamp").isNotNull()) & (col("ngram") != ""))
        return freqlist
        
    def structure_data(self, df, topic):
        print("Begin structuring data...")
        tokenizer = Tokenizer(inputCol="body", outputCol="tokenized_body")
        ngram = NGram(n=2, inputCol="cleaned_body", outputCol="ng2_body")
        remover = StopWordsRemover(inputCol="tokenized_body", outputCol="cleaned_body")

        df = df.filter(col("subreddit") == topic)
        df = df.withColumn("timestamp", to_timestamp(from_unixtime(col("created").cast(FloatType()), "dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))
        df = df.withColumn("body", (regexp_replace(col("body"), r"((www\.\S+)|(https?://\S+))|(\S+\.com)", "")))
        df = df.withColumn("body", (regexp_replace(col("body"), r"[^\sa-zA-Z0-9]", "")))
        df = df.withColumn("body", (regexp_replace(col("body"), r"(.)\1\1+", "")))
        df = self.sentiment_analyzer(df)

        df = tokenizer.transform(df)
        df = remover.transform(df)
        df = ngram.transform(df)

        return df.drop("tokenized_body")
        
    def write_stream(self, df, topic: str):
        session = self.cluster.connect()
        
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS reddit.{topic}(
                id text,
                author text,
                body text,
                score int,
                created float,
                timestamp timestamp,
                subreddit text,
                sentiment_score float,
                sentiment_tag text,
                flair text,
                cleaned_body list<text>,
                ng2_body list<text>,
                PRIMARY KEY(id, created)
            )
            WITH CLUSTERING ORDER BY (created DESC);
        """)

        session.execute(f"""
            CREATE TABLE IF NOT EXISTS reddit.{topic}_freqtable(
                ngram text,
                frequency int,
                mean_sentiment float,
                timestamp timestamp,
                PRIMARY KEY(ngram, frequency)
            )
            WITH CLUSTERING ORDER BY (frequency DESC);
        """)

        df.writeStream \
            .format("console")\
            .outputMode("append")\
            .start()

        # self.nGram_analyzer(df).writeStream \
        #     .option("failOnDataLoss", "false") \
        #     .format("org.apache.spark.sql.cassandra") \
        #     .options(table=f"{topic}_freqtable", keyspace="reddit") \
        #     .option("checkpointLocation", f"./pyspark-checkpoint/freq-table/{topic}") \
        #     .start()

        self.nGram_analyzer(df).writeStream \
            .trigger(processingTime="1 second") \
            .foreachBatch(
                lambda batchDF, batchID: batchDF.write.format("org.apache.spark.sql.cassandra") \
                    .option("checkpointLocation", f"./pyspark-checkpoint/freq-table/{topic}") \
                    .options(table=f"{topic}_freqtable", keyspace="reddit") \
                    .mode("append").save()
            ).outputMode("update").start()
        
        df.writeStream \
            .option("failOnDataLoss", "false") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=topic, keyspace="reddit") \
            .option("checkpointLocation", f"./pyspark-checkpoint/comments/{topic}") \
            .start()

    def process_stream(self):
        df = consumer.read_stream()
        for topic in self.topic_list:
            print(f"Working on topic {topic}")
            df_new = df.alias('df_new')
            df_new = consumer.structure_data(df_new, topic)
            consumer.write_stream(df_new, topic)
        
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    topic_list = []
    while os.stat("topics.txt").st_size == 0:
        sleep(0.5)
    
    with open("topics.txt", "r") as f:
        for line in f:
            if line.strip() != "":
                topic_list.append(line.strip())

    consumer = Consumer(topic_list)
    consumer.process_stream()
