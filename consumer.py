import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import findspark
findspark.init()

class Consumer:
    def __init__(self, topic_list: str):
        self.topic_list = topic_list

        self.spark = SparkSession \
            .builder \
            .appName("SparkConsumer") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.cassandra.connection.host", "127.0.0.1") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.streaming.stopGracefullyOnShutdown", True) \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        
        self.schema = StructType([
            StructField("id", StringType(), True),
            StructField("author", StringType(), True),
            StructField("body", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("created", IntegerType(), True),
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
    
    def word_freq_analyzer(self, tokenized_df):
        stopwordList = ["lol", "cant", "dont", "im", "doesnt"] 
        stopwordList.extend(StopWordsRemover().getStopWords())

        tokenizer = Tokenizer(inputCol="body", outputCol="tokenized_body")
        remover = StopWordsRemover(inputCol="tokenized_body", outputCol="cleaned_body", stopWords=stopwordList)

        df = tokenized_df.withColumn("body", trim(regexp_replace(col("body"), r"((www\.\S+)|(https?://\S+))|(\S+\.com)", "")))
        df = df.withColumn("body", trim(regexp_replace(col("body"), r"[^\sa-zA-Z0-9]", "")))
        df = df.withColumn("body", trim(regexp_replace(col("body"), r"(.)\1\1+", "")))

        df = tokenizer.transform(df)
        df = remover.transform(df)

        freqlist = df.withColumn("ngram", explode("cleaned_body")) \
            .groupBy("ngram") \
            .agg(mean("sentiment_score").alias("mean_sentiment"),
                count("ngram").alias("frequency"))
        
        freqlist = freqlist.filter((col("ngram").isNotNull()) &
                                    (col("ngram") != "") &
                                    ~(col("ngram").rlike(r"^[\s0-9]+$")) &
                                    (col("frequency") > 1))
        return freqlist
        
    def structure_data(self, df, topic):
        print("Begin structuring data...")

        df = df.filter(col("subreddit") == topic)
        df = df.withColumn("timestamp", date_format(col("created").cast("timestamp"), "yyyy-MM-dd hh:mm:ss"))
        df = self.sentiment_analyzer(df)
        return df
        
    def write_stream(self, df, topic: str):
        df.writeStream \
            .format("console")\
            .outputMode("append") \
            .start()
        
        df.writeStream \
            .outputMode("append") \
            .option("failOnDataLoss", "false") \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="comments", keyspace="reddit") \
            .option("checkpointLocation", f"./pyspark-checkpoint/comments/{topic}") \
            .start()

        self.word_freq_analyzer(df).writeStream \
            .foreachBatch(
                lambda batchDF, batchID: batchDF.write.format("org.apache.spark.sql.cassandra") \
                    .option("checkpointLocation", f"./pyspark-checkpoint/freq-table/{topic}") \
                    .options(table="comments_freqtable", keyspace="reddit") \
                    .mode("append").save()
            ).outputMode("complete").start()

    def process_stream(self):
        df = self.read_stream()
        for topic in self.topic_list:
            print(f"Working on topic {topic}")
            df_new = df.alias('df_new')
            df_new = self.structure_data(df_new, topic)
            self.write_stream(df_new, topic)
        
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    topic_list = []
    while os.stat("topics.txt").st_size == 0:
        continue
    
    with open("topics.txt", "r") as f:
        for line in f:
            if line.strip() != "":
                topic_list.append(line.strip())

    consumer = Consumer(topic_list)
    consumer.process_stream()
