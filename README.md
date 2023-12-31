# Reddit-Realtime-Streaming-Pipeline

## Introduction
This is an end-to-end project dedicated to streaming, processing, and visualizing the sentiment aspects of Reddit. The system can be used to monitor the sentiment trends of any subreddit and gain insights into the opinions and emotions of the community. The main tech stacks involved are:

- Python Reddit API Wrapper (PRAW): A Python library that allows easy access to the Reddit API, and enables crawling live comments from any subreddit.
- Kafka: A distributed streaming system that can handle large throughput of data and acts as a broker between the crawler and the processor.
- PySpark: A Python interface for Apache Spark, a powerful framework for large-scale data processing. 
- Cassandra: A distributed NoSQL database system that provides high availability, scalability, and robustness towards errors. 
- Streamlit: A Python library that simplifies the creation and deployment of web interfaces. 

It utilizes PRAW to crawl live comments from a subreddit to distribute them into Kafka as streams. The streams are then processed in PySpark, and the processed data are written to a Cassandra cluster. Finally, Streamlit reads the data from Cassandra and displays them on a GUI.
  
The streaming pipeline is designed towards simplicity and effectiveness. It is deployed using Docker and docker-compose. 

## Structure

The complete flow of the project is illustrated in the following figure:

![asaaa (5)](https://github.com/Hungreeee/Reddit-Realtime-Streaming-Pipeline/assets/46376260/ae39057e-d5de-4f43-b1b8-6c1b328191c1)

**1. Data Source**

Upon receiving the subreddit name from the user query and API tokens from `credentials.cfg`, PRAW starts a comment stream of the specified subreddit and parses the data in JSON format into a Kakfa producer. The producer then sends the data to the Kafka broker.

**2. Message Broker**

On startup, `docker-compose` automatically creates a specified topic for the comment stream. Through this topic, the Kafka broker then distributes the message from the producer to the downstream consumer.

**3. Stream Processor**

A consumer subscribed to the topic passes the data to PySpark, which is responsible for structuring the JSON data into specific formats and data sets. This includes NLTK sentiment classification, tokenizing, and many other downstream tasks, which are handled by PySpark with reasonably small lags despite their costly complexity. PySpark then starts a continuous stream to write all of the data in batches to a Cassandra cluster. 

**4. Data Storage**

Upon starting, `docker-compose` executes a CQL script to initialize keyspace and tables. The Cassandra cluster, with a simple single-node setup, connects to the PySpark writing stream to receive and store the data. Cassandra is relatively ideal for this job as it can handle the high throughput and low latency requirements of the comments from busier subreddits. 

**5. User Interface**

On startup, the Streamlit interface allows the user to create a query of a subreddit name. This information is fed to the producer and consumer through an event listener, which then triggers them to start working. Finally, through connection to the Cassandra cluster, the interface makes queries continuously and visualizes the data. 

## Example
Below is an example use case of the dashboard with the subreddit [r/worldnews](https://www.reddit.com/r/worldnews/) (speed up):

https://github.com/Hungreeee/Reddit-Realtime-Streaming-Pipeline/assets/46376260/44f79db2-5932-46c5-bd86-6cca67b8410d

Please note that the chosen metrics and visualizations only serve for prototyping purposes, and they may not be meaningful when assessed over a longer period. 

## Installation & Setup

System requirements:
- `cassandra: 4.1.0`
- `kafka: 3.6.1`
- `zookeeper: 3.8.3`
- `pyspark: 3.5.0`

To deploy the project locally, you first have to put your credentials for Reddit API in `credentials.cfg`. The API tokens/keys can be obtained by creating a new application [here](https://old.reddit.com/prefs/apps).

```
[reddit-cred]
client_id = [YOUR APPLICATION ID]
client_secret = [YOUR CLIENT SECRET]
user_agent = [YOUR APPLICATION NAME]
```

You can proceed by starting `docker-compose` to initiate the Zookeeper, Kafka, and Cassandra containers needed for the pipeline using the command:
```
docker-compose up
```

Then, start the producer and consumer using the following commands:
> Unfortunately, because the producer is an *Unstoppable Thread*, you will have to kill the terminal the producer is running on to terminate it.
```
py producer.py
py consumer.py
```

Finally, you use the following command to access the Streamlit user interface. Only after a valid subreddit name is provided as input will the pipeline start operating. 
```
streamlit run streamlit/main.py
```

As a final note, if you want to query a different subreddit name, you will have to restart (terminate and start again) the producer-consumer and refresh Streamlit. 

## Acknowledgements

- Docker images: `wurstmeister/zookeeper`, `wurstmeister/kafka`, `cassandra`.
- Inspired by [nama1arpit/reddit-streaming-pipeline](https://github.com/nama1arpit/reddit-streaming-pipeline/tree/main)

