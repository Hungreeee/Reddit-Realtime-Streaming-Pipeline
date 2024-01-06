# Reddit-Realtime-Pipeline

## Introduction
This is an end-to-end project dedicated to streaming, processing, and visualizing the sentiment aspects of any subreddit on Reddit. It utilizes the Python Reddit API Wrapper (PRAW) to crawl live comments from a subreddit to distribute them into Kafka as streams. The streams are then processed in PySpark, and the processed data are written to a Cassandra cluster. Finally, Streamlit reads the data from Cassandra and displays them in an interface. 

The overall setup is relatively basic, yet it involves many different stages to function correctly. As such, the project is containerized using Docker and docker-compose.  

## Example


## Structure

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

## Installation & Setup

```
[reddit-cred]
client_id = 
client_secret = 
user_agent = 
```

```
docker-compose up
```

```
py producer.py
```

```
py consumer.py
```

```
streamlit run streamlit/main.py
```

## Acknowledgements
Inspired greatly by [nama1arpit/reddit-streaming-pipeline](https://github.com/nama1arpit/reddit-streaming-pipeline/tree/main)

