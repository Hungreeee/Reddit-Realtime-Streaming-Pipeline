# Reddit-Realtime-Pipeline

## Introduction
This is an end-to-end project dedicated to streaming, processing, and visualizing the sentiment aspects of any subreddit on Reddit. It utilizes the Python Reddit API Wrapper (PRAW) to crawl live comments from a subreddit to distribute them into Kafka as streams. The streams are then processed in PySpark, and the processed data are written to a Cassandra cluster. Finally, Streamlit reads the data from Cassandra and displays them in an interface. 

The overall setup is relatively basic, yet it involves many different stages to function correctly. As such, the project is containerized using Docker and docker-compose.  

## Structure

![asaaa (5)](https://github.com/Hungreeee/Reddit-Realtime-Streaming-Pipeline/assets/46376260/ae39057e-d5de-4f43-b1b8-6c1b328191c1)

**1. Data Source**

Upon receiving the subreddit name from the user query and API credentials from `credentials.cfg`, PRAW starts a comment stream of the specified subreddit and parses the data in JSON format into a Kakfa producer. The producer then sends the data to the Kafka broker as a topic. 

**2. Message Broker**

The Kafka broker starts a comment stream 

3. Stream Processor

4. Data Storage

5. User Interface

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

