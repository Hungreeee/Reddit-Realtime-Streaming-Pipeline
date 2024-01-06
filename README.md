# Reddit-Realtime-Pipeline

## Introduction
This is an end-to-end project dedicated to streaming, processing, and visualizing the sentiment aspects of any subreddit on Reddit. It utilizes the Python Reddit API Wrapper (PRAW) to crawl live comments from a subreddit to distribute them into Kafka as streams. The streams are then processed in PySpark, and the processed data are written to a Cassandra cluster. Finally, Streamlit reads the data from Cassandra and displays them in an interface. 

The overall setup is relatively basic, yet it involves many different stages to function correctly. As such, the project is containerized using Docker and docker-compose.  

## The pipeline

![asaaa (4)](https://github.com/Hungreeee/Reddit-Realtime-Streaming-Pipeline/assets/46376260/d4f9c512-757d-470e-8708-626dbf8cf7ad)

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

