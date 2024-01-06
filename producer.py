from json import dumps
from threading import Thread
import praw
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import configparser
import os
from time import sleep

class Producer:
    def __init__(self, topic_list: list):
        self.topic_list = topic_list

        config = configparser.ConfigParser()
        config.read_file(open("credentials.cfg"))
        
        self.conn = praw.Reddit(
            client_id=config.get("reddit-cred", "client_id"), 
            client_secret=config.get("reddit-cred", "client_secret"), 
            user_agent=config.get("reddit-cred", "user_agent"))

        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: dumps(x).encode("utf-8"))

        self.admin = KafkaAdminClient(
            bootstrap_servers=["localhost:9092"],
            client_id="admin")
    
    def single_stream(self, topic: str):
        subreddit = self.conn.subreddit(topic)

        # if topic not in self.admin.list_topics():
        #     self.admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])

        for comment in subreddit.stream.comments(skip_existing=True):
            self.producer.send(topic="comments", value={
                "id": comment.id,
                "author": comment.author.name,
                "body": comment.body, 
                "score": comment.score,
                "created": int(comment.created),
                "subreddit": comment.subreddit.display_name.lower(),
                "flair": comment.submission.link_flair_text
            })

            print(comment.body)
            print(comment.created)
            print()

    def stream_data(self):
        process_list = []
        for topic in self.topic_list:
            process = Thread(target=self.single_stream, args=(topic,))
            process.daemon = True
            process.start()
            process_list.append(process)

        for process in process_list:
            process.join()
        
    
if __name__ == "__main__":
    topic_list = []
    while os.stat("topics.txt").st_size == 0:
        sleep(0.5)
    
    with open("topics.txt", "r") as f:
        for line in f:
            if line.strip() != "":
                topic_list.append(line.strip())

    producer = Producer(topic_list)  
    producer.stream_data() 

