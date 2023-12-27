from json import dumps
import praw
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import configparser

class Producer:
    def __init__(self, credentials: str = "credentials.cfg"):
        config = configparser.ConfigParser()
        config.read_file(open(credentials))
        
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
    
    def stream_data(self, subreddit_name):
        subreddit = self.conn.subreddit(subreddit_name)

        if subreddit_name not in self.admin.list_topics():
            self.admin.create_topics([NewTopic(name=subreddit_name, num_partitions=1, replication_factor=1)])

        for comment in subreddit.stream.comments(skip_existing=True):
            self.producer.send(topic=subreddit_name, value={
                "id": comment.id,
                "author": comment.author.name,
                "body": comment.body, 
                "score": comment.score,
                "created": comment.created,
                "subreddit": comment.subreddit.display_name,
            })
            print(comment.body)
            print()

if __name__ == "__main__":
    producer = Producer()
    producer.stream_data("AskReddit")

