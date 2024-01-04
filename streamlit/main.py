import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
from time import sleep
import os
import sys
import time
 
sys.dont_write_bytecode = True
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR + '/../'))

from producer import Producer
from consumer import Consumer

def read_cassandra(cluster, topic_name, timestamp):
  session = cluster.connect()
  # df = pd.DataFrame(list(session.execute(f"SELECT * FROM reddit.{topic_name} WHERE created > {timestamp}")))  
  df = pd.DataFrame(list(session.execute(f"SELECT * FROM reddit.{topic_name}_freqtable;")))

  return df

def render(cluster):
  st.title("Testing program")
  topic_name = st.text_input("Subreddit", placeholder="subreddit name").strip()

  if topic_name != "":
    st.write(f"You choose topic {topic_name}")
    
    with open(f'{CURRENT_DIR}/../topics.txt', 'w') as f:
      f.write(f"{topic_name}\n")
    
    while(True):
      df = read_cassandra(cluster, topic_name, time.time())
      st.write(df)
      sleep(1)
    

if __name__ == "__main__":
  cluster = Cluster(['127.0.0.1'], port=9042)
  render(cluster)