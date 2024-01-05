import os
import sys
import plotly.express as px
import kpi_card
import datetime
import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
from time import sleep
 
sys.dont_write_bytecode = True
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR + '/../'))

st.set_page_config(layout="wide")

def color_highlight1(val):
  color = "green" if val == "positive" else "red" if val == "negative" else "orange" 
  return f'color: {color}'

def color_highlight2(val):
  color = "green" if val >= 0.05 else "red" if val <= -0.05 else "orange" 
  return f'color: {color}'

def disable():
    st.session_state["disabled"] = True

def read_cassandra(cluster, topic_name):
  session = cluster.connect()
  while True:
    try:
      check = pd.DataFrame(list(session.execute(f"SELECT * from reddit.{topic_name}")))
      check = pd.DataFrame(list(session.execute(f"SELECT * from reddit.{topic_name}_freqtable")))
      break
    except Exception:
      print("Retrying connection to table...")

  df2 = pd.DataFrame(list(session.execute(f"SELECT body, sentiment_tag, sentiment_score, author, timestamp from reddit.{topic_name}")))

  df1 = pd.DataFrame(list(session.execute(f"""
        SELECT 
          timestamp, 
          SUM(sentiment_score) / COUNT(sentiment_score) AS sentiment_score
        FROM reddit.{topic_name}
        GROUP BY timestamp;
        """)))
    
  df = pd.DataFrame(list(session.execute(f"""
        SELECT 
          ngram,
          SUM(frequency) AS frequency,
          SUM(frequency * mean_sentiment) / SUM(frequency) AS mean_sentiment
        FROM reddit.{topic_name}_freqtable
        GROUP BY ngram;
        """)))
  
  if not df.empty:
    df = df.sort_values("frequency", ascending=False)

  if not df1.empty:
    df1 = df1.sort_values("timestamp", ascending=False)

  if not df2.empty:
    df2 = df2.sort_values("timestamp", ascending=False)

  return (df2, df1, df)

def render(cluster):
  st.title("Testing program")

  if "disabled" not in st.session_state:
    st.session_state["disabled"] = False

  topic_name = st.text_input("Subreddit", 
                              placeholder="subreddit name",
                              disabled=st.session_state.disabled, 
                              on_change=disable).strip().lower()

  if topic_name != "":
    st.caption(f"You choose topic {topic_name}")
    with open(f'{CURRENT_DIR}/../topics.txt', 'w') as f:
      f.write(f"{topic_name}\n")

    placeholder = st.empty()

    while(True):
      with placeholder.container():
        df2, df1, df = read_cassandra(cluster, topic_name)
        if not (df.empty or df1.empty or df2.empty):
          cen1, cen2, cen3 = st.columns([0.15, 0.8, 0.05], gap="medium")
          with cen2:
            kpi_card.render(df2)

          col1, col2 = st.columns([0.5, 0.5], gap="small")
          with col2: 
            fig = px.line(df1, x="timestamp", y="sentiment_score", markers=True, title="Mean sentiment per second")
            if df1["timestamp"].iloc[0] - datetime.timedelta(seconds=30) >= df1["timestamp"].iloc[-1]:
              fig.update_xaxes(type="date", range=[df1["timestamp"].iloc[0] - datetime.timedelta(seconds=31), df1["timestamp"].iloc[0] + datetime.timedelta(seconds=1)])
            st.plotly_chart(fig)

          with col1:
            st.markdown("###### Comment stream")
            st.dataframe(df2[["body", "sentiment_tag"]].style.applymap(color_highlight1, subset=["sentiment_tag"]), hide_index=True)

          col3, col4, col5 = st.columns([0.23, 0.52, 0.3], gap="small")
          with col3:
            st.markdown("###### Top words")
            st.dataframe(df.style.map(color_highlight2, subset=["mean_sentiment"]), hide_index=True)

          with col4:
            df21 = df2.copy()
            df21["timestamp"] = df21["timestamp"].dt.floor('Min')
            df_sen_pro = df21.groupby(by=["timestamp", "sentiment_tag"]).size().reset_index(name='counts')
            fig = px.line(df_sen_pro, 
                          x="timestamp", 
                          y="counts", 
                          color="sentiment_tag",
                          color_discrete_map={
                                  'positive':'green',
                                  'neutral':'orange',
                                  'negative':'red'}, 
                          markers=True, 
                          title="Number of comments per sentiment per minute")
            fig.update_layout(showlegend=False)
            if df_sen_pro["timestamp"].iloc[0] - datetime.timedelta(minutes=10) >= df_sen_pro["timestamp"].iloc[-1]:
              fig.update_xaxes(type="date", range=[df_sen_pro["timestamp"].iloc[0] - datetime.timedelta(minutes=30.5), df_sen_pro["timestamp"].iloc[0] + datetime.timedelta(minutes=1)])
            st.plotly_chart(fig)

          with col5:
            per_pos_cmt = len(df2[df2["sentiment_tag"] == "positive"])
            per_neg_cmt = len(df2[df2["sentiment_tag"] == "negative"])
            per_neu_cmt = len(df2[df2["sentiment_tag"] == "neutral"])
            fig = px.pie(values=[per_pos_cmt, per_neu_cmt, per_neg_cmt], \
                        names=["Positive", "Neutral", "Negative"], 
                        color=["Positive", "Neutral", "Negative"],
                        color_discrete_map={
                                  'Positive':'green',
                                  'Neutral':'orange',
                                  'Negative':'red'}, 
                        title="Sentiment disitribution")
            fig.update_layout(showlegend=False, width=400)
            st.plotly_chart(fig)
    
      sleep(1)
    

if __name__ == "__main__":
  cluster = Cluster(['127.0.0.1'], port=9042)
  render(cluster)