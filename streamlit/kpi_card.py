import streamlit as st
import numpy as np

import os
import sys
 
sys.dont_write_bytecode = True
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR + '/../'))


def render(df):
  st.write(
    """
    <style>
    [data-testid="stMetricDelta"] svg {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True,
  )

  col1, col2, col3 = st.columns([0.33, 0.4, 0.27], gap="small")
  with col1:
    mean_sentiment_score = np.mean(df["sentiment_score"])
    st.metric("Sentiment score", np.round(mean_sentiment_score, 2), "overall " + ("positive" 
              if mean_sentiment_score >= 0.05 else "negative" 
              if mean_sentiment_score <= -0.05 else "neutral"), delta_color="off")
  with col2:
    total_cmt = len(df)
    st.metric("Total number of comments", total_cmt, "comments so far", delta_color="off")

  with col3:
    total_user = len(df["author"].unique())
    st.metric("Total number of users", total_user, "users engaged", delta_color="off")


if __name__ == "__main__":
  render()