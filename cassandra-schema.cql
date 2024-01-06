CREATE KEYSPACE IF NOT EXISTS reddit 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS reddit.comments(
    id text,
    author text,
    body text,
    score int,
    created int,
    timestamp timestamp,
    subreddit text,
    sentiment_score float,
    sentiment_tag text,
    flair text,
    cleaned_body list<text>,    
    ng2_body list<text>,
    PRIMARY KEY(timestamp, created, id)
) WITH CLUSTERING ORDER BY (created DESC);

CREATE TABLE IF NOT EXISTS reddit.comments_freqtable(
    ngram text,
    frequency int,
    mean_sentiment float,
    PRIMARY KEY(ngram)
);