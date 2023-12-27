CREATE KEYSPACE IF NOT EXISTS reddit 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reddit;

CREATE TABLE IF NOT EXISTS comments(
    id text,
    author text,
    body text,
    score int,
    created float,
    subreddit text,
    PRIMARY KEY(id, created)
)
WITH CLUSTERING ORDER BY (created DESC);

CREATE INDEX IF NOT EXISTS ON comments (created);