CREATE KEYSPACE IF NOT EXISTS reddit 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};