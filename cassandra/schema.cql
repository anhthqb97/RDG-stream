CREATE KEYSPACE IF NOT EXISTS rdg
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
