CREATE KEYSPACE IF NOT EXISTS rdg
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS rdg.metrics_current (
  plant_id text,
  metric text,
  value double,
  unit text,
  updated_at timestamp,
  PRIMARY KEY ((plant_id), metric)
);
