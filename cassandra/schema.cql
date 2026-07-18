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

CREATE TABLE IF NOT EXISTS rdg.metrics_ts (
  plant_id text,
  metric text,
  bucket date,
  ts timestamp,
  value double,
  PRIMARY KEY ((plant_id, metric, bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC);