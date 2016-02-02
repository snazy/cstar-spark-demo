-- Cassandra-Spark Demo - Cassandra Days Germany 2016
-- (C) 2016 DataStax

-- note: NEVER EVER use SimpleStrategy in production - only for demos
CREATE KEYSPACE IF NOT EXISTS cstar_day_spark
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cstar_day_spark;

-- cities is just master data
CREATE TABLE cities (
  loc_id          bigint PRIMARY KEY,
  name            text,
  lat             float,
  lon             float,
  population      int,
  area            float
);

-- ... preloaded with some data
COPY cities FROM 'cities.csv';

-- Contains the timestamp of the most recent data in
-- power_by_time and power_by_city tables below.
CREATE TABLE lastwindow (
  dummy           text PRIMARY KEY,
  timewindow      timestamp
) WITH default_time_to_live = 3600;

-- Contains the power consumption for cities partitioned
-- by timestamp (power consumption by date/time).
CREATE TABLE power_by_time (
  timewindow      timestamp,
  --
  city_id         bigint,
  --
  lat             float,
  lon             float,
  name            text,
  population      int,
  area            float,
  power           float,
  --
  PRIMARY KEY ( timewindow, city_id )
) WITH default_time_to_live = 3600;

-- Contains the power consumption for cities partitioned
-- by city (power consumption by city).
CREATE TABLE power_by_city (
  city_id         bigint,
  --
  timewindow      timestamp,
  --
  lat             float       STATIC,
  lon             float       STATIC,
  name            text        STATIC,
  population      int         STATIC,
  area            float       STATIC,
  --
  power           float,
  --
  PRIMARY KEY ( city_id, timewindow )
) WITH CLUSTERING ORDER BY (timewindow DESC)
  AND default_time_to_live = 3600;
