-- Single node: RF=1. Real deployments use NetworkTopologyStrategy + RF=3 + LOCAL_QUORUM.
CREATE KEYSPACE IF NOT EXISTS academy
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- WRONG design: partition key is just `service`, a 4-value low-cardinality column.
-- Every row for the same service lands in the same partition, on the same node,
-- forever. This is the unbounded wide partition the lab reproduces.
CREATE TABLE IF NOT EXISTS academy.events_by_service (
    service     text,
    event_id    uuid,
    timestamp   timestamp,
    customer_id text,
    user_id     text,
    endpoint    text,
    region      text,
    latency_ms  int,
    status_code int,
    bytes       int,
    PRIMARY KEY (service, timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, event_id ASC);

-- RIGHT design: partition key is (customer_id, day_bucket). Same customer's
-- traffic is bounded to one day per partition -- growth resets every bucket
-- instead of accumulating forever in a single partition.
CREATE TABLE IF NOT EXISTS academy.events_by_customer_day (
    customer_id text,
    day_bucket  text,
    event_id    uuid,
    timestamp   timestamp,
    service     text,
    user_id     text,
    endpoint    text,
    region      text,
    latency_ms  int,
    status_code int,
    bytes       int,
    PRIMARY KEY ((customer_id, day_bucket), timestamp, event_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, event_id ASC);
