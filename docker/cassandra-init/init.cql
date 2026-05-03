CREATE KEYSPACE IF NOT EXISTS clickstream_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE clickstream_analytics;

-- Salt bucket represents hash(product_id) % 16 or similar to prevent hot partitions
CREATE TABLE IF NOT EXISTS agg_by_product_1m (
    product_id text,
    salt_bucket int,
    window_start timestamp,
    window_end timestamp,
    page_views_count counter,
    adds_to_cart_count counter,
    purchases_count counter,
    PRIMARY KEY ((product_id, salt_bucket), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS agg_by_category_1h (
    category_id text,
    window_start timestamp,
    window_end timestamp,
    page_views_count counter,
    purchases_count counter,
    PRIMARY KEY (category_id, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS sessions_by_user (
    user_id text,
    session_id text,
    session_start_ts timestamp,
    session_duration_sec int,
    event_count int,
    is_purchased boolean,
    PRIMARY KEY (user_id, session_start_ts)
) WITH CLUSTERING ORDER BY (session_start_ts DESC);
