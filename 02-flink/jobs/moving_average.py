from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.in_streaming_mode()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

t_env.execute_sql("""
CREATE TABLE ticks (
  `timestamp` TIMESTAMP(3),
  `symbol` STRING,
  `market` STRING,
  `price` DOUBLE,
  `volume` BIGINT,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic-pattern' = '.*',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
)
""")

t_env.execute_sql("""
CREATE TABLE avg_price (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  symbol STRING,
  avg_price DOUBLE
) WITH (
  'connector' = 'print'
)
""")

result = t_env.execute_sql("""
INSERT INTO avg_price
SELECT
  window_start,
  window_end,
  symbol,
  AVG(price) AS avg_price
FROM TABLE(
  TUMBLE(TABLE ticks, DESCRIPTOR(`timestamp`), INTERVAL '1' MINUTE)
)
GROUP BY
  symbol,
  window_start,
  window_end
""")

result.wait()
