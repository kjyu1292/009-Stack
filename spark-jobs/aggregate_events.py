from pyspark.sql import SparkSession
import clickhouse_connect

spark = SparkSession.builder.appName("aggregate_events").getOrCreate()

client = clickhouse_connect.get_client(
    host='clickhouse', port=8123,
    username='default', password='upbase123',
    database='upbase'
)

pdf = client.query_df("SELECT * FROM upbase.events")
df = spark.createDataFrame(pdf)

agg_df = df.groupBy("event_type").count()
agg_df.show()

result_pdf = agg_df.toPandas()
client.insert_df("event_counts", result_pdf)

spark.stop()
