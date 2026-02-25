from pyspark import pipelines as dp
from pyspark.sql.functions import monotonically_increasing_id

@dp.table(
    name="wh_dim_segment",
    comment="Customer segment dimension"
)
def wh_dim_segment():

    df = spark.read.table("silver_claims")

    return (
        df.select("segment")
          .distinct()
          .withColumn("segment_key", monotonically_increasing_id())
    )