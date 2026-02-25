from pyspark import pipelines as dp
from pyspark.sql.functions import monotonically_increasing_id

@dp.table(
    name="wh_dim_region",
    comment="Region dimension table"
)
def wh_dim_region():

    df = spark.read.table("silver_claims")

    return (
        df.select("region_code")
          .distinct()
          .withColumn("region_key", monotonically_increasing_id())
    )