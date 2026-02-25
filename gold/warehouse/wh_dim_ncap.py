from pyspark import pipelines as dp
from pyspark.sql.functions import monotonically_increasing_id

@dp.table(
    name="wh_dim_ncap",
    comment="Vehicle safety NCAP dimension"
)
def wh_dim_ncap():

    df = spark.read.table("silver_claims")

    return (
        df.select("ncap_rating")
          .distinct()
          .withColumn("ncap_key", monotonically_increasing_id())
    )