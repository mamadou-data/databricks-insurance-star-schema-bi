from pyspark import pipelines as dp
from pyspark.sql.functions import monotonically_increasing_id

@dp.table(
    name="wh_dim_fuel",
    comment="Fuel type dimension"
)
def wh_dim_fuel():

    df = spark.read.table("silver_claims")

    return (
        df.select("fuel_type")
          .distinct()
          .withColumn("fuel_key", monotonically_increasing_id())
    )