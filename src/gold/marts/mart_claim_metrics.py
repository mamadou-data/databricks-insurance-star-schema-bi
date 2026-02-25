from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum, avg

@dp.table(
    name="mart_claim_metrics",
    comment="Insurance business KPIs - claim frequency analysis"
)
def mart_claim_metrics():

    fact = spark.read.table("wh_fact_claims")
    dim_region = spark.read.table("wh_dim_region")

    df = fact.join(dim_region, "region_key")

    return (
        df.groupBy("region_code")
          .agg(
              count("*").alias("nb_policies"),
              sum("claim_status").alias("nb_claims"),
              (sum("claim_status") / count("*")).alias("claim_frequency"),
              avg("customer_age").alias("avg_customer_age"),
              avg("vehicle_age").alias("avg_vehicle_age")
          )
    )