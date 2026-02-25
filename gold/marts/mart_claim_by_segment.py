from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum

@dp.table(
    name="mart_claim_by_segment",
    comment="Claim frequency by customer segment"
)
def mart_claim_by_segment():

    fact = spark.read.table("wh_fact_claims")
    dim_segment = spark.read.table("wh_dim_segment")

    df = fact.join(dim_segment, "segment_key")

    return (
        df.groupBy("segment")
          .agg(
              count("*").alias("nb_policies"),
              sum("claim_status").alias("nb_claims"),
              (sum("claim_status") / count("*")).alias("claim_frequency")
          )
    )