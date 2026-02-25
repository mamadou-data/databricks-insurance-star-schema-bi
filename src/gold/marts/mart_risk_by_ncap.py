from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum

@dp.table(
    name="mart_risk_by_ncap",
    comment="Claim frequency by vehicle NCAP safety rating"
)
def mart_risk_by_ncap():

    fact = spark.read.table("wh_fact_claims")
    dim_ncap = spark.read.table("wh_dim_ncap")

    df = fact.join(dim_ncap, "ncap_key")

    return (
        df.groupBy("ncap_rating")
          .agg(
              count("*").alias("nb_policies"),
              sum("claim_status").alias("nb_claims"),
              (sum("claim_status") / count("*")).alias("claim_frequency")
          )
    )