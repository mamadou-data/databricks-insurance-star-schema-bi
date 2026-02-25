from pyspark import pipelines as dp
from pyspark.sql.functions import count, sum

@dp.table(
    name="mart_claim_by_fuel",
    comment="Claim frequency by fuel type"
)
def mart_claim_by_fuel():

    fact = spark.read.table("wh_fact_claims")
    dim_fuel = spark.read.table("wh_dim_fuel")

    df = fact.join(dim_fuel, "fuel_key")

    return (
        df.groupBy("fuel_type")
          .agg(
              count("*").alias("nb_policies"),
              sum("claim_status").alias("nb_claims"),
              (sum("claim_status") / count("*")).alias("claim_frequency")
          )
    )