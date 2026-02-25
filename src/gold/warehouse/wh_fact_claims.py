from pyspark import pipelines as dp
from pyspark.sql.functions import col, date_format

@dp.table(
    name="wh_fact_claims",
    comment="Fact table for insurance claims"
)
def wh_fact_claims():

    df = spark.read.table("silver_claims")
    dim_region = spark.read.table("wh_dim_region")
    dim_segment = spark.read.table("wh_dim_segment")
    dim_fuel = spark.read.table("wh_dim_fuel")
    dim_ncap = spark.read.table("wh_dim_ncap")
    dim_date = spark.read.table("wh_dim_date")

    df_with_date = df.withColumn(
        "date_key",
        date_format("ingestion_date", "yyyyMMdd").cast("int")
    )

    return (
        df_with_date
          .join(dim_region, "region_code")
          .join(dim_segment, "segment")
          .join(dim_fuel, "fuel_type")
          .join(dim_ncap, "ncap_rating")
          .join(dim_date, "date_key")
          .select(
              "policy_id",
              "region_key",
              "segment_key",
              "fuel_key",
              "ncap_key",
              "date_key",
              "claim_status",
              "customer_age",
              "vehicle_age"
          )
    )