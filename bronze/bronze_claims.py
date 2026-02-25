from pyspark import pipelines as dp

@dp.table(
    name="bronze_claims",
    comment="Raw insurance claims data ingested from Unity Catalog volume"
)
def bronze_claims():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/main/insurance_pipeline/raw_data/")
    )