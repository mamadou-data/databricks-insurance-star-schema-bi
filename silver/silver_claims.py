from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, regexp_extract, hash, expr

@dp.table(
    name="silver_claims",
    comment="Cleaned insurance data with quality expectations and feature engineering"
)

# -------------------------
# Data Quality Expectations
# -------------------------
@dp.expect("valid_customer_age", "customer_age >= 18")
@dp.expect("valid_vehicle_age", "vehicle_age >= 0")
@dp.expect("valid_claim_status", "claim_status IN (0,1)")

def silver_claims():

    df = spark.read.table("bronze_claims")

    return (
        df
        # -------------------------
        # Ajouter une date technique
        # -------------------------
        .withColumn(
            "ingestion_date",
            expr("date_add('2024-01-01', abs(hash(policy_id)) % 730)")
        )
        # -------------------------
        # Type Casting
        # -------------------------
        .withColumn("customer_age", col("customer_age").cast("int"))
        .withColumn("vehicle_age", col("vehicle_age").cast("int"))
        .withColumn("claim_status", col("claim_status").cast("int"))
        .withColumn("ncap_rating", col("ncap_rating").cast("int"))

        # -------------------------
        # Boolean Normalization
        # -------------------------
        .withColumn("is_esc", when(col("is_esc") == "Yes", 1).otherwise(0))
        .withColumn("is_tpms", when(col("is_tpms") == "Yes", 1).otherwise(0))
        .withColumn("is_parking_camera", when(col("is_parking_camera") == "Yes", 1).otherwise(0))
        .withColumn("is_brake_assist", when(col("is_brake_assist") == "Yes", 1).otherwise(0))
        .withColumn("is_power_steering", when(col("is_power_steering") == "Yes", 1).otherwise(0))

        # -------------------------
        # Torque Parsing
        # Example: 113Nm@4400rpm
        # -------------------------
        .withColumn(
            "torque_nm",
            regexp_extract(col("max_torque"), r"(\d+\.?\d*)Nm", 1).cast("double")
        )
        .withColumn(
            "torque_rpm",
            regexp_extract(col("max_torque"), r"@(\d+)rpm", 1).cast("int")
        )

        # -------------------------
        # Power Parsing
        # Example: 88.50bhp@6000rpm
        # -------------------------
        .withColumn(
            "power_bhp",
            regexp_extract(col("max_power"), r"(\d+\.?\d*)bhp", 1).cast("double")
        )
        .withColumn(
            "power_rpm",
            regexp_extract(col("max_power"), r"@(\d+)rpm", 1).cast("int")
        )

        # -------------------------
        # Remove original text columns
        # -------------------------
        .drop("max_torque", "max_power")

        # -------------------------
        # Remove duplicates
        # -------------------------
        .dropDuplicates(["policy_id"])
    )