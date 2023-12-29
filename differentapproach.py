import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# ... (previous code for SparkSession, loading tables, and schema definition)

# Join tables and create address history columns
address_history_df = joined_df.withColumn(
    "address_type_code",
    F.when(
        (F.col("customer_type_desc") == "INDIVIDUAL") & F.col("street_address_1").isNotNull(),
        "HOM",
    ).when(
        (F.col("customer_type_desc") == "ORGANIZATION") & F.col("street_address_1").isNotNull(),
        "ROA",
    ).when(
        F.col("Mailing_address_1").isNotNull(), "COM"
    )
).withColumn(
    "address_type",
    F.when(
        F.col("address_type_code") == "HOM", "Home"
    ).when(
        F.col("address_type_code") == "ROA", "Registered office address"
    ).when(
        F.col("address_type_code") == "COM", "Communication Address"
    )
).withColumn(
    "Address_Line_1_text",
    F.when(
        F.col("address_type_code") == "HOM", F.col("street_address_1")
    ).when(
        F.col("address_type_code") == "ROA", F.col("street_address_1")
    ).when(
        F.col("address_type_code") == "COM", F.col("Mailing_address_1")
    )
).withColumn(
    "address_start_date", F.col("record_start_ts")
).withColumn(
    "address_end_date", F.col("record_end_ts")
)

# Filter for address changes and collect as a list of dictionaries
address_history_list = (
    address_history_df.filter(
        F.col("record_end_ts") != "5999-01-01 00:00:00"  # Filter for non-terminated records
    )
    .groupBy("customer_id")
    .agg(
        F.collect_list(
            F.struct(
                "address_type_code",
                "address_type",
                "Address_Line_1_text",
                "address_start_date",
                "address_end_date",
            )
        ).alias("address_history")
    )
    .select("customer_id", "address_history")
    .collect()
)

print(address_history_list)
