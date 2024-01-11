from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder.appName("AlertCaseProcessing").getOrCreate()

# Define your S3 Delta table path
delta_table_path = "s3://your_s3_bucket/path/to/delta_table"

# Define your Oracle connection details
oracle_connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "oracle.jdbc.driver.OracleDriver",
    "url": "jdbc:oracle:thin:@your_oracle_host:your_oracle_port:your_oracle_sid"
}

# Function to process Alert and Case data
def process_alert_and_case_data(alert_data, case_data, batch_start, batch_end):
    # Your logic to process Alert data
    processed_alert_data = alert_data.withColumn("processed_column_alert", col("original_column_alert") * 2)

    # Your logic to process Case data
    processed_case_data = case_data.withColumn("processed_column_case", col("original_column_case") + 10)

    return processed_alert_data, processed_case_data

# Read Delta table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Get the current SCN from v$database
current_scn = spark.read.jdbc(
    url=oracle_connection_properties["url"],
    table="v$database",
    properties=oracle_connection_properties
).select("CURRENT_SCN").first()[0]

# Process data in batches of 100,000
batch_size = 100000
for batch_start in range(0, current_scn, batch_size):
    batch_end = min(batch_start + batch_size, current_scn)

    # Read data from Oracle for the current batch
    alert_data = spark.read.jdbc(
        url=oracle_connection_properties["url"],
        table="your_alert_table",
        predicates=[f"SCN BETWEEN {batch_start} AND {batch_end}"],
        properties=oracle_connection_properties
    )

    case_data = spark.read.jdbc(
        url=oracle_connection_properties["url"],
        table="your_case_table",
        predicates=[f"SCN BETWEEN {batch_start} AND {batch_end}"],
        properties=oracle_connection_properties
    )

    # Process Alert and Case data
    processed_alert_data, processed_case_data = process_alert_and_case_data(alert_data, case_data, batch_start, batch_end)

    # Combine Alert and Case data
    combined_data = processed_alert_data.join(processed_case_data, "common_key_column", "inner")

    # Update Delta table with combined processed data
    delta_table.alias("dt").merge(
        source=combined_data.alias("s"),
        condition="dt.key_col = s.key_col",
        whenMatchedUpdate={"column1": "s.column1", "column2": "s.column2"}
    ).execute()

    # Update Delta table with the processed SCN
    delta_table.update("SCN >= {0} AND SCN <= {1}".format(batch_start, batch_end), {"processed_scn": batch_end})

# Stop Spark session
spark.stop()
