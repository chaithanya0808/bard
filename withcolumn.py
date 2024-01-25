from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Create a Spark session
spark = SparkSession.builder.appName("AddressTypeCode").getOrCreate()

# Sample data
data = [
    ("John Doe", "INDIVIDUAL", "street_address"),
    ("Jane Doe", "INDIVIDUAL", "mail_address"),
    ("Acme Corp", "ORGANIZATION", "street_address"),
    ("Tech Inc", "ORGANIZATION", "mail_address"),
]

# Define schema
schema = ["name", "party_type", "type"]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Define conditions and populate the new column
df = df.withColumn(
    "address_type_code",
    when((col("type") == "street_address") & (col("party_type") == "INDIVIDUAL"), "HOM")
    .when((col("type") == "mail_address") & (col("party_type") == "INDIVIDUAL"), "COM")
    .when((col("type") == "street_address") & (col("party_type") == "ORGANIZATION"), "ROA")
    .when((col("type") == "mail_address") & (col("party_type") == "ORGANIZATION"), "COM")
    .otherwise(None)  # Set default value or use `lit('')` if you want an empty string
)

# Show the result
df.show(truncate=False)

# Stop the Spark session
spark.stop()
