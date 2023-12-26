import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Create SparkSession
spark = SparkSession.builder.appName("AddressHistory").getOrCreate()

# Define schemas (replace with actual column names)
customer_schema = StructType([
    # ... customer fields as described
])
address_schema = StructType([
    # ... address fields as described
])

# Read customer and address data
customer_df = spark.read.schema(customer_schema).csv("path/to/customer_data")
address_df = spark.read.schema(address_schema).csv("path/to/address_data")

# Join tables on customer_id and party_entity_number
joined_df = customer_df.join(address_df,
                              F.col("customer_id") == F.col("party_entity_number"),
                              how="inner")

# Derive address type code and address type
joined_df = joined_df.withColumn("address_typecode",
                                 F.when((F.col("customer_type_desc") == "Individual") & F.col("street_address_1").isNotNull(), "HOM")
                                   .when((F.col("customer_type_desc") == "Organization") & F.col("street_address_1").isNotNull(), "ROA")
                                   .when((F.col("customer_type_desc") == "Individual") & F.col("Mailing_address_1").isNotNull(), "COM")
                                   .when((F.col("customer_type_desc") == "Organization") & F.col("Mailing_address_1").isNotNull(), "COM")
                                   .otherwise("UNK"))
joined_df = joined_df.withColumn("address_type",
                                 F.when(F.col("address_typecode") == "HOM", "Home Address")
                                   .when(F.col("address_typecode") == "COM", "Postal Address")
                                   .when(F.col("address_typecode") == "ROA", "Registered Office Address")
                                   .otherwise("Unknown Address Type"))

# Compare addresses and derive address_line1_text
joined_df = joined_df.withColumn("address_line1_text",
                                 F.when(F.col("address_typecode") == "HOM",
                                        F.coalesce(F.col("street_address_1"), F.col("street_address_2")))
                                   .when(F.col("address_typecode") == "COM",
                                        F.coalesce(F.col("Mailing_address_1"), F.col("Mailing_address_2")))
                                   .otherwise(F.col("address_line_1_text")))

# Identify address changes within each customer group using window functions
windowSpec = Window.partitionBy("customer_id", "address_typecode").orderBy("record_start_ts")
joined_df = joined_df.withColumn("address_change", F.lead("address_line1_text").over(windowSpec) != F.col("address_line1_text"))

# Filter for address changes and create address history
individual_address_changes = joined_df.filter(F.col("customer_type_desc") == "Individual").filter("address_change")
organization_address_changes = joined_df.filter(F.col("customer_type_desc") == "Organization").filter("address_change")

address_history_array = []

def create_address_history(df):
    for row in df.collect():
        customer_id = row["customer_id"]
        address_history = {
            "customer_id": customer_id,
            "customer_type": row["customer_type_desc"],
            "addresses": {}
        }

        address_type = row["address_type"]
        address_history["addresses"][address_type] = {
            "address_line1_text": row["address_line1_text"],
            #
