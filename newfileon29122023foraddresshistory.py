import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Create SparkSession
spark = SparkSession.builder.appName("AddressHistory").getOrCreate()

# Load customer and address tables
customer_df = spark.read.parquet("path/to/customers.parquet")
address_df = spark.read.parquet("path/to/address.parquet")

# Join tables on customer ID
joined_df = customer_df.join(address_df, customer_df.customer_id == address_df.party_entity_number)

# Define a schema for the address history payload
address_history_schema = StructType([
   StructField("address_type_code", StringType(), True),
   StructField("address_type", StringType(), True),
   StructField("Address_Line_1_text", StringType(), True),
   StructField("record_start_ts", TimestampType(), True),
   StructField("record_end_ts", TimestampType(), True),
   # Add other necessary fields as needed
])

# Create a function to create address history dictionaries for each customer
def create_address_history(customer_row):
   address_histories = []

   # Handle individual customers
   if customer_row["customer_type_desc"] == "INDIVIDUAL":
       if customer_row["street_address_1"] is not None:
           address_history = {
               "address_type_code": "HOM",
               "address_type": "Home",
               "Address_Line_1_text": customer_row["street_address_1"],
               "record_start_ts": customer_row["record_start_ts"],
               "record_end_ts": customer_row["record_end_ts"],
               # Fill other fields using customer_row
           }
           address_histories.append(address_history)
       if customer_row["Mailing_address_1"] is not None:
           address_history = {
               "address_type_code": "COM",
               "address_type": "Communication Address",
               "Address_Line_1_text": customer_row["Mailing_address_1"],
               "record_start_ts": customer_row["record_start_ts"],
               "record_end_ts": customer_row["record_end_ts"],
               # Fill other fields using customer_row
           }
           address_histories.append(address_history)

   # Handle organization customers
   elif customer_row["customer_type_desc"] == "ORGANIZATION":
       if customer_row["street_address_1"] is not None:
           address_history = {
               "address_type_code": "ROA",
               "address_type": "Registered office address",
               "Address_Line_1_text": customer_row["street_address_1"],
               "record_start_ts": customer_row["record_start_ts"],
               "record_end_ts": customer_row["record_end_ts"],
               # Fill other fields using customer_row
           }
           address_histories.append(address_history)
       if customer_row["Mailing_address_1"] is not None:
           address_history = {
               "address_type_code": "COM",
               "address_type": "Communication Address",
               "Address_Line_1_text": customer_row["Mailing_address_1"],
               "record_start_ts": customer_row["record_start_ts"],
               "record_end_ts": customer_row["record_end_ts"],
               # Fill other fields using customer_row
           }
           address_histories.append(address_history)

   return address_histories

# Apply the function to create address histories for each customer
address_history_df = joined_df.groupBy("customer_id").applyInPandas(create_address_history, schema=address_history_schema)

# Filter for change records and address matches
address_history_df = address_history_df.filter(
   (F.col("record_end_ts") != "5999-01-01 00:00:00
