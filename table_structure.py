import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder.appName("AddressHistory").getOrCreate()

# Define the schema for the customers table
customers_schema = StructType([
   StructField("customer_id", StringType(), True),
   StructField("record_start_ts", TimestampType(), True),
   StructField("record_end_ts", TimestampType(), True),
   StructField("customer_type_desc", StringType(), True),
   StructField("street_address_1", StringType(), True),
   StructField("street_address_2", StringType(), True),
   StructField("Street_city_name", StringType(), True),
   StructField("Street_state_code", StringType(), True),
   StructField("street_state_name", StringType(), True),
   StructField("street_postal_code", StringType(), True),
   StructField("street_country_code", StringType(), True),
   StructField("street_country_name", StringType(), True),
   StructField("Mailing_address_1", StringType(), True),
   StructField("Mailing_address_2", StringType(), True),
   StructField("Mailing_city_name", StringType(), True),
   StructField("Mailing_state_code", StringType(), True),
   StructField("Mailing_state_name", StringType(), True),
   StructField("Mailing_postal_code", StringType(), True),
   StructField("mailing_country_code", StringType(), True),
   StructField("mailing_country_name", StringType(), True),
   StructField("residence_country_code", StringType(), True),
   StructField("residence_country_name", StringType(), True)
])

# Define the schema for the address table
address_schema = StructType([
   StructField("party_entity_number", StringType(), True),
   StructField("address_line_1_text", StringType(), True),
   StructField("address_line2_text", StringType(), True),
   StructField("city_name", StringType(), True),
   StructField("state_code", StringType(), True),
   StructField("state_name", StringType(), True),
   StructField("postal_code", StringType(), True),
   StructField("org_country_of_business_code", StringType(), True),
   StructField("org_country_of_business_name", StringType(), True),
   StructField("country_code", StringType(), True),
   StructField("country_name", StringType(), True),
   StructField("address_start_date", TimestampType(), True),
   StructField("address_end_date", TimestampType(), True)
])

# Read the customers and address data
customers_df = spark.read.schema(customers_schema).parquet("path/to/customers/data")
address_df = spark.read.schema(address_schema).parquet("path/to/address/data")

# Join the tables on customer_id and party_entity_number
joined_df = customers_df.join(address_df, customers_df.customer_id == address_df.party_entity_number)

# Derive address_type_code and address_type based on customer type and address fields
def derive_address_type(customer_type, address_1, address_2):
   if customer_type == "Individual":
       if address_1 is not None:
           return "HOM", "Home", address_1
       elif address_2 is not None:
           return "COM", "Communication Address", address_2
   else:  # Organization
       if address_1 is not None:
           return "ROA", "Registered office address", address_1
       elif address_2 is not None:
           return "COM", "Communication Address", address_2
   return None, None, None

udf_derive_address_type = F.udf(derive_address_type, StructType([
   StructField("address_type_code", StringType()),
   StructField("address_type", StringType()),
   StructField("Address_Line_1_text", StringType())
]))
