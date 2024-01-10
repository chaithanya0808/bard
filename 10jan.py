from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("AddressHistory").getOrCreate()

# Sample data
data = [
    (1, 'Address1', 'Area1', 'Pincode1', 'StateCode1', 'StateName1', 'CountryCode1', 'CountryName1', '2022-01-01', '2022-01-15'),
    (1, 'Address2', 'Area2', 'Pincode2', 'StateCode2', 'StateName2', 'CountryCode2', 'CountryName2', '2022-01-16', '2022-02-28'),
    (1, 'Address3', 'Area3', 'Pincode3', 'StateCode3', 'StateName3', 'CountryCode3', 'CountryName3', '2022-03-01', '2022-04-30'),
    (2, 'AddressA', 'AreaA', 'PincodeA', 'StateCodeA', 'StateNameA', 'CountryCodeA', 'CountryNameA', '2022-01-01', '2022-02-28'),
    (2, 'AddressB', 'AreaB', 'PincodeB', 'StateCodeB', 'StateNameB', 'CountryCodeB', 'CountryNameB', '2022-03-01', '2022-04-30'),
]

# Define schema
schema = ["customer_id", "address1", "area", "pincode", "state_code", "state_name", "country_code", "country_name", "address_start_date", "address_end_date"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Convert date columns to timestamp type
df = df.withColumn("address_start_date", F.to_date(df["address_start_date"]))
df = df.withColumn("address_end_date", F.to_date(df["address_end_date"]))

# Define a window specification
window_spec = Window.partitionBy("customer_id").orderBy("address_start_date")

# Lag function to get the previous row's values
for col in schema[1:]:
    df = df.withColumn(f"prev_{col}", F.lag(col).over(window_spec))

# Compare current and previous values to check for changes
changed_address_df = df.filter(
    F.expr(
        """
        address_start_date > coalesce(prev_address_end_date + 1, '1970-01-01') AND
        (address1 != prev_address1 OR area != prev_area OR pincode != prev_pincode OR
         state_code != prev_state_code OR state_name != prev_state_name OR
         country_code != prev_country_code OR country_name != prev_country_name)
        """
    )
).select(
    "customer_id", "address1", "area", "pincode", "state_code", "state_name", "country_code", "country_name",
    "address_start_date", "address_end_date"
)

# Show the result
changed_address_df.show(truncate=False)

# Stop the Spark session
spark.stop()
