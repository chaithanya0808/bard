from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from json import dumps
from kafka import KafkaProducer

# Initialize Spark session
spark = SparkSession.builder.appName("AddressHistory").getOrCreate()

# Read customers and address dimension tables
customers_df = spark.read.csv("path/to/customers.csv", header=True)
address_df = spark.read.csv("path/to/address.csv", header=True)

# Define a window specification for customer partitioning
customer_window = Window.partitionBy("party_number").orderBy("change_begin_date")

# Create a new column to determine the address type
customers_df = customers_df.withColumn("address_type_code", F.when(
    (F.col("customer_type_desc") == "individual") & (F.col("street_address_1").isNotNull()),
    "HOM"
).when(
    (F.col("customer_type_desc") == "individual") & (F.col("Mailing_address_1").isNotNull()),
    "COM"
).when(
    (F.col("customer_type_desc") == "organization") & (F.col("street_address_1").isNotNull()),
    "ROA"
).when(
    (F.col("customer_type_desc") == "organization") & (F.col("Mailing_address_1").isNotNull()),
    "COM"
).otherwise("UNKNOWN"))

customers_df = customers_df.withColumn("address_type", F.when(
    F.col("address_type_code") == "HOM", "Home"
).when(
    F.col("address_type_code") == "COM", "Communication Address"
).when(
    F.col("address_type_code") == "ROA", "Registered office address"
).otherwise("UNKNOWN"))

# Create a new column Address_Line_1_text based on the address type
customers_df = customers_df.withColumn("Address_Line_1_text", F.when(
    F.col("address_type_code").isin(["HOM", "COM", "ROA"]),
    F.when(F.col("address_type_code") == "HOM", F.col("street_address_1"))
    .when(F.col("address_type_code") == "COM", F.col("Mailing_address_1"))
    .when(F.col("address_type_code") == "ROA", F.col("street_address_1"))
).otherwise(F.lit(None)))

# Join customers and address tables
joined_df = customers_df.join(address_df, customers_df["party_number"] == address_df["party_entity_number"])

# Define a window specification for address partitioning
address_window = Window.partitionBy("party_entity_number").orderBy("change_begin_date")

# Create a new column indicating whether the address has changed
joined_df = joined_df.withColumn("address_changed", F.when(
    (F.col("street_address_1") != F.lag("street_address_1").over(address_window)) |
    (F.col("street_address_2") != F.lag("street_address_2").over(address_window)) |
    (F.col("Mailing_address_1") != F.lag("Mailing_address_1").over(address_window)) |
    (F.col("Mailing_address_2") != F.lag("Mailing_address_2").over(address_window)),
    1
).otherwise(0))

# Filter rows where address has changed and record_end_ts is not equal to "5999-01-01 00:00:00"
history_df = joined_df.filter((F.col("address_changed") == 1) & (F.col("record_end_ts") != "5999-01-01 00:00:00"))

# Group by party_number and collect records as dictionaries in a list
history_df = history_df.groupBy("party_number").agg(
    F.collect_list(F.struct(
        F.col("address_type_code"),
        F.col("address_type"),
        F.col("Address_Line_1_text"),
        F.col("change_begin_date").alias("address_start_date"),
        F.col("change_end_date").alias("address_end_date")
    )).alias("address_history")
)

# Convert the DataFrame to a list of dictionaries
result_list = history_df.collect()

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='your_bootstrap_servers', value_serializer=lambda x: dumps(x).encode('utf-8'))

# Send each customer's address history to Kafka
for row in result_list:
    customer_id = row.party_number
    address_history = row.address_history
    payload = {
        "customer_id": customer_id,
        "address_history": address_history
    }
    producer.send('your_topic', value=payload)

# Close Kafka producer
producer.close()
