import pyspark.sql.functions as F

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
                                   .when((F.col("customer_type_desc") == "Individual") & F.col("Mailing_address_1").isNotNull(), "COM")
                                   .when((F.col("customer_type_desc") == "Organization") & F.col("address_line_1_text").isNotNull(), "ROA")
                                   .when((F.col("customer_type_desc") == "Organization") & F.col("Mailing_address_1").isNotNull(), "COM")
                                   .otherwise("UNK"))
joined_df = joined_df.withColumn("address_type",
                                 F.when(F.col("address_typecode") == "HOM", "Home")
                                   .when(F.col("address_typecode") == "COM", "Communication Address")
                                   .when(F.col("address_typecode") == "ROA", "Registered office address")
                                   .otherwise("Unknown Address Type"))

# Populate Address_Line_1_text based on address_typecode
joined_df = joined_df.withColumn("Address_Line_1_text",
                                 F.when(F.col("address_typecode") == "HOM", F.col("street_address_1"))
                                   .when(F.col("address_typecode") == "COM", F.col("Mailing_address_1"))
                                   .otherwise(F.col("address_line_1_text")))

# Create list of dictionaries for each customer
address_history_array = []

def create_address_history(df):
    for row in df.collect():
        customer_id = row["customer_id"]
        address_history = []
        address_dict = {
            "address_typecode": row["address_typecode"],
            "address_type": row["address_type"],
            "Address_Line_1_text": row["Address_Line_1_text"],
            # Add other relevant address fields as needed
        }
        address_history.append(address_dict)
        address_history_array.append({"customer_id": customer_id, "addresses": address_history})

create_address_history(joined_df)

# Now you have the address history as a list of dictionaries in address_history_array
