import pyspark.sql.functions as F

# Load the customers and address tables
customers_df = spark.read.parquet("path/to/customers_table")
address_df = spark.read.parquet("path/to/address_table")

# Join the tables on customer_id and party_entity_number
joined_df = customers_df.join(address_df, customers_df.customer_id == address_df.party_entity_number)

# Define a function to process address types for each customer
def process_address_types(row):
   customer_id = row["customer_id"]
   address_history = []

   if row["customer_type_desc"] == "Individual":
       if row["street_address_1"] is not None:
           address_history.append({
               "address_type_code": "HOM",
               "address_type": "Home",
               "Address_Line_1_text": row["street_address_1"],
               "record_start_ts": row["record_start_ts"],
               "record_end_ts": row["record_end_ts"],
               "address_start_date": row["address_start_date"],
               "address_end_date": row["address_end_date"]
           })
       if row["Mailing_address_1"] is not None:
           address_history.append({
               "address_type_code": "COM",
               "address_type": "Communication Address",
               "Address_Line_1_text": row["Mailing_address_1"],
               "record_start_ts": row["record_start_ts"],
               "record_end_ts": row["record_end_ts"],
               "address_start_date": row["address_start_date"],
               "address_end_date": row["address_end_date"]
           })
   elif row["customer_type_desc"] == "Organization":
       if row["Address_Line_1_text"] is not None:
           address_history.append({
               "address_type_code": "ROA",
               "address_type": "Registered office address",
               "Address_Line_1_text": row["Address_Line_1_text"],
               "record_start_ts": row["record_start_ts"],
               "record_end_ts": row["record_end_ts"],
               "address_start_date": row["address_start_date"],
               "address_end_date": row["address_end_date"]
           })
       if row["Mailing_address_1"] is not None:
           address_history.append({
               "address_type_code": "COM",
               "address_type": "Communication Address",
               "Address_Line_1_text": row["Mailing_address_1"],
               "record_start_ts": row["record_start_ts"],
               "record_end_ts": row["record_end_ts"],
               "address_start_date": row["address_start_date"],
               "address_end_date": row["address_end_date"]
           })

   return (customer_id, address_history)

# Apply the function to create list of dictionaries for each customer
result_df = joined_df.groupBy("customer_id").apply(process_address_types).toDF("customer_id", "address_history")

# Display the results
result_df.show()
