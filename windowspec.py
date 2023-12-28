import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ... (previous code)

# Detect address changes within each address type
windowSpec = Window.partitionBy("customer_id", "address_typecode").orderBy("record_start_ts")
joined_df = joined_df.withColumn("address_change", F.lead("Address_Line_1_text").over(windowSpec) != F.col("Address_Line_1_text"))

# Create list of dictionaries for each customer, grouping multiple address change records
address_history_dict = {}

def create_address_history(df):
    for row in df.where(F.col("address_change") == True).collect():
        customer_id = row["customer_id"]
        address_dict = {
            "address_typecode": row["address_typecode"],
            "address_type": row["address_type"],
            "Address_Line_1_text": row["Address_Line_1_text"],
            # Add other relevant address fields as needed
        }
        address_history_dict.setdefault(customer_id, []).append(address_dict)

create_address_history(joined_df)

# Now you have the address history with changes as a dictionary of lists in address_history_dict
