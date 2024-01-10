# Compare address fields for each customer within a reasonable time window
window = Window.partitionBy("customer_id").orderBy("records_start_ts")

address_changes = data.withColumn("prev_address1", lag("address1", 1).over(window)) \
                       .withColumn("prev_address2", lag("address2", 1).over(window)) \
                       .withColumn("prev_area", lag("area", 1).over(window)) \
                       .withColumn("prev_pincode", lag("pincode", 1).over(window)) \
                       .withColumn("prev_state_code", lag("state_code", 1).over(window)) \
                       .withColumn("prev_state_name", lag("state_name", 1).over(window)) \
                       .withColumn("prev_country_code", lag("country_code", 1).over(window)) \
                       .withColumn("prev_country_name", lag("country_name", 1).over(window)) \
                       .withColumn("address_changed", 
                                       when(col("address1") != col("prev_address1") |
                                            col("address2") != col("prev_address2") |
                                            col("area") != col("prev_area") |
                                            col("pincode") != col("prev_pincode") |
                                            col("state_code") != col("prev_state_code") |
                                            col("state_name") != col("prev_state_name") |
                                            col("country_code") != col("prev_country_code") |
                                            col("country_name") != col("prev_country_name"), True).otherwise(False))
address_history = address_changes.filter(col("address_changed") == True) \
                                  .select("customer_id", "address1", "address2", "area", "pincode",
                                          "state_code", "state_name", "country_code", "country_name",
                                          "records_start_ts", "record_end_ts")
