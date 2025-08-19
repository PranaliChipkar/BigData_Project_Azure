# Databricks notebook source
spark

# COMMAND ----------

storage_account = "storage_account_name"
application_id = "application_name"
directory_id = "directory_name"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net","secret_key")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

file_path = "abfss://olistdatacontainer@olistdatastorageaccount2.dfs.core.windows.net/bronze/olist_customers_dataset.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
display(df)

# COMMAND ----------


base_path="abfss://olistdatacontainer@olistdatastorageaccount2.dfs.core.windows.net/bronze/"
orders_path= base_path + "olist_orders_dataset.csv"
payments_path= base_path + "olist_order_payments_dataset.csv"
reviews_path= base_path + "olist_order_reviews_dataset.csv"
items_path=base_path + "olist_order_items_dataset.csv"
customers_path=base_path + "olist_customers_dataset.csv"
sellers_path=base_path + "olist_sellers_dataset.csv"
geolocation_path=base_path + "olist_geolocation_dataset.csv"
products_path=base_path + "olist_products_dataset.csv"


orders_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(orders_path)
payments_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(payments_path)
reviews_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(reviews_path)
items_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(items_path)
customers_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(customers_path)
sellers_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(sellers_path)
geolocation_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(geolocation_path)
products_df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(products_path)
                                                                                                    

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "pi4aaj.h.filess.io"
database = "olistDataNoSQL_topiclogit"
port = "27018"
username = "olistDataNoSQL_topiclogit"
password = "8d922e1bed64524fe485b79e2bdb589f19a74e77"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]


# COMMAND ----------

import pandas as pd
collection = mydatabase["product_categories"]

mongo_data= pd.DataFrame(list(collection.find()))
mongo_data.head()

# COMMAND ----------

display(products_df)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, current_date,when

# COMMAND ----------

def clean_dataframe(df,name):
    print(f"Cleaning {name}...")
    return df.dropDuplicates().na.drop('all')
    
orders_df=clean_dataframe(orders_df,"orders")
display(orders_df)


# COMMAND ----------

#convert Date columns

orders_df=orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp")))\
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date")))\
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))
                                                            

# COMMAND ----------


# Calculate Delivery and Time Delays

orders_df = orders_df.withColumn(
    "actual_delivery_time", 
    datediff("order_delivered_customer_date", "order_purchase_timestamp")
)
orders_df = orders_df.withColumn(
    "estimated_delivery_time", 
    datediff("order_estimated_delivery_date", "order_purchase_timestamp")
)
orders_df = orders_df.withColumn(
    "Delay Time", 
    (col("actual_delivery_time") - col("estimated_delivery_time"))
)

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Joining

# COMMAND ----------



orders_customers_df=orders_df.join(customers_df,orders_df.customer_id==customers_df.customer_id,"left")

orders_payments_df=orders_customers_df.join(payments_df,orders_customers_df.order_id==payments_df.order_id, "left")

order_items_df=orders_payments_df.join(items_df,"order_id","left")

order_items_products_df=order_items_df.join(products_df,order_items_df.product_id==products_df.product_id,"left")

final_df=order_items_products_df.join(sellers_df,order_items_products_df.seller_id==sellers_df.seller_id,"left")




# COMMAND ----------



display(final_df)

# COMMAND ----------


mongo_data.drop('_id', axis=1, inplace=True)

mongo_spark_df = spark.createDataFrame(mongo_data)

display(mongo_spark_df)

# COMMAND ----------

display(mongo_spark_df)

# COMMAND ----------


final_df=final_df.join(mongo_spark_df,"product_category_name","left")

# COMMAND ----------


display(final_df)

# COMMAND ----------


def remove_duplicate_columns(df):
    columns = df.columns

    seen_columns = set()
    columns_to_drop=[]

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)
            
    df_cleaned=df.drop(*columns_to_drop)
    return df_cleaned

final_df=remove_duplicate_columns(final_df)

# COMMAND ----------


final_df.write.mode("overwrite").parquet("abfss://olistdatacontainer@olistdatastorageaccount2.dfs.core.windows.net/silver")