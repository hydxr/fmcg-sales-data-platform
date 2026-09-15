# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable 


# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/setup_folder/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")


# COMMAND ----------

base_path = f's3://child-comp-dp/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header",True)
    .option("inferSchema",True)
    .load(base_path)
    .withColumn("read_timestamp",F.current_timestamp())
    .select("*","_metadata.file_name","_metadata.file_size")
)
display(df.limit(10))

# COMMAND ----------

df.write\
.format("delta")\
.option("delta.enableChangeDataFeed","true")\
.mode("overwrite")\
.saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source}")
df_bronze.show(10)

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count")>1)
display(df_duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["customer_id"])

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",F.trim(F.col("customer_name")))


# COMMAND ----------

#verification of removing duplicates

display(
    df_silver.filter(F.col("customer_name")!= F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

#typos to correct name

city_mapping = {
    "Bengaluruu":"Bengaluru",
    "Bengalore":"Bengaluru",

    "Hyderabadd":"Hyderabad",
    "Hyderbad":"Hyderabad",

    "NewDelhi":"New Delhi",
    "NewDheli":"New Delhi",
    "NewDelhee":"New Delhi"
}

allowed = ["Bengaluru","Hyderabad","New Delhi"]

df_silver = (

    df_silver
    .replace(city_mapping,subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(),None)
         .when(F.col("city").isin(allowed),F.col("city"))
         .otherwise(None)
    )
)

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(),None)
      .otherwise(F.initcap("customer_name"))
    
    )

# COMMAND ----------

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_name = ["Sprintx Nutrition","Zenathlete Foods","Primefuel Nutrition","Recovery Lane"]
df_silver.filter(F.col("customer_name").isin(null_customer_name)).show(truncate=False)

# COMMAND ----------

#city correction will be done by business teams
customer_city_fix = {
    #Sprintx Nutrition
    789403:"New Delhi"

    #Zenathlete Foods
    ,789404:"Bengaluru"

    #Primefuel Nutrition
    ,789405:"Hyderabad"

    #Recovery Lane
    ,789406:"Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k,v) for k,v in customer_city_fix.items()],
    ["customer_id","fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, on="customer_id",how="left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")   # Replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id",F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "customer",
        F.concat_ws("-","customer_name",F.coalesce(F.col("city"),F.lit("unknown")))
    )
    .withColumn("market",F.lit("India"))
    .withColumn("platform",F.lit("Sports Bar"))
    .withColumn("channel",F.lit("Aquisition"))
)
display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")


# take req cols only
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Merging DataSource with Parent

# COMMAND ----------

delta_table = DeltaTable.forName(spark,"fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dimcustomers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source = df_child_customers.alias("source"),
    condition = "target.customer_code = source.customer_code" 
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

