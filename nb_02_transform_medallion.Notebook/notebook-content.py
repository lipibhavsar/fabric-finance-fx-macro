# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "922f950f-5aec-4c3c-8387-bc8532f2e77c",
# META       "default_lakehouse_name": "lh_finance_fx_macro",
# META       "default_lakehouse_workspace_id": "24fc40af-3cf5-495e-be3e-02846833798a",
# META       "known_lakehouses": [
# META         {
# META           "id": "922f950f-5aec-4c3c-8387-bc8532f2e77c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from delta.tables import DeltaTable

# 1. Explicit Schema Enforcement & Data Quality Quarantine
fx_schema = StructType([
    StructField("rate_date", StringType(), True),
    StructField("base_currency", StringType(), True),
    StructField("target_currency", StringType(), True),
    StructField("exchange_rate", DoubleType(), True),
    StructField("ingested_at", StringType(), True)
])

raw_fx = spark.read.schema(fx_schema).json("Files/bronze/fx/*.json")

valid_rule = (
    F.col("rate_date").isNotNull() &
    F.col("target_currency").isNotNull() &
    (F.col("exchange_rate") > 0)
)

clean_fx = (
    raw_fx.filter(valid_rule)
    .withColumn("rate_date", F.to_date("rate_date"))
    .withColumn("ingested_ts", F.to_timestamp("ingested_at"))
    .dropDuplicates(["rate_date", "base_currency", "target_currency"])
)

bad_fx = raw_fx.filter(~valid_rule)
if bad_fx.count() > 0:
    bad_fx.write.mode("append").format("delta").saveAsTable("quarantine_fx_rates")
    print(f"Quarantined {bad_fx.count()} invalid FX rows.")

# 2. Silver Layer: Idempotent Delta MERGE (Upsert)
if not spark.catalog.tableExists("silver_fx_rates"):
    clean_fx.write.format("delta").saveAsTable("silver_fx_rates")
    print("Created initial silver_fx_rates Delta table.")
else:
    DeltaTable.forName(spark, "silver_fx_rates").alias("t").merge(
        clean_fx.alias("s"),
        "t.rate_date = s.rate_date AND t.base_currency = s.base_currency AND t.target_currency = s.target_currency"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("Completed incremental Delta MERGE into silver_fx_rates.")

# 3. Process World Bank Macro Data & Bridge to Currency Dimension
macro_schema = StructType([
    StructField("country_iso3", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("gdp_usd", DoubleType(), True),
    StructField("ingested_at", StringType(), True)
])
raw_macro = spark.read.schema(macro_schema).json("Files/bronze/macro/*.json")

currency_map = spark.createDataFrame([
    ("EMU", "EUR", "Euro"),
    ("GBR", "GBP", "British Pound"),
    ("IND", "INR", "Indian Rupee"),
    ("JPN", "JPY", "Japanese Yen"),
    ("CAN", "CAD", "Canadian Dollar"),
    ("AUS", "AUD", "Australian Dollar"),
    ("CHE", "CHF", "Swiss Franc")
], ["country_iso3", "currency_code", "currency_name"])

latest_yr = raw_macro.agg(F.max("year")).collect()[0][0]

# 4. Gold Star Schema: Dimension Currency + Fact Daily FX
dim_currency = (
    raw_macro.filter(F.col("year") == latest_yr)
    .dropDuplicates(["country_iso3"])
    .join(currency_map, on="country_iso3", how="inner")
    .select(
        F.abs(F.hash("currency_code")).alias("currency_id"),
        "currency_code",
        "currency_name",
        "country_iso3",
        "country_name",
        F.col("year").alias("gdp_reference_year"),
        F.round(F.col("gdp_usd") / 1e12, 3).alias("gdp_trillions_usd")
    )
)
dim_currency.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_dim_currency")

fact_fx = spark.read.table("silver_fx_rates").select(
    "rate_date",
    F.abs(F.hash("target_currency")).alias("currency_id"),
    "base_currency",
    "target_currency",
    "exchange_rate"
)
fact_fx.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_fact_fx_rates")

print(f"SUCCESS: Built Gold Star Schema with {fact_fx.count()} FX records.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
