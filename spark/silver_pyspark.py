"""Lab 07: bronze to silver with Spark DataFrames, Spark SQL and Iceberg.

Run from the project root, after loading fresh credentials from the AWS profile:
  eval "$(aws configure export-credentials --profile default --format env)"
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 spark/silver_pyspark.py
"""
import os
import time
import timeit

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUCKET = os.environ.get("LAB_BUCKET_NAME") or os.environ["KUBERNETES_NAMESPACE"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def title(text):
    print(f"\n===== {text} =====", flush=True)


spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------- bronze
title("Bronze")
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv",
                              header=True, inferSchema=True, samplingRatio=0.01, multiLine=True)
orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv",
                               header=True, inferSchema=True, samplingRatio=0.01, multiLine=True)
users_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)

# ---------------------------------------------------------------- silver users
title("Silver users, DataFrame API")
users_typed = (
    users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
# regexp_extract returns "" on no match: turn it into NULL like nullif() in dbt
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)

# ---------------------------------------------------------------- silver orders
title("Silver orders, window deduplication")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
orders_with_duplicates = orders_typed.unionByName(orders_typed.limit(3))
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter("count > 1").show()

dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter("row_number = 1")
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter("count > 1").show()

title("Same deduplication with Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
          FROM orders_typed)
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("DataFrame and SQL versions agree.")

stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state, count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users GROUP BY state ORDER BY users DESC
""").show(10)

# ---------------------------------------------------------------- parquet
title("Write silver as Parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("Parquet round trip OK:", users_silver.count(), "users,", orders_silver.count(), "orders")

# ---------------------------------------------------------------- iceberg
title("Write silver as Iceberg tables")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes "
          "FROM lab.silver.orders.files").show(truncate=False)
assert spark.table("lab.silver.users").count() == users_bronze.count()
assert spark.table("lab.silver.orders").count() == stg_orders.count()
print("Iceberg tables OK.")

# ---------------------------------------------------------------- exercise 1
title("Exercise 1: birthdate_is_valid")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").cast("date").alias("first_order_date"))
users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn("birthdate_is_valid",
                F.coalesce(F.col("birthdate") <= F.col("first_order_date"), F.lit(True)))
    .drop("first_order_date")
)
stg_orders.createOrReplaceTempView("stg_orders")
users_valid_sql = spark.sql("""
    SELECT u.*, coalesce(u.birthdate <= f.first_order_date, true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (SELECT user_id, cast(min(ordered_at) AS date) AS first_order_date
               FROM stg_orders GROUP BY user_id) f
      ON u.user_id = f.user_id
""")
diff = users_valid_df.exceptAll(users_valid_sql.select(*users_valid_df.columns)).count()
print("Invalid birthdates:", users_valid_df.filter("NOT birthdate_is_valid").count(),
      "| rows differing between DataFrame and SQL:", diff)

# ---------------------------------------------------------------- exercise 2
title("Exercise 2: regexp_extract vs Python UDF vs pandas_udf")
ZIP = r"([A-Z]{2}) ([0-9]{5})$"
addr = users_bronze.select(F.element_at(F.split("address", "\n"), 2).alias("line"))


@F.udf("string")
def zip_udf(line):
    import re
    match = re.search(ZIP, line or "")
    return match.group(2) if match else ""


variants = {
    "regexp_extract": lambda: addr.select(F.regexp_extract("line", ZIP, 2).alias("z")).agg(F.count("z")).collect(),
    "python udf": lambda: addr.select(zip_udf("line").alias("z")).agg(F.count("z")).collect(),
}
try:
    import pandas as pd
    from pyspark.sql.functions import pandas_udf

    @pandas_udf("string")
    def zip_pandas(lines: pd.Series) -> pd.Series:
        return lines.fillna("").str.extract(ZIP)[1].fillna("")

    variants["pandas_udf"] = lambda: addr.select(zip_pandas("line").alias("z")).agg(F.count("z")).collect()
except Exception as error:  # pandas or pyarrow missing
    print("pandas_udf skipped:", error)
for name, run in variants.items():
    run()  # warm-up
    print(f"{name:<15} {min(timeit.repeat(run, number=3, repeat=3)) / 3 * 1000:8.1f} ms per run")

# ---------------------------------------------------------------- exercise 3
title("Exercise 3: cache the bronze DataFrames")
def timed(label, action):
    start = time.perf_counter()
    action()
    print(f"{label:<28} {time.perf_counter() - start:.3f} s")

timed("orders count, no cache", orders_bronze.count)
timed("orders count, no cache (2)", orders_bronze.count)
users_bronze.cache()
orders_bronze.cache()
timed("orders count, fills cache", orders_bronze.count)
timed("orders count, from cache", orders_bronze.count)
users_bronze.count()
import json, urllib.request
sc = spark.sparkContext
storage = json.load(urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/storage/rdd"))
print("Storage tab:", [(r["name"][:40], r["numCachedPartitions"], r["memoryUsed"]) for r in storage])

# ---------------------------------------------------------------- exercise 4
title("Exercise 4: Iceberg time travel")
before = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
new_order = spark.createDataFrame(
    [("00000000-0000-0000-0000-000000000001", stg_users.first()["user_id"], None, 1, "bread")],
    stg_orders.schema,
).withColumn("ordered_at", F.current_timestamp())
new_order.writeTo("lab.silver.orders").append()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
print("Rows now:", spark.table("lab.silver.orders").count())
print("Rows at snapshot", before, ":",
      spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {before}").first()[0])
new_order.write.mode("append").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("Parquet directory rows after append:",
      spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet").count(),
      "(no snapshot to go back to)")
# Restore the Parquet silver layer for the dbt comparison and the next modules
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")

spark.stop()
print("\nLAB07 OK")
