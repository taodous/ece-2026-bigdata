"""Lab 08: explore Iceberg tables with Spark (snapshots, time travel, schema evolution).

  eval "$(aws configure export-credentials --profile default --format env)"
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 spark/iceberg_explore.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ.get("LAB_BUCKET_NAME") or os.environ["KUBERNETES_NAMESPACE"]


def title(text):
    print(f"\n===== {text} =====", flush=True)


def sql(query, n=20):
    spark.sql(query).show(n, truncate=False)


spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

title("Catalog")
sql("SHOW NAMESPACES IN lab")
sql("SHOW TABLES IN lab.silver")

title("Table and metadata")
sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders")
sql("SELECT snapshot_id, parent_id, operation, committed_at, summary['added-records'] AS added "
    "FROM lab.silver.orders.snapshots ORDER BY committed_at")
sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files")

title("Second snapshot: append 3 orders")
stg_orders = spark.table("lab.silver.orders")
stg_orders.limit(3).withColumn("order_id", F.expr("uuid()")).writeTo("lab.silver.orders").append()
sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at")
sql("SELECT count(*) AS orders FROM lab.silver.orders")
sql("SELECT count(*) AS data_files FROM lab.silver.orders.files")

title("Time travel")
snapshots = [r["snapshot_id"] for r in spark.sql(
    "SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()]
for sid in snapshots:
    count = spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {sid}").first()[0]
    print(f"snapshot {sid}: {count} rows")
print("current:", spark.table("lab.silver.orders").count(), "rows")

title("Schema evolution: ADD COLUMN source")
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
sql("DESCRIBE lab.silver.orders")
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders LIMIT 1
""")
sql("SELECT source, count(*) AS rows FROM lab.silver.orders GROUP BY source")
sql("SELECT count(*) AS data_files FROM lab.silver.orders.files")
sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at")

title("Iceberg vs Parquet")
parquet_files = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
    f"s3a://{BUCKET}/silver/spark/orders.parquet")
fs = parquet_files.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
print("silver/spark/orders.parquet:", [s.getPath().getName() for s in fs.listStatus(parquet_files)])

# ---------------------------------------------------------------- exercises
title("Exercise 1: lab.silver.users as an Iceberg table")
spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet").writeTo("lab.silver.users").createOrReplace()
sql("SELECT count(*) AS users FROM lab.silver.users")

title("Exercise 2: append a batch of users")
users = spark.table("lab.silver.users")
(users.limit(5)
 .withColumn("user_id", F.expr("uuid()"))
 .withColumn("username", F.concat(F.col("username"), F.lit("_new")))
 .withColumn("email", F.concat(F.lit("new_"), F.col("email")))
 .writeTo("lab.silver.users").append())
sql("SELECT snapshot_id, operation, summary['added-records'] AS added, committed_at "
    "FROM lab.silver.users.snapshots ORDER BY committed_at")

title("Exercise 3: time travel on users")
first, last = [r[0] for r in spark.sql(
    "SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at").collect()][-2:]
print("before append:", spark.sql(f"SELECT count(*) FROM lab.silver.users VERSION AS OF {first}").first()[0])
print("after append: ", spark.sql(f"SELECT count(*) FROM lab.silver.users VERSION AS OF {last}").first()[0])

title("Exercise 4: ADD COLUMN country")
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
(spark.table("lab.silver.users").limit(2)
 .withColumn("user_id", F.expr("uuid()"))
 .withColumn("email", F.concat(F.lit("country_"), F.col("email")))
 .withColumn("country", F.lit("US"))
 .writeTo("lab.silver.users").append())
sql("SELECT country, count(*) AS users FROM lab.silver.users GROUP BY country")

title("Exercise 5: S3 layout of the two tables")
for table in ("users", "orders"):
    root = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/warehouse/silver/{table}")
    for sub in ("data", "metadata"):
        files = [s.getPath().getName() for s in fs.listStatus(root.suffix("/" + sub))]
        print(f"{table}/{sub}: {len(files)} files", sorted(files) if sub == "metadata" else "")

spark.stop()
print("\nLAB08 OK")
