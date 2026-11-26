"""Lab 09-2: govern Iceberg tables with Polaris.

Usage: spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
         --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
         spark/polaris_lab.py <dev|analyst|reader|cleanup>
"""
import os
import sys

from pyspark.sql import SparkSession

MODE = sys.argv[1]
PRINCIPAL = {"dev": "dev", "cleanup": "dev", "analyst": "analyst", "reader": "reader"}[MODE]
CREDENTIAL = os.environ[f"POLARIS_{PRINCIPAL.upper()}_CREDENTIAL"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

spark = (
    SparkSession.builder
    .appName(f"lab-polaris-{MODE}")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{os.environ['POLARIS_URI']}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", os.environ["POLARIS_CATALOG"])
    .config("spark.sql.catalog.polaris.credential", CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def title(text):
    print(f"\n===== [{PRINCIPAL}] {text} =====", flush=True)


def sql(query, n=20):
    spark.sql(query).show(n, truncate=False)


def attempt(label, query):
    try:
        spark.sql(query).collect()
        print(f"{label}: ALLOWED")
    except Exception as error:  # Polaris answers 403 Forbidden
        first = next((l for l in str(error).splitlines() if "Forbidden" in l), str(error).splitlines()[0])
        print(f"{label}: DENIED -> {first[:220]}")


print("Polaris principal:", PRINCIPAL)

if MODE == "dev":
    title("Catalogs")
    sql("SHOW NAMESPACES IN lab")
    sql("SHOW NAMESPACES IN polaris")

    title("Register lab.silver.orders in Polaris")
    version = os.environ["ORDERS_VERSION"]
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    if "orders" not in [r.tableName for r in spark.sql("SHOW TABLES IN polaris.silver").collect()]:
        sql(f"""CALL polaris.system.register_table(
                  table => 'silver.orders',
                  metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{version}.metadata.json')""")
    sql("SELECT count(*) AS orders FROM polaris.silver.orders")
    sql("SELECT count(*) AS orders FROM lab.silver.orders")

    title("Create silver.users through Polaris")
    sql("SHOW TABLES IN polaris.silver")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")
    users = spark.table("lab.silver.users")
    users = users.drop("country") if "country" in users.columns else users
    users.writeTo("polaris.silver.users").createOrReplace()
    sql("SELECT count(*) AS users FROM polaris.silver.users")

    title("Schema change: ADD COLUMN country")
    spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    sql("DESCRIBE polaris.silver.users")

    title("Exercise 1: DROP COLUMN country")
    spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
    sql("DESCRIBE polaris.silver.users")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.users.snapshots")
    sql("SELECT file, latest_schema_id FROM polaris.silver.users.metadata_log_entries")

    title("Exercise 2: gold namespace and table for the access test")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.sql("""
        CREATE OR REPLACE TABLE polaris.gold.sales_by_product AS
        SELECT product, sum(quantity) AS quantity FROM polaris.silver.orders GROUP BY product
    """)
    sql("SELECT * FROM polaris.gold.sales_by_product ORDER BY quantity DESC")

elif MODE == "analyst":
    title("Read")
    sql("SHOW TABLES IN polaris.silver")
    sql("SELECT product, count(*) AS orders FROM polaris.silver.orders GROUP BY product ORDER BY product")
    title("Write attempts")
    attempt("INSERT INTO silver.orders",
            "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    attempt("ALTER TABLE silver.users ADD COLUMN phone",
            "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

elif MODE == "reader":
    title("Exercise 2: silver_reader on silver vs gold")
    attempt("SELECT silver.orders", "SELECT count(*) FROM polaris.silver.orders")
    print("silver.orders rows:", spark.sql("SELECT count(*) FROM polaris.silver.orders").first()[0])
    attempt("SELECT gold.sales_by_product", "SELECT * FROM polaris.gold.sales_by_product")
    attempt("INSERT INTO silver.orders",
            "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")

elif MODE == "cleanup":
    title("Snapshots after the analyst attempts")
    sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")
    title("Cleanup: drop gold (silver.users and silver.orders are kept for the Trino lab)")
    spark.sql("DROP TABLE IF EXISTS polaris.gold.sales_by_product")
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
    sql("SHOW NAMESPACES IN polaris")
    sql("SHOW TABLES IN polaris.silver")

spark.stop()
print(f"\nLAB09 {MODE} OK")
