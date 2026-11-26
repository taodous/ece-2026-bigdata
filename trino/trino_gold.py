"""Lab 10-2: silver to gold with Trino.  Run: uv run python trino/trino_gold.py"""
import os

import pandas as pd
import trino

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

conn = trino.dbapi.connect(
    host=os.environ.get("TRINO_HOST", "trino"),
    port=int(os.environ.get("TRINO_PORT", "8080")),
    user=os.environ.get("TRINO_USER", os.environ["KUBERNETES_NAMESPACE"]),
    http_scheme="http",
    catalog="lakehouse",
)


def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(title, statement):
    print(f"\n===== {title} =====", flush=True)
    print(sql(statement).to_string(index=False, max_colwidth=70))


show("version", "SELECT version()")
show("catalogs", "SHOW CATALOGS")
show("schemas", "SHOW SCHEMAS FROM lakehouse")
show("silver tables", "SHOW TABLES FROM lakehouse.silver")
show("describe orders", "DESCRIBE lakehouse.silver.orders")
show("describe users", "DESCRIBE lakehouse.silver.users")
show("counts", """SELECT (SELECT count(*) FROM lakehouse.silver.orders) AS orders,
                         (SELECT count(*) FROM lakehouse.silver.users) AS users""")
show("users by state", """
    SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users GROUP BY state ORDER BY users DESC, state LIMIT 10""")
show("orphan orders", """
    SELECT count(*) AS orphan_orders FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id WHERE u.user_id IS NULL""")
show("orders$snapshots", """SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots" ORDER BY committed_at""")
show("orders$files", """SELECT regexp_extract(file_path, '[^/]+$') AS file, file_format, record_count,
    file_size_in_bytes FROM lakehouse.silver."orders$files" """)
show("orders$history", 'SELECT * FROM lakehouse.silver."orders$history"')

JOIN = """SELECT u.state, count(*) AS orders FROM lakehouse.silver.orders AS o
          JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id GROUP BY u.state"""
print("\n===== EXPLAIN (TYPE DISTRIBUTED) =====")
print(sql(f"EXPLAIN (TYPE DISTRIBUTED) {JOIN}").iloc[0, 0])
print("\n===== EXPLAIN ANALYZE =====")
print(sql(f"EXPLAIN ANALYZE {JOIN}").iloc[0, 0])
try:
    show("recent queries", """
        SELECT query_id, state, date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms,
               substr(query, 1, 50) AS query
        FROM system.runtime.queries ORDER BY created DESC LIMIT 5""")
except Exception as error:
    print("system.runtime.queries denied:", str(error)[:150])

# ---------------------------------------------------------------- gold
sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
for table in ("orders_by_state_month", "top_products", "user_activity"):
    sql(f"DROP TABLE IF EXISTS lakehouse.gold.{table}")
print("\nrows written:", sql("""
    CREATE TABLE lakehouse.gold.orders_by_state_month AS
    SELECT u.state, date_trunc('month', o.ordered_at) AS month, count(*) AS orders,
           sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
    FROM lakehouse.silver.orders AS o JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    GROUP BY 1, 2""").iloc[0, 0])
show("orders_by_state_month", "SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")
sql("""
    CREATE TABLE lakehouse.gold.top_products AS
    SELECT product, count(*) AS orders, sum(quantity) AS units,
           rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders GROUP BY product""")
show("top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank")
sql("""
    CREATE TABLE lakehouse.gold.user_activity AS
    SELECT u.user_id, u.state, date_diff('year', u.birthdate, current_date) AS age,
           count(o.order_id) AS orders, coalesce(sum(o.quantity), 0) AS units,
           min(o.ordered_at) AS first_order_at, max(o.ordered_at) AS last_order_at
    FROM lakehouse.silver.users AS u LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate""")
show("user_activity", "SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 5")
show("gold tables", "SHOW TABLES FROM lakehouse.gold")
show("top_products$files", """SELECT file_path, record_count, file_size_in_bytes
    FROM lakehouse.gold."top_products$files" """)

# ---------------------------------------------------------------- validation
one = lambda q: sql(q).iloc[0, 0]
silver_units = one("SELECT sum(quantity) FROM lakehouse.silver.orders")
assert silver_units == one("SELECT sum(units) FROM lakehouse.gold.orders_by_state_month")
assert silver_units == one("SELECT sum(units) FROM lakehouse.gold.top_products")
assert one("SELECT count(*) FROM lakehouse.silver.users") == one("SELECT count(*) FROM lakehouse.gold.user_activity")
assert one("SELECT count(*) FROM lakehouse.silver.orders") == one("SELECT sum(orders) FROM lakehouse.gold.user_activity")
print("\nVALIDATION OK: silver units =", silver_units)

# ---------------------------------------------------------------- refresh and time travel
sql("DELETE FROM lakehouse.gold.top_products")
sql("""INSERT INTO lakehouse.gold.top_products
       SELECT product, count(*), sum(quantity), rank() OVER (ORDER BY sum(quantity) DESC)
       FROM lakehouse.silver.orders GROUP BY product""")
show("top_products$snapshots", """SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")
snaps = sql('SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')["snapshot_id"]
for sid in snaps:
    print(f"snapshot {sid}: {one(f'SELECT count(*) FROM lakehouse.gold.top_products FOR VERSION AS OF {sid}')} products")
print("current:", one("SELECT count(*) FROM lakehouse.gold.top_products"), "products")
print("\nLAB10-2 OK")
