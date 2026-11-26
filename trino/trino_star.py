"""Lab 10-3: star schema with Trino.  Run: uv run python trino/trino_star.py"""
import os
import re

import pandas as pd
import trino

pd.set_option("display.width", 200)

conn = trino.dbapi.connect(host="trino", port=8080, http_scheme="http", catalog="lakehouse",
                           user=os.environ["KUBERNETES_NAMESPACE"])


def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[c[0] for c in cur.description] if cur.description else [])


def show(title, statement):
    print(f"\n===== {title} =====", flush=True)
    print(sql(statement).to_string(index=False, max_colwidth=70))


one = lambda q: sql(q).iloc[0, 0]
G = "lakehouse.gold"
for t in ("dim_user", "dim_product", "dim_date", "fact_orders", "orders_by_state_month_part", "cohorts"):
    sql(f"DROP TABLE IF EXISTS {G}.{t}")

# ---------------------------------------------------------------- dimensions and fact
sql(f"""CREATE TABLE {G}.dim_user AS
    SELECT row_number() OVER (ORDER BY user_id) AS user_key,
           user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users""")
sql(f"""CREATE TABLE {G}.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)""")
first_day, last_day = sql("""SELECT CAST(min(ordered_at) AS date), CAST(max(ordered_at) AS date)
                             FROM lakehouse.silver.orders""").iloc[0]
sql(f"""CREATE TABLE {G}.dim_date AS
    SELECT CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
           d AS full_date, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
           date_format(CAST(d AS timestamp), '%M') AS month_name,
           day_of_week(d) AS day_of_week, day_of_week(d) IN (6, 7) AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)""")
sql(f"""CREATE TABLE {G}.fact_orders AS
    SELECT o.order_id, u.user_key, p.product_key,
           CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key, o.ordered_at, o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN {G}.dim_user AS u ON u.user_id = o.user_id
    LEFT JOIN {G}.dim_product AS p ON p.product = o.product""")
show("row counts", f"""
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM {G}.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM {G}.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM {G}.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM {G}.fact_orders""")

# ---------------------------------------------------------------- query the star
show("year x state x product", f"""
    SELECT d.year, u.state, p.product, count(*) AS orders, sum(f.quantity) AS units
    FROM {G}.fact_orders f JOIN {G}.dim_date d ON d.date_key = f.date_key
    JOIN {G}.dim_user u ON u.user_key = f.user_key JOIN {G}.dim_product p ON p.product_key = f.product_key
    GROUP BY 1, 2, 3 ORDER BY units DESC LIMIT 10""")
STAR = f"""SELECT u.state, d.year, d.month, count(*) AS orders, sum(f.quantity) AS units,
                  count(DISTINCT f.user_key) AS buyers
           FROM {G}.fact_orders f JOIN {G}.dim_date d ON d.date_key = f.date_key
           JOIN {G}.dim_user u ON u.user_key = f.user_key GROUP BY 1, 2, 3"""
show("orders_by_state_month rebuilt from the star", f"{STAR} ORDER BY year, month, state LIMIT 5")
show("orders_by_state_month (lab 10-2)", f"""SELECT state, year(month) AS year, month(month) AS month,
    orders, units, buyers FROM {G}.orders_by_state_month ORDER BY month, state LIMIT 5""")
diff = one(f"""SELECT count(*) FROM (
    (SELECT state, year, month, orders, units, buyers FROM ({STAR}))
    EXCEPT
    (SELECT state, year(month), month(month), orders, units, buyers FROM {G}.orders_by_state_month))""")
print("rows of the star query absent from orders_by_state_month:", diff)

# ---------------------------------------------------------------- validation
assert one("SELECT count(*) FROM lakehouse.silver.orders") == one(f"SELECT count(*) FROM {G}.fact_orders")
assert one("SELECT sum(quantity) FROM lakehouse.silver.orders") == one(f"SELECT sum(quantity) FROM {G}.fact_orders")
unmatched = one(f"""SELECT count(*) FROM {G}.fact_orders f
    LEFT JOIN {G}.dim_user u ON u.user_key = f.user_key
    LEFT JOIN {G}.dim_product p ON p.product_key = f.product_key
    LEFT JOIN {G}.dim_date d ON d.date_key = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL""")
assert unmatched == 0, unmatched
print("\nVALIDATION OK")

# ---------------------------------------------------------------- exercise 1: partitioning
sql(f"""CREATE TABLE {G}.orders_by_state_month_part WITH (partitioning = ARRAY['month(month)']) AS
        SELECT * FROM {G}.orders_by_state_month""")
for t in ("orders_by_state_month", "orders_by_state_month_part"):
    print(f"{t}: {one(f'SELECT count(*) FROM {G}.\"{t}$files\"')} data files")
show("files of the partitioned table", f"""SELECT partition, record_count, file_size_in_bytes
    FROM {G}."orders_by_state_month_part$files" ORDER BY 1""")
for t in ("orders_by_state_month", "orders_by_state_month_part"):
    plan = one(f"EXPLAIN ANALYZE SELECT sum(units) FROM {G}.{t} WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'")
    scan = [l.strip() for l in plan.splitlines() if "Input:" in l][-1]
    print(f"{t}: {scan}")

# ---------------------------------------------------------------- exercise 2: cohorts
sql(f"""CREATE TABLE {G}.cohorts AS
    SELECT (year(u.birthdate) / 10) * 10 AS birth_decade, count(DISTINCT u.user_key) AS users,
           count(*) AS orders, round(avg(f.quantity), 2) AS avg_units_per_order
    FROM {G}.fact_orders f JOIN {G}.dim_user u ON u.user_key = f.user_key
    GROUP BY 1""")
show("cohorts", f"SELECT * FROM {G}.cohorts ORDER BY birth_decade")

# ---------------------------------------------------------------- exercise 3: MERGE refresh
before = one(f'SELECT snapshot_id FROM {G}."orders_by_state_month$snapshots" ORDER BY committed_at DESC LIMIT 1')
sql("""INSERT INTO lakehouse.silver.orders (order_id, user_id, ordered_at, quantity, product)
       SELECT 'trino-lab-' || CAST(uuid() AS varchar), user_id, TIMESTAMP '2020-02-15 12:00:00 UTC', 4, 'cookie'
       FROM lakehouse.silver.users WHERE state = 'NJ' LIMIT 1""")
sql(f"""MERGE INTO {G}.orders_by_state_month t
    USING (SELECT u.state, date_trunc('month', o.ordered_at) AS month, count(*) AS orders,
                  sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
           FROM lakehouse.silver.orders o JOIN lakehouse.silver.users u ON u.user_id = o.user_id
           GROUP BY 1, 2) s
    ON t.state = s.state AND t.month = s.month
    WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
      THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
    WHEN NOT MATCHED THEN INSERT VALUES (s.state, s.month, s.orders, s.units, s.buyers)""")
q = f"SELECT orders, units FROM {G}.orders_by_state_month{{}} WHERE state = 'NJ' AND month = TIMESTAMP '2020-02-01 00:00:00 UTC'"
show("NJ / 2020-02 now", q.format(""))
show("NJ / 2020-02 before the MERGE", q.format(f" FOR VERSION AS OF {before}"))
show("orders_by_state_month$snapshots", f"""SELECT snapshot_id, committed_at, operation
    FROM {G}."orders_by_state_month$snapshots" ORDER BY committed_at""")

# ---------------------------------------------------------------- exercise 4: optimize
def state(label):
    s = one(f'SELECT count(*) FROM {G}."top_products$snapshots"')
    f = one(f'SELECT count(*) FROM {G}."top_products$files"')
    print(f"{label}: {s} snapshots, {f} data files")

for i in range(3):  # small appends create small files, as a streaming job would
    sql(f"INSERT INTO {G}.top_products SELECT product, 0, 0, 99 FROM {G}.dim_product LIMIT 1")
state("top_products before optimize")
sql(f"ALTER TABLE {G}.top_products EXECUTE optimize")
state("top_products after optimize ")
show("top_products$snapshots", f"""SELECT committed_at, operation, element_at(summary, 'added-data-files') AS added,
    element_at(summary, 'deleted-data-files') AS removed FROM {G}."top_products$snapshots" ORDER BY committed_at""")
sql(f"DELETE FROM {G}.top_products WHERE units_rank = 99")

show("gold tables", f"SHOW TABLES FROM {G}")
conn.close()
print("\nLAB10-3 OK")
