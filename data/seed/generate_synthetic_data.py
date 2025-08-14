# Databricks notebook or Python file
# Writes Parquet into UC Volume: /Volumes/genie_poc/raw/seed/
# Scale via SCALE_FACT (approx row millions for fact tables)
import numpy as np, pandas as pd, datetime as dt, os, json
from pyspark.sql import functions as F

CATALOG = "genie_poc"
SEED_DIR = f"/Volumes/{CATALOG}/raw/seed"
SCALE_FACT = 20  # ~20M order_line rows; adjust 10–50 for demo size

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("USE SCHEMA raw")

# --- Dimension generators ---
np.random.seed(42)

product_families = ["OSB", "Plywood", "Timber"]
grades = ["A", "B", "C"]
dim_product = pd.DataFrame({
    "product_id": range(1, 2001),
    "sku": [f"{pf}-{i:04d}" for i, pf in zip(range(1,2001), np.random.choice(product_families, 2000))],
    "family": np.random.choice(product_families, 2000),
    "grade": np.random.choice(grades, 2000),
    "base_cost": np.round(np.random.uniform(8, 35, 2000), 2)
})

regions = ["Southeast","Gulf","Midwest","Northeast","West"]
dim_region = pd.DataFrame({
    "region_id": range(1, 6),
    "region_name": regions,
    "price_multiplier": [1.00,1.03,1.02,1.05,1.07]
})

channels = ["Direct","Distributor","Retail"]
dim_channel = pd.DataFrame({
    "channel_id": range(1,4),
    "channel_name": channels
})

mills = ["Oakdale","Corrigan","Lena","Mill4","Mill5"]
dim_mill = pd.DataFrame({
    "mill_id": range(1,6),
    "mill_name": mills,
    "state": ["LA","TX","LA","AR","MS"],
    "yield_baseline_pct": [0.89,0.91,0.88,0.90,0.87]  # baseline raw->finished yield
})

dim_customer = pd.DataFrame({
    "customer_id": range(1, 50001),
    "customer_name": [f"Customer-{i:05d}" for i in range(1,50001)],
    "segment": np.random.choice(["BigBox","Regional","Contractor","OEM"], 50000)
})

# calendar (3 years +)
start = dt.date.today().replace(day=1) - dt.timedelta(days=365*2)
dates = pd.date_range(start, periods=1100, freq="D").date
dim_calendar = pd.DataFrame({
    "date_key": [int(d.strftime("%Y%m%d")) for d in dates],
    "date": dates,
    "year": [d.year for d in dates],
    "quarter": [(d.month-1)//3 + 1 for d in dates],
    "month": [d.month for d in dates],
    "day": [d.day for d in dates],
    "is_month_end": [d == (dt.date(d.year, d.month, 1) + dt.timedelta(days=32)).replace(day=1) - dt.timedelta(days=1) for d in dates]
})

# --- Write dims ---
spark.createDataFrame(dim_product).write.mode("overwrite").parquet(f"{SEED_DIR}/dim_product")
spark.createDataFrame(dim_region).write.mode("overwrite").parquet(f"{SEED_DIR}/dim_region")
spark.createDataFrame(dim_channel).write.mode("overwrite").parquet(f"{SEED_DIR}/dim_channel")
spark.createDataFrame(dim_mill).write.mode("overwrite").parquet(f"{SEED_DIR}/dim_mill")
spark.createDataFrame(dim_customer).write.mode("overwrite").parquet(f"{SEED_DIR}/dim_customer")
spark.createDataFrame(dim_calendar).write.mode("overwrite").parquet(f"{SEED_DIR}/dim_calendar")

# --- Fact generators ---
def gen_fact_sales(n_millions):
    # simulate seasonality + regional pricing + yield impact on COGS
    N = n_millions * 1_000_000
    prod = spark.read.parquet(f"{SEED_DIR}/dim_product").toPandas()
    reg = spark.read.parquet(f"{SEED_DIR}/dim_region").toPandas()
    chan = spark.read.parquet(f"{SEED_DIR}/dim_channel").toPandas()
    mill = spark.read.parquet(f"{SEED_DIR}/dim_mill").toPandas()
    cust = spark.read.parquet(f"{SEED_DIR}/dim_customer").toPandas()
    cal = spark.read.parquet(f"{SEED_DIR}/dim_calendar").toPandas()

    # vectorized sampling in batches to avoid driver OOM
    batch = 2_000_000
    files = []
    for start in range(0, N, batch):
        size = min(batch, N-start)
        prod_idx = np.random.randint(0, len(prod), size)
        reg_idx = np.random.randint(0, len(reg), size)
        chan_idx = np.random.randint(0, len(chan), size)
        mill_idx = np.random.randint(0, len(mill), size)
        cust_idx = np.random.randint(0, len(cust), size)
        cal_idx = np.random.randint(0, len(cal), size)

        base_cost = prod["base_cost"].values[prod_idx]
        price_mult = reg["price_multiplier"].values[reg_idx]
        qty = np.random.poisson(lam=50, size=size).clip(min=1)  # order line qty
        discount = np.round(np.random.beta(2, 20, size)*0.15, 4)  # up to ~15%
        asp = np.round((base_cost * (1.4 + np.random.normal(0, 0.05, size))) * price_mult, 2)
        net_price = np.round(asp * (1 - discount), 2)
        # COGS impacted by mill yield variance per shift
        yield_noise = np.random.normal(0, 0.01, size)
        mill_yield = np.clip(mill["yield_baseline_pct"].values[mill_idx] + yield_noise, 0.80, 0.94)
        cogs = np.round(base_cost / mill_yield * (1 + np.random.normal(0.02, 0.015, size)), 2)

        ship_delay = np.random.normal(0, 3, size).astype(int)  # some negative/early, some late
        ship_idx = np.clip(cal_idx + ship_delay, 0, len(cal)-1)
        on_time = (ship_delay <= 0).astype(int)

        pdf = pd.DataFrame({
            "order_line_id": np.arange(start+1, start+1+size),
            "order_date_key": cal["date_key"].values[cal_idx],
            "ship_date_key": cal["date_key"].values[ship_idx],
            "product_id": prod["product_id"].values[prod_idx],
            "region_id": reg["region_id"].values[reg_idx],
            "channel_id": chan["channel_id"].values[chan_idx],
            "mill_id": mill["mill_id"].values[mill_idx],
            "customer_id": cust["customer_id"].values[cust_idx],
            "qty": qty,
            "list_price": asp,
            "discount_pct": discount,
            "net_price": net_price,
            "cogs": cogs,
            "on_time_flag": on_time
        })
        fpath = f"{SEED_DIR}/fact_sales/part_{start:09d}.parquet"
        spark.createDataFrame(pdf).write.mode("overwrite").parquet(fpath)
        files.append(fpath)
    return files

def gen_daily_by_mill(table, size_days=900):
    prod = spark.read.parquet(f"{SEED_DIR}/dim_product").select("product_id").limit(300).toPandas()
    mills = spark.read.parquet(f"{SEED_DIR}/dim_mill").toPandas()
    cal = spark.read.parquet(f"{SEED_DIR}/dim_calendar").toPandas()[:size_days]
    rows = []
    for d in cal["date_key"]:
        for m in mills["mill_id"]:
            # sample 50 SKUs per day per mill
            sku_ids = np.random.choice(prod["product_id"].values, 50, replace=False)
            if table == "inventory":
                on_hand = np.random.poisson(lam=500, size=50)
                rows += [(d, m, int(s), int(q)) for s,q in zip(sku_ids, on_hand)]
            elif table == "production":
                shifts = np.random.choice(["A","B","C"], 50)
                made = np.random.poisson(lam=60, size=50)
                rows += [(d, m, int(s), str(sh), int(q)) for s,sh,q in zip(sku_ids, shifts, made)]
            elif table == "logistics":
                carriers = np.random.choice(["DHL","XPO","ODFL","JBHunt"], 50)
                lead = np.clip(np.random.normal(5, 2, 50), 1, 20)
                cost = np.round(np.random.gamma(2, 15, 50), 2)
                rows += [(d, m, int(s), str(ca), float(le), float(co)) for s,ca,le,co in zip(sku_ids, carriers, lead, cost)]
    if table == "inventory":
        cols = ["date_key","mill_id","product_id","qty_on_hand"]
    elif table == "production":
        cols = ["date_key","mill_id","product_id","shift","qty_produced"]
    else:
        cols = ["date_key","mill_id","product_id","carrier","lead_time_days","ship_cost_usd"]
    pdf = pd.DataFrame(rows, columns=cols)
    spark.createDataFrame(pdf).write.mode("overwrite").parquet(f"{SEED_DIR}/fact_{table}")

# Generate
gen_fact_sales(SCALE_FACT)
gen_daily_by_mill("inventory")
gen_daily_by_mill("production")
gen_daily_by_mill("logistics")

print("Synthetic data files written to", SEED_DIR)
