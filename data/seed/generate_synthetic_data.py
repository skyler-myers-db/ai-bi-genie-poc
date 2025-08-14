#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Synthetic wood-products manufacturing dataset generator.
Writes partitioned Parquet to a Unity Catalog Volume path.

Tables:
  - dim_product, dim_customer, dim_region, dim_channel, dim_mill, dim_calendar
  - fact_sales (order_line)
  - fact_inventory (by mill/SKU/day)
  - fact_production (by mill/SKU/shift)
  - fact_logistics (shipment-level)

Usage:
  python generate_synthetic_data.py --catalog royomartin --seed_volume raw.seed_vol --scale 12
"""

import argparse, os, math, random, uuid
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

np.random.seed(42)
random.seed(42)

PRODUCT_FAMILIES = ["OSB", "Plywood", "Timber"]
GRADES = ["A", "B", "C"]
CHANNELS = ["Direct", "Distributor", "Retail"]
REGIONS = [
    ("Southeast","SE"), ("Gulf","GL"), ("Midwest","MW"),
    ("Northeast","NE"), ("Southwest","SW"), ("Northwest","NW")
]
MILLS = [
    ("Alexandria","LA-AX"), ("Oakdale","LA-OK"), ("Corrigan","TX-CR"),
    ("Nacogdoches","TX-NC"), ("Meridian","MS-ME")
]

def mk_dates(start="2023-01-01", end="2025-08-01"):
    d0 = pd.Timestamp(start)
    d1 = pd.Timestamp(end)
    days = (d1 - d0).days
    return pd.date_range(d0, periods=days, freq="D")

def seasonality(day_idx):
    # Annual sinusoid: demand higher in spring/summer for construction
    return 1.0 + 0.25 * math.sin(2*math.pi*day_idx/365.0)

def region_price_factor(region_code):
    # Regional price deltas (Gulf & Northeast slightly higher)
    mapping = {"GL":1.05, "NE":1.04, "SE":0.98, "MW":1.00, "SW":0.99, "NW":1.01}
    return mapping.get(region_code, 1.0)

def product_base_price(family, grade):
    base = {"OSB": 350, "Plywood": 600, "Timber": 450}[family]
    grade_delta = {"A":1.15, "B":1.05, "C":0.95}[grade]
    return base * grade_delta

def mill_yield_mean(mill_code, family):
    # Simulate slight yield improvements over time by mill & product
    base = {"OSB":0.88, "Plywood":0.85, "Timber":0.92}[family]
    mill_adj = {
        "LA-AX": +0.01, "LA-OK": 0.0, "TX-CR": +0.005, "TX-NC": -0.005, "MS-ME": +0.007
    }[mill_code]
    return max(min(base + mill_adj, 0.97), 0.80)

def gen_dims(catalog, vol_path):
    # Product
    products = []
    skuid = 1000
    for fam in PRODUCT_FAMILIES:
        for grade in GRADES:
            for thickness in [7, 11, 15, 19]:  # mm
                for width in [48, 60]:         # inches
                    products.append((skuid, fam, grade, thickness, width))
                    skuid += 1
    dim_product = pd.DataFrame(products, columns=["product_id","family","grade","thickness_mm","width_in"])

    # Customers
    custs = []
    for cid in range(1, 4001):
        seg = np.random.choice(["Homebuilder","Wholesaler","Retailer"], p=[0.35,0.45,0.20])
        custs.append((cid, f"Customer {cid:05d}", seg))
    dim_customer = pd.DataFrame(custs, columns=["customer_id","customer_name","segment"])

    # Region
    dim_region = pd.DataFrame([(i+1, n, c) for i,(n,c) in enumerate(REGIONS)], columns=["region_id","region_name","region_code"])

    # Channel
    dim_channel = pd.DataFrame([(i+1, ch) for i,ch in enumerate(CHANNELS)], columns=["channel_id","channel_name"])

    # Mill
    dim_mill = pd.DataFrame([(i+1, n, c) for i,(n,c) in enumerate(MILLS)], columns=["mill_id","mill_name","mill_code"])

    # Calendar (through current month - 1)
    dates = mk_dates()
    dim_calendar = pd.DataFrame({
        "date": dates,
        "day": dates.day,
        "week": dates.isocalendar().week.values,
        "month": dates.month,
        "quarter": dates.quarter,
        "year": dates.year,
        "is_weekend": dates.weekday>=5
    })

    # Write dims
    out = os.path.join(vol_path)
    os.makedirs(out, exist_ok=True)
    dim_product.to_parquet(os.path.join(out, "dim_product"), index=False)
    dim_customer.to_parquet(os.path.join(out, "dim_customer"), index=False)
    dim_region.to_parquet(os.path.join(out, "dim_region"), index=False)
    dim_channel.to_parquet(os.path.join(out, "dim_channel"), index=False)
    dim_mill.to_parquet(os.path.join(out, "dim_mill"), index=False)
    dim_calendar.to_parquet(os.path.join(out, "dim_calendar"), index=False)

    return dim_product, dim_customer, dim_region, dim_channel, dim_mill, dim_calendar

def gen_facts(scale, dim_product, dim_customer, dim_region, dim_channel, dim_mill, dim_calendar, vol_path):
    rng = np.random.default_rng(7)
    dates = dim_calendar["date"].values

    # Scale factor ≈ M rows = scale * 1,000,000
    target_rows = int(scale * 1_000_000)

    # Prepare probability spaces
    product_ids = dim_product["product_id"].values
    customer_ids = dim_customer["customer_id"].values
    region_ids = dim_region["region_id"].values
    region_codes = dict(zip(dim_region["region_id"], dim_region["region_code"]))
    channel_ids = dim_channel["channel_id"].values
    mill_ids = dim_mill["mill_id"].values
    mill_codes = dict(zip(dim_mill["mill_id"], dim_mill["mill_code"]))
    family_lut = dict(zip(dim_product["product_id"], dim_product["family"]))
    grade_lut = dict(zip(dim_product["product_id"], dim_product["grade"]))

    # Distributions
    prod_probs = np.array([0.45 if family_lut[p]=="OSB" else (0.35 if family_lut[p]=="Plywood" else 0.20) for p in product_ids])
    prod_probs = prod_probs / prod_probs.sum()

    chan_probs = np.array([0.4, 0.45, 0.15])  # Direct, Distributor, Retail
    region_probs = np.array([0.24,0.18,0.18,0.16,0.12,0.12])  # SE,GL,MW,NE,SW,NW
    mill_probs = np.array([0.22,0.22,0.20,0.18,0.18])

    # Generate sales rows in daily chunks for seasonality
    sales_rows = []
    order_id = 1
    for di, day in enumerate(dates):
        if len(sales_rows) >= target_rows:
            break
        daily_base = int(3500 * seasonality(di))  # ~3.5K lines/day baseline
        # Random fluctuation
        daily_rows = int(daily_base * rng.uniform(0.85, 1.15))
        for _ in range(daily_rows):
            if len(sales_rows) >= target_rows:
                break
            product_id = rng.choice(product_ids, p=prod_probs)
            customer_id = rng.integers(1, len(customer_ids)+1)
            region_id = rng.choice(region_ids, p=region_probs)
            channel_id = rng.choice(channel_ids, p=chan_probs)
            mill_id = rng.choice(mill_ids, p=mill_probs)

            family = family_lut[product_id]
            grade = grade_lut[product_id]
            qty = int(max(1, rng.poisson(8 if family=="OSB" else (6 if family=="Plywood" else 5))))
            base = product_base_price(family, grade)
            price = base * rng.uniform(0.9, 1.1) * region_price_factor(region_codes[region_id])
            discount = rng.choice([0, 0.02, 0.05, 0.1], p=[0.55,0.25,0.15,0.05])
            net_price = price * (1 - discount)
            cogs = price * rng.uniform(0.6, 0.8) * (1.0 if family!="Plywood" else 1.03)

            # logistics lead time spikes (occasional)
            base_lt = rng.integers(2, 8)
            spike = rng.choice([0, 5, 10], p=[0.94, 0.05, 0.01])
            lead_time = base_lt + spike
            on_time = 1 if lead_time <= 7 else 0
            ship_date = pd.to_datetime(day) + timedelta(days=lead_time)

            sales_rows.append((
                order_id, str(uuid.uuid4()), pd.to_datetime(day),
                product_id, customer_id, region_id, channel_id, mill_id,
                qty, round(float(net_price),2), round(float(discount),2),
                round(float(cogs),2), ship_date, on_time
            ))
            order_id += 1

    fact_sales = pd.DataFrame(sales_rows, columns=[
        "order_id","order_line_id","order_date","product_id","customer_id","region_id","channel_id","mill_id",
        "qty","net_price","discount_pct","cogs","ship_date","on_time_flag"
    ])

    # Inventory by mill/SKU/day
    inv_rows = []
    for day in dates:
        for mill_id in mill_ids:
            # sample 3K SKUs inventory/day scaled down
            sku_subset = np.random.choice(product_ids, size=min(1500, len(product_ids)), replace=False)
            for pid in sku_subset:
                inv = max(0, int(rng.normal(1200, 300)))
                inv_rows.append((pd.to_datetime(day), mill_id, pid, inv))
    fact_inventory = pd.DataFrame(inv_rows, columns=["date","mill_id","product_id","on_hand_units"])

    # Production by mill/SKU/shift
    prod_rows = []
    for day in dates:
        for mill_id in mill_ids:
            for shift in ["A","B","C"]:
                for pid in np.random.choice(product_ids, size=400, replace=False):
                    yield_mean = mill_yield_mean(mill_codes[mill_id], family_lut[pid])
                    produced = max(0, int(rng.normal(1000, 200)))
                    good = int(produced * rng.normal(yield_mean, 0.01))
                    prod_rows.append((pd.to_datetime(day), shift, mill_id, pid, produced, good, round(yield_mean,3)))
    fact_production = pd.DataFrame(prod_rows, columns=[
        "date","shift","mill_id","product_id","units_produced","good_units","yield_target"
    ])

    # Logistics (carrier, lead_time, cost)
    carriers = ["Acme Logistics","PineTrans","Southern Freight","DeltaHaul"]
    logi_rows = []
    for i in range(len(fact_sales)):
        lt = (fact_sales.iloc[i]["ship_date"] - fact_sales.iloc[i]["order_date"]).days
        dist_miles = int(rng.normal(450, 120))
        cost = max(200.0, float(1.2*dist_miles + rng.normal(50,20)))
        logi_rows.append((
            fact_sales.iloc[i]["order_line_id"], rng.choice(carriers), lt, dist_miles, round(cost,2)
        ))
    fact_logistics = pd.DataFrame(logi_rows, columns=["order_line_id","carrier","lead_time_days","distance_miles","freight_cost"])

    # Write partitioned Parquet to the UC volume
    base = vol_path
    fact_sales.to_parquet(os.path.join(base, "fact_sales"), index=False)
    fact_inventory.to_parquet(os.path.join(base, "fact_inventory"), index=False)
    fact_production.to_parquet(os.path.join(base, "fact_production"), index=False)
    fact_logistics.to_parquet(os.path.join(base, "fact_logistics"), index=False)

    return {
        "fact_sales_rows": len(fact_sales),
        "fact_inventory_rows": len(fact_inventory),
        "fact_production_rows": len(fact_production),
        "fact_logistics_rows": len(fact_logistics)
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--seed_volume", required=True, help="e.g. raw.seed_vol")
    ap.add_argument("--scale", type=float, default=12.0, help="Millions of sales rows target")
    args = ap.parse_args()

    # Volume mount path for Databricks SQL/Notebooks:
    vol_path = f"/Volumes/{args.catalog}/{args.seed_volume.replace('.', '/')}"
    os.makedirs(vol_path, exist_ok=True)

    dims = gen_dims(args.catalog, vol_path)
    stats = gen_facts(args.scale, *dims, vol_path)
    print("Generated:", stats)

if __name__ == "__main__":
    main()
