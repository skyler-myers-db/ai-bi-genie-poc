%pip install mlflow
%pip install --upgrade "typing_extensions>=4.12,<5"
# ====== Minimal seasonal forecaster: train + register (Unity Catalog) ======
# Expects the view genie_poc.gold.forecast_series_monthly with columns:
#   ds (DATE), product_id (INT), region_id (INT), y (DOUBLE)
# If you don't have it yet, create it once:
#   CREATE OR REPLACE VIEW genie_poc.gold.forecast_series_monthly AS
#   SELECT CAST(date_trunc('month', order_date) AS DATE) AS ds, product_id, region_id, SUM(qty) AS y
#   FROM genie_poc.silver.fact_sales_silver GROUP BY 1,2,3;

# ===== Seasonal forecaster: train + register (Unity Catalog) =====
# Source view must expose: ds (DATE), product_id (INT), region_id (INT), y (DOUBLE)

import os, json, tempfile
import pandas as pd, numpy as np
from pyspark.sql import functions as F
import mlflow, mlflow.pyfunc

# ---------- Context ----------
spark.sql("USE CATALOG genie_poc")
spark.sql("USE SCHEMA gold")

SRC_VIEW = "genie_poc.gold.forecast_series_monthly"
MODEL_NAME = "genie_poc.ml.units_forecaster_sku_region"  # UC registry path

# UC Volume path (governed, not DBFS root)
ART_DIR = (
    "/Volumes/genie_poc/ml/forecast_artifacts/seasonal"  # a subfolder inside the volume
)
os.makedirs(ART_DIR, exist_ok=True)

# ---------- Load monthly series ----------
df = (
    spark.table(SRC_VIEW)
    .select(
        F.to_date("ds").alias("ds"),
        F.col("product_id").cast("int").alias("product_id"),
        F.col("region_id").cast("int").alias("region_id"),
        F.col("y").cast("double").alias("y"),
    )
    .orderBy("product_id", "region_id", "ds")
)

required = {"ds", "product_id", "region_id", "y"}
missing = required - set(df.columns)
if missing:
    raise RuntimeError(f"Source {SRC_VIEW} missing columns: {missing}")

pdf = df.toPandas()
if pdf.empty:
    raise RuntimeError(f"No training data found in {SRC_VIEW}")

# ---------- Simple seasonal model ----------
def _fit_seasonal_model(series: pd.Series, season_length: int = 12):
    s = series.dropna().astype(float)
    if len(s) < season_length + 6:
        lvl = float(s.mean()) if len(s) else 0.0
        return {"level": lvl, "seasonal": [1.0] * season_length}
    idx_month = s.index.month
    month_means = s.groupby(idx_month).mean()
    seasonal = (
        (month_means / month_means.mean())
        .reindex(range(1, season_length + 1))
        .fillna(1.0)
        .values
    )
    lvl = float(s.tail(min(12, len(s))).mean())
    return {"level": lvl, "seasonal": seasonal.tolist()}


def _forecast(
    model_dict, start_ds: pd.Timestamp, periods: int = 1, season_length: int = 12
):
    level = model_dict["level"]
    seasonal = np.array(model_dict["seasonal"], dtype=float)
    out, cur = [], pd.Timestamp(start_ds)
    for _ in range(periods):
        cur = (cur + pd.offsets.MonthBegin(1)).normalize()
        out.append({"ds": cur, "yhat": float(level * seasonal[cur.month - 1])})
    return pd.DataFrame(out)


# ---------- Train per-(product, region) ----------
pdf["ds"] = pd.to_datetime(pdf["ds"])
models = {}
for (pid, rid), g in pdf.groupby(["product_id", "region_id"]):
    g = g.sort_values("ds").set_index("ds")
    m = _fit_seasonal_model(g["y"])
    models[f"{int(pid)}|{int(rid)}"] = m

# ---------- PyFunc wrapper (robust init) ----------
class SeasonalForecaster(mlflow.pyfunc.PythonModel):
    def __init__(self, models_dict=None):
        self._models = models_dict or {}

    def load_context(self, context):
        if not self._models:
            with open(context.artifacts["seasonal_json"], "r") as fh:
                self._models = json.load(fh)

    def _key(self, pid, rid):
        return f"{int(pid)}|{int(rid)}"

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        req = model_input.copy()
        if "horizon" not in req.columns:
            req["horizon"] = 1
        rows = []
        for _, r in req.iterrows():
            key = self._key(r["product_id"], r["region_id"])
            base = self._models.get(key, {"level": 0.0, "seasonal": [1.0] * 12})
            start = pd.to_datetime(r["ds"])
            fc = _forecast(base, start_ds=start, periods=int(r["horizon"]))
            fc["product_id"] = int(r["product_id"])
            fc["region_id"] = int(r["region_id"])
            rows.append(fc)
        return (
            pd.concat(rows, ignore_index=True)
            if rows
            else pd.DataFrame(columns=["ds", "yhat", "product_id", "region_id"])
        )


# ---------- Persist artifact to the UC Volume (atomic write) ----------
seasonal_path = os.path.join(ART_DIR, "seasonal.json")
tmp_path = seasonal_path + ".tmp"

with open(tmp_path, "w") as fh:
    json.dump(models, fh)
    fh.flush()
    os.fsync(fh.fileno())
os.replace(tmp_path, seasonal_path)  # atomic rename

# ---------- Log & register in UC ----------
mlflow.set_registry_uri(
    "databricks-uc"
)  # UC model registry (default in MLflow 3, explicit here)  # noqa: E265

# Build input & output examples WITHOUT calling the model (no file I/O at all)
example_in = pd.DataFrame(
    {
        "product_id": [int(pdf["product_id"].iloc[0])],
        "region_id": [int(pdf["region_id"].iloc[0])],
        "ds": [pd.Timestamp(pdf["ds"].max())],
        "horizon": [2],
    }
)
# Construct example output directly from in-memory model dict
k = f"{int(example_in.iloc[0]['product_id'])}|{int(example_in.iloc[0]['region_id'])}"
base = models.get(k, {"level": 0.0, "seasonal": [1.0] * 12})
example_out = _forecast(
    base,
    start_ds=pd.Timestamp(example_in.iloc[0]["ds"]),
    periods=int(example_in.iloc[0]["horizon"]),
)
example_out["product_id"] = int(example_in.iloc[0]["product_id"])
example_out["region_id"] = int(example_in.iloc[0]["region_id"])

signature = mlflow.models.infer_signature(example_in, example_out)

with mlflow.start_run(run_name="units_forecaster_sku_region") as run:
    mlflow.pyfunc.log_model(
        name="model",
        python_model=SeasonalForecaster(),  # loads from artifact at inference time
        artifacts={
            "seasonal_json": seasonal_path
        },  # UC Volume file; MLflow copies it into the model
        signature=signature,
        input_example=example_in,
        registered_model_name=MODEL_NAME,
        conda_env={
            "name": "seasonal-forecaster",
            "channels": ["conda-forge"],
            "dependencies": [
                "python=3.10",
                {
                    "pip": [
                        "mlflow>=2.11,<3",
                        "pandas>=2.1,<3",
                        "numpy>=1.26,<3",
                        "typing_extensions>=4.12",
                    ]
                },
            ],
        },
    )

print(f"Registered model to UC: {MODEL_NAME}")
print(f"Seasonal JSON logged from Volume path: {seasonal_path}")