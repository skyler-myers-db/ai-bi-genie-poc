import pandas as pd, numpy as np
from pyspark.sql import functions as F
import mlflow, mlflow.pyfunc

spark.sql("USE CATALOG genie_poc")
spark.sql("USE SCHEMA gold")

df = spark.table("fact_sales").select("order_date","product_family","region_name","qty").toPandas()
# simple seasonal naive: next month = avg of last 3 same-family-region months
df['month'] = pd.to_datetime(df['order_date']).dt.to_period('M').dt.to_timestamp()
grp = df.groupby(['product_family','region_name','month'], as_index=False)['qty'].sum().sort_values('month')

class NaiveForecaster(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        # expects columns: product_family, region_name, horizon_months
        out = []
        for _, r in model_input.iterrows():
            hist = grp[(grp.product_family==r['product_family']) & (grp.region_name==r['region_name'])].tail(3)
            base = hist['qty'].mean() if len(hist)>0 else 100.0
            horizon = int(r.get('horizon_months', 1))
            out.append(base * (1.0 + 0.02*horizon))  # tiny growth
        return pd.DataFrame({"forecast_units": out})

with mlflow.start_run():
    model = NaiveForecaster()
    conda_env = mlflow.pyfunc.get_default_conda_env()
    mlflow.pyfunc.log_model("model", python_model=model, conda_env=conda_env)
    model_uri = mlflow.get_artifact_uri("model")

print("Registered model URI:", model_uri)
