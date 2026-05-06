## Building gold tables for predicting length of stay and complication risk
%sql
CREATE OR REPLACE TABLE gold_ML_features AS
WITH encounter_conditions AS (
  SELECT 
    encounter_key, 
    description as condition_name 
  FROM silver_conditions
  QUALIFY row_number() OVER (PARTITION BY encounter_key ORDER BY start_date DESC) = 1
),
base_features AS (
  SELECT
      e.encounter_bronze_id,
      e.encounter_key, 
      p.patient_key,
      datediff(e.start_time, p.birth_date) / 365 as patient_age,
      CASE 
          WHEN datediff(e.stop_time, e.start_time) > 10 THEN 1
          ELSE 0
      END as target_long_stay,
      observation_count,
      c.condition_name
  FROM silver_encounters e
  JOIN silver_patients p ON e.patient_key = p.patient_key
  LEFT JOIN (
      SELECT patient_key, COUNT(*) as observation_count
      FROM silver_observations
      GROUP BY patient_key
  ) observation_summary ON e.patient_key = observation_summary.patient_key
  LEFT JOIN encounter_conditions c ON e.encounter_key = c.encounter_key
  WHERE p.is_current = true
)
SELECT * FROM base_features WHERE target_long_stay = 1
UNION ALL
SELECT * FROM base_features 
WHERE target_long_stay = 0 
  AND rand() < 0.10
import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import col

df_gold = spark.table("gold_ML_features")

df_gold = df_gold.fillna({
    "observation_count": 0, 
    "condition_name": "Unknown"
})

df_gold = df_gold.withColumn("patient_age", col("patient_age").cast("double")) \
                 .withColumn("observation_count", col("observation_count").cast("double")) \
                 .withColumn("target_long_stay", col("target_long_stay").cast("double"))
from pyspark.sql.functions import col

df_gold = spark.table("gold_ML_features")

df_gold = df_gold.fillna({
    "observation_count": 0, 
    "condition_name": "Unknown"
})

df_gold = df_gold.withColumn("patient_age", col("patient_age").cast("double")) \
                 .withColumn("observation_count", col("observation_count").cast("double")) \
                 .withColumn("target_long_stay", col("target_long_stay").cast("double"))


long_stay_count = df_gold.filter(col("target_long_stay") == 1.0).count()
short_stay_count = df_gold.filter(col("target_long_stay") == 0.0).count()

print(f"Original Long Stays: {long_stay_count}")
print(f"Original Short Stays: {short_stay_count}")

if short_stay_count > 0:
    fraction_to_keep = long_stay_count / short_stay_count
else:
    fraction_to_keep = 1.0

fractions = {1.0: 1.0, 0.0: fraction_to_keep}
df_balanced = df_gold.sampleBy("target_long_stay", fractions=fractions, seed=42)

print(f"Balanced Data Count: {df_balanced.count()}")
df_balanced.groupBy("target_long_stay").count().show()

df_gold = df_balanced
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

indexer = StringIndexer(
    inputCol="condition_name", 
    outputCol="condition_index", 
    handleInvalid="keep" 
)


encoder = OneHotEncoder(
    inputCol="condition_index", 
    outputCol="condition_vec"
)

assembler = VectorAssembler(
    inputCols=["patient_age", "observation_count", "condition_vec"],
    outputCol="features"
)
train_data, test_data = df_gold.randomSplit([0.8, 0.2], seed=42)

print(f"Training Data Count: {train_data.count()}")
print(f"Testing Data Count: {test_data.count()}")
%sql
CREATE VOLUME IF NOT EXISTS db_02.default.mlflow_model_tmp;
import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from mlflow.models.signature import infer_signature

rf_trees = 100
rf_depth = 7
uc_volume_tmp_path = "/Volumes/db_02/default/mlflow_model_tmp/tmp"
registered_model_name = "db_02.default.hospital_los_predictor"

with mlflow.start_run(run_name="Predict_Covid_Long_Stay_Balanced"):
    
    rf = RandomForestClassifier(
        labelCol="target_long_stay", 
        featuresCol="features", 
        numTrees=rf_trees,      
        maxDepth=rf_depth,        
        seed=42
    )

    pipeline = Pipeline(stages=[indexer, encoder, assembler, rf])

    model = pipeline.fit(train_data)
    
    mlflow.log_param("numTrees", rf_trees)
    mlflow.log_param("maxDepth", rf_depth)
    
    predictions_for_signature = model.transform(train_data)
    signature = infer_signature(
        train_data.drop("target_long_stay"), 
        predictions_for_signature.select("prediction")
    )
    
    mlflow.set_registry_uri("databricks-uc")
    
    mlflow.spark.log_model(
        spark_model=model, 
        artifact_path="random_forest_model", 
        dfs_tmpdir=uc_volume_tmp_path,
        registered_model_name=registered_model_name,
        signature=signature
    )
  from pyspark.ml.evaluation import BinaryClassificationEvaluator
import mlflow

predictions = model.transform(test_data)



evaluator = BinaryClassificationEvaluator(
    labelCol="target_long_stay",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

auc = evaluator.evaluate(predictions)
print(f"Model Area Under ROC (AUC): {auc:.4f}")

last_run = mlflow.last_active_run()

if last_run:
    with mlflow.start_run(run_id=last_run.info.run_id):
        mlflow.log_metric("test_auc", auc)
        print("Test AUC successfully logged to MLflow.")
else:
    print("Could not find active MLflow run to log metrics.")
  
final_predictions = predictions.select(
    col("encounter_bronze_id").alias("encounter_id"),
    col("patient_age"),
    col("condition_name"),
    col("target_long_stay").alias("actual_stay_type"),
    col("prediction").alias("predicted_stay_type")
)

final_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("platinum_covid_length_of_stay_predictions")
import os
import shutil
import mlflow.spark
from pyspark.sql.functions import lit, current_timestamp

uc_volume_path = "/Volumes/db_02/default/mlflow_model_tmp"
model_staging_path = f"{uc_volume_path}/serialized_model"

local_scratch_dir = "/local_disk0/tmp"
local_zip_path = f"{local_scratch_dir}/los_model_final" 

os.makedirs(local_scratch_dir, exist_ok=True) 

shutil.make_archive(local_zip_path, 'zip', model_staging_path)

db_password = dbutils.secrets.get(scope="sqldb-pw", key="sqldb-pw")
jdbc_url = "jdbc:sqlserver://bootcamp01-server.database.windows.net:1433;database=bcdatabase"


with open(f"{local_zip_path}.zip", "rb") as f:
    model_binary = f.read()


model_df = spark.createDataFrame([(model_binary,)], ["model_binary"]) \
    .withColumn("model_name", lit("Length_of_Stay_RF_Model")) \
    .withColumn("auc_score", lit(0.9517)) \
    .withColumn("exported_at", current_timestamp())

model_df.write \
     .format("jdbc") \
     .option("url", jdbc_url) \
     .option("dbtable", "dbo.SavedMLModels") \
     .option("user", "sai") \
     .option("password", db_password) \
     .mode("overwrite") \
     .save()

import os
import shutil
from mlflow.spark import load_model

local_scratch_dir = "/local_disk0/tmp"
os.makedirs(local_scratch_dir, exist_ok=True) 

local_zip_file = f"{local_scratch_dir}/reconstructed_model.zip"
local_extract_path = f"{local_scratch_dir}/final_model_inference"

with open(local_zip_file, "wb") as f:
    f.write(retrieved_binary)

if os.path.exists(local_extract_path):
    shutil.rmtree(local_extract_path)
shutil.unpack_archive(local_zip_file, local_extract_path, "zip")

final_model = load_model(
    local_extract_path, 
    dfs_tmpdir="/Volumes/db_02/default/mlflow_model_tmp/tmp" 
)
import mlflow
from pyspark.sql.functions import lit

model_path = "/Volumes/db_02/default/mlflow_model_tmp/serialized_model"
staging_dir = "/Volumes/db_02/default/mlflow_model_tmp/tmp"

production_model = mlflow.spark.load_model(
    model_uri=model_path, 
    dfs_tmpdir=staging_dir
)

def predict_stay_with_confidence(age_val, obs_count_val, condition_val):
    template_df = spark.table("gold_ML_features").limit(1)
    

    new_data = template_df \
        .withColumn("patient_age", lit(age_val)) \
        .withColumn("observation_count", lit(obs_count_val)) \
        .withColumn("condition_name", lit(condition_val))
    

    result_df = production_model.transform(new_data)
    
    row = result_df.select("prediction", "probability").collect()[0]
    prediction_value = row["prediction"]
    probability_long_stay = row["probability"][1] 
    

    result_text = "Long Stay (>10 days)" if prediction_value == 1.0 else "Short Stay (<=10 days)"
    


