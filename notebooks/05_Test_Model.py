## testing the model saving the predicted data back into sql server
import mlflow
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.ml.functions import vector_to_array

model_path = "/Volumes/db_02/default/mlflow_model_tmp/serialized_model"
staging_dir = "/Volumes/db_02/default/mlflow_model_tmp/tmp"
db_password = dbutils.secrets.get(scope="sqldb-pw", key="sqldb-pw")
jdbc_url = "jdbc:sqlserver://bootcamp01-server.database.windows.net:1433;database=bcdatabase"

connection_properties = {
    "user": "sai",
    "password": db_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

production_model = mlflow.spark.load_model(model_uri=model_path, dfs_tmpdir=staging_dir)

def predict_and_export(age_val, obs_count_val, condition_val):
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
    
    print(f"--- Prediction for Patient ---")
    print(f"Age: {age_val} | Observations: {obs_count_val} | Condition: {condition_val}")
    print(f"Result: {result_text}")
    print(f"Confidence: {probability_long_stay * 100:.2f}% chance of a Long Stay")

    final_output = (result_df.select(
        col("patient_age"),
        col("observation_count"),
        col("condition_name"),
        col("prediction"),
        vector_to_array(col("probability"))[1].alias("long_stay_probability"),
        current_timestamp().alias("prediction_at")
    ))
    
    (final_output.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.PatientPredictions")
        .option("user", connection_properties["user"])
        .option("password", connection_properties["password"])
        .mode("append")
        .save())
    
    print(f"✅ Data successfully appended to dbo.PatientPredictions\n")
production_model = mlflow.spark.load_model(
    model_uri="/Volumes/db_02/default/mlflow_model_tmp/serialized_model",
    dfs_tmpdir="/Volumes/db_02/default/mlflow_model_tmp/tmp"
)
predict_and_export(age_val=12, obs_count_val=2, condition_val="Unknown")
