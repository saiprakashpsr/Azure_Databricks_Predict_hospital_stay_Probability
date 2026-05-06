%run ./Source_setup
df_patients = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("path")
df_conditions = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("path")
df_observations = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("path")
df_encounters = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("path")
df_providers = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("path")
SAVING THE DATAFRAMES AS DELTA TABLES IN ADLS
df_patients.write.format("delta").mode("overwrite").saveAsTable("bronze_patients")
df_conditions.write.format("delta").mode("overwrite").saveAsTable("bronze_conditions")
df_observations.write.format("delta").mode("overwrite").saveAsTable("bronze_observations")
df_encounters.write.format("delta").mode("overwrite").saveAsTable("bronze_encounters")
df_providers.write.format("delta").mode("overwrite").saveAsTable("bronze_providers")
