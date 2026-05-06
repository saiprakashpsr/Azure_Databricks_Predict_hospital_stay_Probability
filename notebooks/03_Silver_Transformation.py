%run ./Source_setup
bronze_patients = spark.read.table("bronze_patients")
bronze_conditions = spark.read.table("bronze_conditions")
bronze_encounters = spark.read.table("bronze_encounters")
bronze_observations = spark.read.table("bronze_observations")
bronze_providers = spark.read.table("bronze_providers")
from pyspark.sql.functions import col, sha2
## Creating a hash key for provider key using sha2 function standardizing the columns for easier interpretation and then saving the table as delta in hive metastore for quicker access
df_silver_providers = (
    bronze_providers
    .dropDuplicates(['ID'])
    .select(
        sha2(col("id"), 256).alias("provider_key"),
        col("ID").alias("provider_bronze_id"),
        col("NAME").alias("provider_name"),
        col("GENDER").alias("provider_gender"),
        col("SPECIALITY").alias("provider_speciality")
            )
)
df_silver_providers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_providers")
silver_providers = spark.table("silver_providers")
from pyspark.sql.functions import lit, current_date
## Creating hash patient key and adding effective_start_date, effective_end_date and is_current flag to implement SCD-2.
df_silver_patients = (
    bronze_patients
    .select(
        sha2(col("id"), 256).alias("patient_key"),
        col("ID").alias("patient_bronze_id"),
        col("FIRST").alias("first_name"),
        col("LAST").alias("last_name"),
        col("BIRTHDATE").alias("birth_date"),
        col("GENDER").alias("gender"),
        col("ADDRESS").alias("address"),
        col("CITY").alias("city"),
        col("STATE").alias("state"),
    )
    .withColumn("effective_start_date", current_date())
    .withColumn("effective_end_date", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True))
)
df_silver_patients.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_patients")
silver_patients = spark.table("silver_patients")
## Joining the ecounters table with patients table to integrate the surrogate hash keys and all metrics
from pyspark.sql.functions import sha2, col
df_encounters_key = (

    bronze_encounters
    .join(silver_patients, bronze_encounters["PATIENT"] == silver_patients["patient_bronze_id"], "left")
    .join(silver_providers, bronze_encounters["PROVIDER"] == silver_providers["provider_bronze_id"], "left")
    .select(
        sha2(col("Id"), 256).alias("encounter_key"),
        col("Id").alias("encounter_bronze_id"),
        col("patient_key"),
        col("provider_key"),
        col("START").alias("start_time"),
        col("STOP").alias("stop_time"),
        col("ENCOUNTERCLASS").alias("encounter_class"),
        col("BASE_ENCOUNTER_COST").alias("base_cost"),
        col("TOTAL_CLAIM_COST").alias("total_cost"),
        col("PAYER_COVERAGE").alias("coverage")
    )
)

df_encounters_key.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_encounters")
silver_encounters = spark.table("silver_encounters")
## join the observations with providers and patients table and casting the value column to double datatype
df_silver_observations = (
    bronze_observations
    .join(silver_patients, bronze_observations['PATIENT'] == silver_patients['patient_bronze_id'], "left")
    .join(silver_encounters, col("ENCOUNTER") == col("encounter_bronze_id"), "left")
    .select(
        col("silver_patients.patient_key"),
        col("DATE").alias("observation_date"),
        col("CODE").alias("observation_code"),
        col("DESCRIPTION").alias("observation_description"),
        col("VALUE").try_cast("double").alias("observation_value"),
        col("UNITS").alias("observation_units")
    )
)
df_silver_observations.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_observations")
silver_observations = spark.table("silver_observations")
## join the conditions table with patients table and casting the start and stop columns to date datatype and dropping all duplicate records
df_silver_conditions = (
    bronze_conditions
    .join(silver_patients, col("PATIENT") == col("patient_bronze_id"), "left")
    .join(silver_encounters, col("ENCOUNTER") == col("encounter_bronze_id"), "left")
    .select(
        col("silver_patients.patient_key"),
        col("silver_encounters.encounter_key"),
        col("START").cast("date").alias("start_date"),
        col("STOP").cast("date").alias("stop_date"),
        col("CODE").alias("condition_code"),
        col("DESCRIPTION").alias("description")
    )
    .dropDuplicates()
)
df_silver_conditions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_conditions")
silver_conditions = spark.table("silver_conditions")
