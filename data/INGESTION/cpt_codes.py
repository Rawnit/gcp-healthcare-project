from pyspark.sql import SparkSession #, functions as f
from pyspark.sql.functions import input_file_name, when

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("CPT Codes Ingestion")
    .getOrCreate()
)

# Google Cloud Storage (GCS) Configuration
# SOURCE
BUCKET_NAME = "healthcare-bucket-086"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"

#DESTINATION
BQ_TABLE = "fifth-trainer-484215-c7.bronze_dataset.cpt_codes"

TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from cpt source
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

for col in cptcodes_df.columns:
    new_col = col.replace(" ","_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)


# write to bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())