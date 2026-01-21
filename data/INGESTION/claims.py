from pyspark.sql import SparkSession #, functions as f
from pyspark.sql.functions import input_file_name, when

# Initialize Spark Session
spark = (
    SparkSession.builder
    .appName("Healthcare Claims Ingestion")
    .getOrCreate()
)

# Google Cloud Storage (GCS) Configuration
# SOURCE
BUCKET_NAME = "healthcare-bucket-086"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"

#DESTINATION
BQ_TABLE = "fifth-trainer-484215-c7.bronze_dataset.claims"

TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# read from claims source
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# adding hospital source for future reference
claims_df = (claims_df
                .withColumn("datasource", 
                               when(input_file_name().contains("hospital2"),"hosb")
                              .when(input_file_name().contains("hospital1"),"hosa")
                              .otherwise("None")
                                ))

claims_df = claims_df.dropDuplicates()
# write to bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())