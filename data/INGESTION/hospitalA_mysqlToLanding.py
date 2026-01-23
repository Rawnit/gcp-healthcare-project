import datetime
import json
import pandas as pd
import socket  # Required for connectivity check
import sys     # Required to stop script if network fails
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession

# 1. --- CONNECTIVITY PRE-CHECK ---
# This checks if the network path is open before Spark even starts.
def verify_network(host, port):
    print(f"Checking connectivity to {host}:{port}...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    try:
        s.connect((host, port))
        print("✅ Network is OPEN.")
        return True
    except Exception as e:
        print(f"❌ Network is BLOCKED: {e}")
        return False
    finally:
        s.close()

if not verify_network("35.222.242.255", 3306):
    sys.exit("Stopping: Check GCP Firewall or Cloud SQL 'Authorized Networks' for IP 35.222.242.255")

# 2. --- INITIALIZE SPARK WITH CONNECTOR ---
# Added 'spark.jars.packages' to ensure Spark has the MySQL driver.
spark = (
    SparkSession.builder
    .appName("HospitalAMySQLToLanding")
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28")
    .getOrCreate()
)

# Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Configuration
GCS_BUCKET = "healthcare-bucket-086"
HOSPITAL_NAME = "hospital-a"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

BQ_PROJECT = "fifth-trainer-484215-c7"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# MySQL Config - Changed useSSL to false for easier debugging initially
MYSQL_CONFIG = {
    "url": "jdbc:mysql://35.222.242.255:3306/hospital_a_db?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Warvickz191#"
}

# --- LOGGING ---
log_entries = []
def log_event(event_type, message, table=None):
    log_entry = {"timestamp": datetime.datetime.now().isoformat(), "event_type": event_type, "message": message, "table": table}
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs():
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"  
    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")
    print(f"✅ Logs saved to GCS: gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery").option("table", BQ_LOG_TABLE).option("temporaryGcsBucket", BQ_TEMP_PATH).mode("append").save()
        print("✅ Logs stored in BigQuery")

# --- DATA FUNCTIONS ---
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]
    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()
        log_event("INFO", f"Archived {file}", table=table)

def get_latest_watermark(table_name):
    query = f"SELECT MAX(load_timestamp) AS ts FROM `{BQ_AUDIT_TABLE}` WHERE tablename = '{table_name}'"
    result = bq_client.query(query).result()
    for row in result:
        return row.ts if row.ts else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"

def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full" else f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query).load())

        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"
        
        # Write to GCS via Pandas (lines=True is best for BigQuery later)
        json_str = df.toPandas().to_json(orient="records", lines=True)
        storage_client.bucket(GCS_BUCKET).blob(JSON_FILE_PATH).upload_from_string(json_str)

        log_event("SUCCESS", f"Extracted {table}", table=table)

        # Audit Entry
        audit_data = [("hospital_a_db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")]
        audit_df = spark.createDataFrame(audit_data, ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])
        audit_df.write.format("bigquery").option("table", BQ_AUDIT_TABLE).option("temporaryGcsBucket", GCS_BUCKET).mode("append").save()

    except Exception as e:
        log_event("ERROR", f"Failed {table}: {str(e)}", table=table)

# --- EXECUTION ---
config_df = spark.read.csv(CONFIG_FILE_PATH, header=True)
for row in config_df.collect():
    if row["is_active"] == '1' and row["datasource"] == "hospital_a_db":
        move_existing_files_to_archive(row["table"])
        extract_and_save_to_landing(row["table"], row["load_type"], row["watermark_column"])

save_logs_to_gcs()
save_logs_to_bigquery()
