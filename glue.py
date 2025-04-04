import sys
import re
import boto3
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col

# --- Step 1: Setup ---
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'dst_bucket_name', 'file_name'])
bucket = args['dst_bucket_name']
file_name = args['file_name']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

base_prefix = "des_file"
timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
input_path = f"s3://{bucket}/intermediate_data/world/Person_Lewis/{file_name}"
temp_output_path = f"s3://{bucket}/{base_prefix}/temp_output_{timestamp}/"
final_output_key = f"{base_prefix}/final_output.csv"  # <-- final file name

# --- Step 2: Read Input (No Header) ---
df = spark.read.option("header", False).csv(input_path)
num_cols = len(df.columns)

if num_cols == 4:
    file_type = "cdc"
elif num_cols == 3:
    file_type = "snapshot"
else:
    raise ValueError(f"❌ Unexpected number of columns: {num_cols}")

logger.info(f"✅ Detected file type: {file_type}")

# --- Step 3: Apply Logic Based on File Type ---
if file_type == "cdc":
    df = df.toDF("Op", "PersonID", "FullName", "City") \
           .withColumn("PersonID", col("PersonID").cast("int"))

    try:
        existing = spark.read.option("header", True).csv(f"s3://{bucket}/{final_output_key}")
        existing = existing.withColumn("PersonID", col("PersonID").cast("int")) \
                           .select("PersonID", "FullName", "City")
    except:
        logger.warn("⚠️ No existing final file found. Starting fresh.")
        existing = spark.createDataFrame([], "PersonID INT, FullName STRING, City STRING")

    inserts = df.filter(col("Op") == "I").select("PersonID", "FullName", "City")
    updates = df.filter(col("Op") == "U").select("PersonID", "FullName", "City")
    deletes = df.filter(col("Op") == "D").select("PersonID")

    logger.info(f"Inserts: {inserts.count()}, Updates: {updates.count()}, Deletes: {deletes.count()}")

    current = (
        existing.join(deletes, on="PersonID", how="left_anti")
                .join(updates.select("PersonID"), on="PersonID", how="left_anti")
    )

    df = current.unionByName(inserts).unionByName(updates)

else:
    df = df.toDF("PersonID", "FullName", "City") \
           .withColumn("PersonID", col("PersonID").cast("int"))

logger.info(f"✅ Final row count to write: {df.count()}")

# --- Step 4: Write to Temporary Folder ---
df.repartition(1).write.mode("overwrite").option("header", True).csv(temp_output_path)
logger.info(f"📁 Written to temp path: {temp_output_path}")

# --- Step 5: Move part file to final_output.csv ---
def move_part_to_final(bucket, temp_path, final_key):
    s3 = boto3.resource('s3')
    bucket_obj = s3.Bucket(bucket)
    prefix = "/".join(temp_path.replace("s3://", "").split("/")[1:])  # remove bucket name

    for obj in bucket_obj.objects.filter(Prefix=prefix):
        if re.match(r".*/part-.*\.csv$", obj.key):
            logger.info(f"🔁 Found part file: {obj.key}")
            copy_source = {'Bucket': bucket, 'Key': obj.key}
            s3.Object(bucket, final_key).copy(copy_source)
            logger.info(f"✅ Copied to final: s3://{bucket}/{final_key}")
            return
    logger.error("❌ No part file found to move.")

move_part_to_final(bucket, temp_output_path, final_output_key)

# --- Step 6: Cleanup temp folder ---
def cleanup_temp_folder(bucket, temp_path):
    s3 = boto3.client('s3')
    prefix = "/".join(temp_path.replace("s3://", "").split("/")[1:])
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" in response:
        delete_keys = [{'Key': obj['Key']} for obj in response['Contents']]
        s3.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
        logger.info(f"🧹 Cleaned up temp folder: {prefix}")
    else:
        logger.info("ℹ️ No temp objects to clean.")

cleanup_temp_folder(bucket, temp_output_path)

# --- Step 7: Done ---
job.commit()
