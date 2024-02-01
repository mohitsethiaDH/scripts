from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import hashlib

# Set up logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("your-job-name", args)

# Define SHA256 hashing function
def hash_field(field):
    if field:
        hashed_field = hashlib.sha256(field.encode()).hexdigest()
        return hashed_field
    else:
        return None

# Register UDF for SHA256 hashing
hash_field_udf = udf(hash_field, StringType())

# Define transformation function
def transform_record(record):
    try:
        # hashed
        hashed_ip = hash_field(record["request"]["ip_address"])
        hashed_email = hash_field(record["customer"]["email"])
        hashed_mobile =  hash_field(record["customer"]["phone"])
        hashed_first_name = hash_field(record["customer"]["first_name"])
        hashed_last_name = hash_field(record["customer"]["last_name"])
        hashed_submitter = hash_field(record["request"]["submitter"])

        date = record["timestamp"][:10]

        parquet_record = {
            "customer_code": record["customer_code"], # partition key
            "timestamp": record["timestamp"], # sort key
            # "created_at": , # no created_at field in ddb
            # "last_login": , # there is no field for storing last_login in ddb
            "action": record["action"],
            "email": hashed_email, # Hash email with SHA256,
            # "email_verified": true/false, # we don't store email_verified in DDB
            "ip": hashed_ip, # hashed
            # "country": "", # geid present but not country
            "global_entity_id": record["request"]["global_entity_id"],
            "password_changed": False if record["password"] == "old" else True,
            "mobile": hashed_mobile,
            "mobile_verified": record["customer"]["mobile_verified"],
            "first_name": hashed_first_name,
            "last_name": hashed_last_name,
            "submitter": hashed_submitter,
            "platform": record["request"]["jwt_client"],
            "app_version": record["request"]["headers"]["App-Version"],
        }

        csv_record_list = create_csv_records(customer_code=record["customer_code"], timestamp=record["timestamp"], field_name="email", field_value=record["customer"]["email"], hashed_value=hashed_email)
        csv_record_list.extend(create_csv_records(customer_code=record["customer_code"], timestamp=record["timestamp"], field_name="ip", field_value=record["request"]["ip_address"], hashed_value=hashed_ip))
        csv_record_list.extend(create_csv_records(customer_code=record["customer_code"], timestamp=record["timestamp"], field_name="mobile", field_value=record["customer"]["phone"], hashed_value=hashed_mobile))
        csv_record_list.extend(create_csv_records(customer_code=record["customer_code"], timestamp=record["timestamp"], field_name="first_name", field_value=record["customer"]["first_name"], hashed_value=hashed_first_name))
        csv_record_list.extend(create_csv_records(customer_code=record["customer_code"], timestamp=record["timestamp"], field_name="last_name", field_value=record["customer"]["last_name"], hashed_value=hashed_last_name))
        csv_record_list.extend(create_csv_records(customer_code=record["customer_code"], timestamp=record["timestamp"], field_name="submitter", field_value=record["request"]["submitter"], hashed_value=hashed_submitter))


        return parquet_record, csv_record_list, date
    except Exception as e:
        logger.error(f"Failed to transform record: {record}, Error: {e}")
        return None, None, None

# Function to create CSV records for a field
def create_csv_records(customer_code, timestamp, field_name, field_value, hashed_value):
    return [{
        "schema": "customer_audit_log_staging",
        "timestamp": timestamp,
        "customer_code": customer_code,
        field_name: field_value,
        f"hashed_{field_name}": hashed_value
    }]

# Read data from S3
source_data = glueContext.create_dynamic_frame.from_catalog(
    database="your_database_name",
    table_name="asia-staging-customer-logs-fp-ph",
    transformation_ctx="datasource"
)

# Convert DynamicFrame to DataFrame
data_frame = source_data.toDF()

# Apply transformation
transformed_data = data_frame.rdd.map(transform_record)

# Count successful and failed entries
successful_count = transformed_data.filter(lambda x: x[0] is not None and x[1] is not None).count()
failed_count = data_frame.count() - successful_count
logger.info(f"successful entries: {successful_count}, Failed entries: {failed_count}")

# Filter out failed records
failed_records = data_frame.filter(lambda x: x[0] is None or x[1] is None)

# Write failed records to a different file
if failed_count > 0:
    failed_output_path = "s3://failed_output_path/"
    failed_records.write.mode("append").parquet(failed_output_path)

# Convert RDD back to DataFrame
parquet_transformed_df = transformed_data.filter(lambda x: x[0] is not None).map(lambda x: x[0]).toDF()

# Write data to Parquet files partitioned by date
parquet_output_path = "s3://"
parquet_transformed_df.write.partitionBy("date").mode("append").parquet(parquet_output_path)

csv_transformed_df = transformed_data.flatMap(lambda x: x[1] is not None).toDF()

csv_output_path = "s3://"
csv_transformed_df.write.mode("append").option("header", "false").CSV(csv_output_path)

# Commit job bookmark
job.commit()

logger.info(f"Data transformation complete.")