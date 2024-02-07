from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import Row
from datetime import datetime, timedelta
import concurrent.futures
import logging
import hashlib
import gzip
import boto3
import botocore
import io
import json
import os

# Set up logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize Boto3 S3 client
s3 = boto3.client('s3')

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("test-job")

# Define SHA256 hashing function
def hash_field(field):
    if field:
        # Convert field to string before hashing
        field_str = str(field)
        # Convert the field to bytes
        field_bytes = field_str.encode()
        hashed_field = hashlib.sha256(field_bytes).hexdigest()
        return hashed_field
    else:
        return None

# Register UDF for SHA256 hashing
hash_field_udf = udf(hash_field, StringType())


total_count = 0

# Define transformation function
def transform_record(record):
    try:
        email = record.get("customer", {}).get("M", {}).get("email", {}).get("S", "")
        ip = record.get("request", {}).get("M", {}).get("ip_address", {}).get("S", "")
        mobile = record.get("customer", {}).get("M", {}).get("phone", {}).get("S", "")
        first_name = record.get("customer", {}).get("M", {}).get("first_name", {}).get("S", "")
        last_name = record.get("customer", {}).get("M", {}).get("last_name", {}).get("S", "")
        submitter = record.get("request", {}).get("M", {}).get("submitter", {}).get("S", "")

        # hashed
        hashed_ip = hash_field(ip)
        hashed_email = hash_field(email)
        hashed_mobile =  hash_field(mobile)
        hashed_first_name = hash_field(first_name)
        hashed_last_name = hash_field(last_name)
        hashed_submitter = hash_field(submitter)

        timestamp = record.get("timestamp", {}).get("S", "")
        # Calculate expiry time as timestamp + 3 years
        # Parse timestamp into a datetime object
        datetime_obj = datetime.strptime(timestamp[:19], "%Y-%m-%dT%H:%M:%S") # slicing :19 because of format error with nano seconds

        # Add three years to the timestamp
        expiry_time = datetime_obj + timedelta(days=3*365)

        # Format the expiry time as an RFC3339 string
        expiry_time_str = expiry_time.strftime("%Y-%m-%dT%H:%M:%S") + timestamp[19:]

        # Log the value of password to debug
        password = record.get("customer", {}).get("M", {}).get("password", {}).get("S", "")

        # Determine password_changed value based on the condition
        password_changed = password == "new"


        parquet_record = {
            "customer_code": record.get("customer_code", {}).get("S", ""), # partition key
            "timestamp": timestamp, # sort key
            # "created_at": , # no created_at field in ddb
            # "last_login": , # there is no field for storing last_login in ddb
            "expiry_time": expiry_time_str,
            "action": record.get("action", {}).get("S", ""),
            "email": hashed_email,
            # "email_verified": true/false, # we don't store email_verified in DDB
            "ip": hashed_ip,
            # "country": "", # geid present but not country
            "global_entity_id": record.get("request", {}).get("M", {}).get("global_entity_id", {}).get("S", ""),
            "password_changed": password_changed,
            "mobile": hashed_mobile,
            "mobile_verified": record.get("customer", {}).get("M", {}).get("mobile_verified", {}).get("BOOL", False),
            "first_name": hashed_first_name,
            "last_name": hashed_last_name,
            "submitter": hashed_submitter,
            "platform": record.get("request", {}).get("M", {}).get("jwt_client", {}).get("S", ""),
            "app_version": record.get("request", {}).get("M", {}).get("headers", {}).get("M", {}).get("App-Version", {}).get("S", ""),
        }

        csv_record_list = []
        # Append CSV records only if the corresponding fields exist
        if email:
            csv_record_list.extend(create_csv_records(customer_code=record.get("customer_code", {}).get("S", ""), timestamp=timestamp, field_name="email", field_value=email, hashed_value=hashed_email))
        if ip:
            csv_record_list.extend(create_csv_records(customer_code=record.get("customer_code", {}).get("S", ""), timestamp=timestamp, field_name="ip", field_value=ip, hashed_value=hashed_ip))
        if mobile:
            csv_record_list.extend(create_csv_records(customer_code=record.get("customer_code", {}).get("S", ""), timestamp=timestamp, field_name="mobile", field_value=mobile, hashed_value=hashed_mobile))
        if first_name:
            csv_record_list.extend(create_csv_records(customer_code=record.get("customer_code", {}).get("S", ""), timestamp=timestamp, field_name="first_name", field_value=first_name, hashed_value=hashed_first_name))
        if last_name:
            csv_record_list.extend(create_csv_records(customer_code=record.get("customer_code", {}).get("S", ""), timestamp=timestamp, field_name="last_name", field_value=last_name, hashed_value=hashed_last_name))
        if submitter:
            csv_record_list.extend(create_csv_records(customer_code=record.get("customer_code", {}).get("S", ""), timestamp=timestamp, field_name="submitter", field_value=submitter, hashed_value=hashed_submitter))

        return parquet_record, csv_record_list, timestamp[:10]
    except Exception as e:
        logger.error(f"Failed to transform record, Error: {e}")
        return None, None, None

# Function to create CSV records for a field
def create_csv_records(customer_code, timestamp, field_name, field_value, hashed_value):
    # Parse timestamp into a datetime object
    datetime_obj = datetime.strptime(timestamp[:19], "%Y-%m-%dT%H:%M:%S")

    # Add three years to the timestamp
    expiry_time = datetime_obj + timedelta(days=3*365)

    # Format the expiry time as an RFC3339 string
    expiry_time_str = expiry_time.strftime("%Y-%m-%dT%H:%M:%S") + timestamp[19:]
    return [{
        "schema": "customer_audit_log_staging",
        "customer_code": customer_code,
        "timestamp": timestamp,
        "expiry_time": expiry_time_str,
        "field": field_value,
        "hashed_field": hashed_value
    }]

def process_json_data(json_data):

    global total_count
    json_objects = json_data.strip().split('\n')
    current_count = 0
    failed_count = 0
    success_count = 0
    # available_dates = []
    for json_object in json_objects:
        current_count = current_count + 1
        json_reader = json.loads(json_object)

        # Perform any further processing or transformation as needed
        transformed_data = transform_record(json_reader["Item"])

        # Check if transformation was successful
        if transformed_data[0] is None or transformed_data[1] is None or transformed_data[2] is None:
            failed_count = failed_count + 1
            failed_output_path = f"s3://customer-id-test-glue/failed/failed_records.json"
            # Check if the file exists in S3
            try:
                s3.head_object(Bucket='customer-id-test-glue', Key='failed/failed_records.json')
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # File does not exist, create it
                    s3.put_object(Bucket='customer-id-test-glue', Key='failed/failed_records.json', Body='')
                    print("Created failed_records.json file in S3")

            # Write the JSON data for failed records
            try:
                s3.put_object(Bucket='customer-id-test-glue', Key='failed/failed_records.json', Body=json_object+'\n', ACL='bucket-owner-full-control')
                logger.info("Appended JSON record to failed_records.json file in S3")
            except Exception as e:
                logger.error(f"Failed to append JSON record to failed_records.json file: {e}")
        else:
            parquet_transformed_record = transformed_data[0]
            csv_transformed = transformed_data[1]
            date = transformed_data[2]

            if csv_transformed:
                # convert CSV transformed data to DataFrame
                csv_rows = [Row(**d) for d in csv_transformed]
                csv_transformed_df = spark.createDataFrame(csv_rows)
                csv_transformed_df = csv_transformed_df.repartition(1)

                csv_output_path = f"s3://customer-id-test-glue/successful/csv/{date}"
                csv_transformed_df.write.mode("append").option("header", "true").csv(csv_output_path)

            parquet_row = Row(**parquet_transformed_record)
            parquet_rows = [parquet_row]

            # Create DataFrame from Row object
            parquet_transformed_df = spark.createDataFrame(parquet_rows, schema=parquet_schema)
            parquet_transformed_df = parquet_transformed_df.repartition(1)
            # Write data to Parquet files partitioned by date
            parquet_output_path = f"s3://customer-id-test-glue/successful/parquet/{date}"
            parquet_transformed_df.write.mode("append").parquet(parquet_output_path)

            success_count = success_count + 1
    total_count = total_count + current_count
    logger.info(f"completed processing of current gzip file with total count: {current_count}, out of which {success_count} were successful and rest {failed_count} were failed\n")



# Function to process a gzip file
def process_gzip_file(bucket, key):
    # Download the gzip file contents
    response = s3.get_object(Bucket=bucket, Key=key)
    gzip_content = response['Body'].read()

    # Decompress the gzip file
    with gzip.open(io.BytesIO(gzip_content), 'rt') as f:
        json_data = f.read()

    # Process the json data
    process_json_data(json_data)

# Function to process S3 files recursively
def process_s3_files(bucket, prefix):
    # List objects in the bucket with the given prefix
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    # Process each object
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.gz'):
            process_gzip_file(bucket, key)

def process_s3_files_with_threads(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        # Process each object
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.gz'):
                futures.append(executor.submit(process_gzip_file, bucket, prefix))
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()

parquet_schema = StructType([
    StructField("customer_code", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("expiry_time", StringType(), nullable=False),
    StructField("action", StringType(), nullable=False),
    StructField("email", StringType(), nullable=True),
    StructField("ip", StringType(), nullable=True),
    StructField("global_entity_id", StringType(), nullable=True),
    StructField("password_changed", BooleanType(), nullable=True),
    StructField("mobile", StringType(), nullable=True),
    StructField("mobile_verified", BooleanType(), nullable=True),
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("submitter", StringType(), nullable=False),
    StructField("platform", StringType(), nullable=True),
    StructField("app_version", StringType(), nullable=True),
])

# Read data from S3
bucket_name = 'customer-id-test-glue'
folder_path = 'AWSDynamoDB/01707147231081-3011f252/data/'
process_s3_files(bucket_name, folder_path)

# Commit job bookmark
job.commit()
logger.info(f"In total processed {total_count} records")
logger.info(f"Data transformation complete.\n")