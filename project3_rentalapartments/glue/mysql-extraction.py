import sys
import traceback

# Helper function to guarantee unbuffered output to CloudWatch
def log(message):
    print(f"[GLUE_LOG] {message}", flush=True)

log("Starting execution setup...")

# --- Step 1: Module Imports ---
try:
    log("Importing standard and AWS libraries...")
    import boto3, csv, pymysql, io, json
    from botocore.exceptions import ClientError
    from awsglue.utils import getResolvedOptions
    log("Libraries imported successfully.")
except Exception as e:
    log(f"FATAL: Library import failed: {e}")
    traceback.print_exc()
    sys.exit(1)

# --- Step 2: Argument Resolution ---
try:
    log("Parsing Glue Job arguments...")
    args = getResolvedOptions(sys.argv, ["table_name", "load_type"])
    table_name = args["table_name"]
    load_type = args["load_type"]
    log(f"Arguments parsed -> table_name: {table_name}, load_type: {load_type}")
except Exception as e:
    log(f"FATAL: Failed to resolve parameters 'table_name' or 'load_type'. Ensure Job Parameters pass '--table_name' and '--load_type': {e}")
    traceback.print_exc()
    sys.exit(1)

# --- Configuration & Helper Functions ---
AWS_REGION = 'ap-south-1'
s3_bucket = "dcn-dataengg"
s3_key = f'raw_landing_zone/apartment_db/{table_name}/data.csv'

def get_rds_credentials(secret_name, region_name):
    log(f"Fetching secret '{secret_name}' from Secrets Manager...")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            log("Successfully retrieved secret string.")
            return json.loads(get_secret_value_response['SecretString'])
        else:
            log("ERROR: Secret response does not contain 'SecretString'.")
            return None
    except ClientError as e:
        log(f"ERROR: Secrets Manager API call failed: {e}")
        return None

# --- Step 3: Secrets Manager Credentials ---
credentials = get_rds_credentials("rental_db", AWS_REGION)
if not credentials:
    log("FATAL: Could not retrieve RDS credentials. Exiting.")
    sys.exit(1)

user_name = credentials['username']
host = credentials['host']
password = credentials['password']
db_name = "rental_apartments"

# --- Step 4: DynamoDB Configuration Check ---
log("Initializing DynamoDB resource...")
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
config_table = dynamodb.Table('incremental_load_configurations')

def fetch_configurations(tbl_name):
    try:
        log(f"Fetching incremental config from DynamoDB for table '{tbl_name}'...")
        response = config_table.get_item(Key={'table_name': tbl_name})
        if 'Item' in response:
            log("DynamoDB item found.")
            return response['Item'].get('load_column'), response['Item'].get('last_extracted_value')
        else:
            log(f"ERROR: No item in DynamoDB for table_name '{tbl_name}'.")
            return None, None
    except Exception as e:
        log(f"ERROR: DynamoDB query failed: {e}")
        return None, None

incr_column, last_extracted_value = None, None

if load_type == 'incremental':
    incr_column, last_extracted_value = fetch_configurations(table_name)
    log(f"Incremental parameters -> Column: {incr_column}, Last Value: {last_extracted_value}")
    if not incr_column:
        log("FATAL: Incremental column missing from config. Exiting.")
        sys.exit(1)

def update_last_extracted_value(tbl_name, last_val):
    try:
        config_table.update_item(
            Key={'table_name': tbl_name},
            UpdateExpression="SET last_extracted_value = :val",
            ExpressionAttributeValues={':val': last_val},
            ReturnValues="UPDATED_NEW"
        )
        log(f"Updated DynamoDB last_extracted_value to {last_val}")
    except Exception as e:
        log(f"ERROR: DynamoDB update failed: {e}")

def convert_to_csv(data):
    if not data:
        return ""
    csv_file = io.StringIO()
    writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    return csv_file.getvalue()

# --- Step 5 & 6: Main ETL Execution ---
def main():
    connection = None
    try:
        log(f"Connecting to MySQL database '{db_name}' at {host}...")
        connection = pymysql.connect(
            host=host,
            user=user_name,
            password=password,
            database=db_name,
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )
        log("Database connection established.")

        with connection.cursor() as cursor:
            sql = f"SELECT * FROM {table_name}"
            if load_type == 'incremental' and incr_column and last_extracted_value:
                sql += f" WHERE {incr_column} > '{last_extracted_value}' ORDER BY {incr_column} DESC"
            
            log(f"Executing Query: {sql}")
            cursor.execute(sql)
            result = cursor.fetchall()
            log(f"Query returned {len(result)} records.")

        csv_data = convert_to_csv(result)

        log(f"Uploading output CSV to s3://{s3_bucket}/{s3_key}...")
        s3 = boto3.client('s3', region_name=AWS_REGION)
        s3.put_object(Body=csv_data, Bucket=s3_bucket, Key=s3_key)
        log("S3 write completed successfully.")

        if result and load_type == 'incremental':
            new_last_value = result[0][incr_column]
            update_last_extracted_value(table_name, str(new_last_value))

    except Exception as e:
        log(f"FATAL: Error during ETL execution: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if connection:
            connection.close()
            log("Database connection closed.")

if __name__ == '__main__':
    main()