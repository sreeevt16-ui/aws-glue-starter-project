import requests
import time
import json
from datetime import datetime
from pyspark.sql import SparkSession

# -----------------------------
# Initialize Spark session
# -----------------------------
spark = SparkSession.builder \
    .appName("NIHReporterIngest") \
    .getOrCreate()

# -----------------------------
# Config / variables
# -----------------------------
url = "https://api.reporter.nih.gov/v2/projects/search"
s3_bucket = "nih-reporter-starter-project"
LIMIT = 500
offset = 0
total_records = []

# -----------------------------
# Pull data from NIH Reporter API
# (API pagination is inherently sequential/stateful, so this part
# stays as plain Python running on the driver, same as the original script)
# -----------------------------
while True:
    print(f"Offset value: {offset}")

    payload = {
        "criteria": {
            "project_start_date": {
                "from_date": "2014-01-01"
            },
            "org_names": ["University Of California, San Francisco"]
        },
        "include_fields": [
            "ApplId",
            "FiscalYear",
            "ProjectNum",
            "ProjectTitle",
            "AwardAmount",
            "Organization",
            "PrincipalInvestigators"
        ],
        "offset": offset,
        "limit": LIMIT
    }

    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"API failed because :{e}")
        raise

    results = data.get("results", [])
    print(f"Records received:{len(results)}")

    if not results:
        break

    total_records.extend(results)

    # pagination check
    if len(results) < LIMIT:
        break

    offset += LIMIT

    # rest for API
    time.sleep(1)

print(f"total records:{len(total_records)}")

# -----------------------------
# Convert results into a Spark DataFrame
# -----------------------------
if total_records:
    # Each record becomes one JSON line; spark.read.json can parse
    # a list of JSON strings distributed as an RDD
    json_strings = [json.dumps(record) for record in total_records]
    rdd = spark.sparkContext.parallelize(json_strings)
    df = spark.read.json(rdd)

    print(f"DataFrame record count: {df.count()}")
    df.printSchema()
else:
    print("No records retrieved — skipping DataFrame creation and S3 write.")
    df = None

# -----------------------------
# Write DataFrame to S3 as JSON
# -----------------------------
if df is not None:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_path = f"s3a://{s3_bucket}/raw/nih_ucsf_{timestamp}"

    # coalesce(1) keeps a single output file, similar to the original
    # single-object put_object behavior. Drop it if you want parallel
    # part-files instead.
    df.coalesce(1).write.mode("overwrite").json(s3_path)

    print(f"Successfully saved to {s3_path}")

spark.stop()