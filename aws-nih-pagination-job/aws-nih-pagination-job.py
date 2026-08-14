import boto3
import requests
from pyspark.sql import SparkSession
import time
import json
from datetime import datetime

# initialize variables

url = "https://api.reporter.nih.gov/v2/projects/search"
s3_bucket = "nih-reporter-starter-project"

LIMIT = 500
offset = 0
total_records = []

while True:
    print(f"Offset value: {offset}")
    
    payload = {
        "criteria": {
            "fiscal_years": [2023,2024,2025],
            "org_names": ["Johns Hopkins University"]
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
    
    response = requests.post(url,json=payload,timeout=60)
    response.raise_for_status()
    data = response.json()
    
    results = data.get("results",[])
    
    print(f"Records received:{len(results)}")
    
    if not results:
      break

    total_records.extend(results)
    
    # if condition for pagination
    
    if len(results)<LIMIT:
        break
    
    offset+=LIMIT
    
    # Rest for api
    
    time.sleep(1)
    
print(f"total records:{len(total_records)}")

#Load data into s3_bucket
#strftime() function converts a date and time object into a formatted text string
s3 = boto3.client("s3")

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
s3_Key = f"raw/nih_project_{timestamp}.json"

s3.put_object(Bucket=s3_bucket,Key=s3_Key,Body=json.dumps(total_records),ContentType="application/json")

print(
    f"Successfully saved to "
    f"s3://{s3_bucket}/{s3_Key}"
)
    