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
            "project_start_date": {
                "from_date":"2014-01-01"
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
        
        response = requests.post(url,json=payload,timeout=60)
        response.raise_for_status()
        data = response.json()
        
    except requests.exceptions.RequestException as e:
        
        print(f"API failed because :{e}")
        raise
        
    
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
s3_Key = f"raw/nih_ucsf_{timestamp}.json"

json_lines = "\n".join(json.dumps(record)for record in total_records)

s3.put_object(Bucket=s3_bucket,Key=s3_Key,Body=json.dumps(total_records),ContentType="application/json")

print(
    f"Successfully saved to "
    f"s3://{s3_bucket}/{s3_Key}"
)
    