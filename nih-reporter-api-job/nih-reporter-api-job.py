import requests
import boto3
import json
from datetime import datetime

# configuration

API_URL = "https://api.reporter.nih.gov/v2/projects/search"

BUCKET_NAME = "nih-reporter-starter-project"

# NIH API request body

payload = {
    "criteria": {
        "fiscal_years": [2025]
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
    "offset": 0,
    "limit": 10
}

# Call NIH API

print("Calling NIH RePORTER API...")

response = requests.post(API_URL, json=payload, timeout = 60)
response.raise_for_status()

# Convert response to JSON

data=response.json()
print("API call successful")
results=data.get("results", [])

print(f"Number of records received: {len(results)}")

# Create S3 filename

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

s3_key = f"raw/nih_projects_{timestamp}.json"

# Upload data to S3

s3 = boto3.client("s3")

s3.put_object(
    Bucket=BUCKET_NAME,
    Key=s3_key,
    Body=json.dumps(results, indent=2),
    ContentType="application/json"
)


print(
    f"File successfully written to "
    f"s3://{BUCKET_NAME}/{s3_key}"
)