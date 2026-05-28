import os
import json
import requests
from db import jobs
from bedrock_utils import titan_embedding

# =====================================================
# ENV VARIABLES
# =====================================================

APP_ID = os.getenv("ADZUNA_APP_ID")

APP_KEY = os.getenv("ADZUNA_APP_KEY")

# =====================================================
# FETCH JOBS FROM ADZUNA
# =====================================================

def fetch_jobs():

    url = (
        f"https://api.adzuna.com/v1/api/jobs/in/search/1"
        f"?app_id={APP_ID}"
        f"&app_key={APP_KEY}"
        f"&results_per_page=50"
        f"&what=software developer"
        f"&content-type=application/json"
    )

    response = requests.get(
        url,
        timeout=60
    )

    print("API Status:", response.status_code)

    if response.status_code != 200:

        print("API ERROR:")
        print(response.text)

        return []

    data = response.json()

    return data.get(
        "results",
        []
    )

# =====================================================
# MAIN LAMBDA
# =====================================================

def lambda_handler(event, context):

    try:

        print("=" * 50)
        print("JOB REFRESH STARTED")
        print("=" * 50)

        # ---------------------------------------------
        # Validate Credentials
        # ---------------------------------------------

        if not APP_ID or not APP_KEY:

            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error":
                    "Missing Adzuna credentials"
                })
            }

        # ---------------------------------------------
        # Fetch Jobs
        # ---------------------------------------------

        jobs_data = fetch_jobs()

        print(
            "Total jobs received:",
            len(jobs_data)
        )

        inserted = 0

        # ---------------------------------------------
        # Insert Jobs
        # ---------------------------------------------

        for job in jobs_data:

            try:

                description = job.get(
                    "description",
                    ""
                )

                embedding = titan_embedding(
                    description[:8000]
                )

                document = {

                    "job_id":
                    str(job.get("id")),

                    "title":
                    job.get("title"),

                    "company":
                    job.get(
                        "company",
                        {}
                    ).get(
                        "display_name",
                        "Unknown"
                    ),

                    "description":
                    description,

                    "redirect_url":
                    job.get(
                        "redirect_url"
                    ),

                    "location":
                    job.get(
                        "location",
                        {}
                    ).get(
                        "display_name",
                        ""
                    ),

                    "created":
                    job.get("created"),

                    "embedding":
                    embedding
                }

                jobs.update_one(

                    {
                        "job_id":
                        document["job_id"]
                    },

                    {
                        "$set":
                        document
                    },

                    upsert=True
                )

                inserted += 1

            except Exception as job_error:

                print(
                    "Job Insert Error:",
                    str(job_error)
                )

        print(
            f"Inserted/Updated {inserted} jobs"
        )

        return {

            "statusCode": 200,

            "headers": {
                "Content-Type":
                "application/json",

                "Access-Control-Allow-Origin":
                "*"
            },

            "body": json.dumps({

                "message":
                "Jobs refreshed successfully",

                "jobs_inserted":
                inserted
            })
        }

    except Exception as e:

        print(
            "FATAL ERROR:",
            str(e)
        )

        return {

            "statusCode": 500,

            "headers": {
                "Content-Type":
                "application/json",

                "Access-Control-Allow-Origin":
                "*"
            },

            "body": json.dumps({
                "error": str(e)
            })
        }