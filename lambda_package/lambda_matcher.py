import json
import os
import certifi
from datetime import datetime, timezone
from pymongo import MongoClient

# -------------------------------
# ENV
# -------------------------------
MONGO_URI = os.environ["MONGO_URI"]

# TLS + certifi (avoids certificate errors)
client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where()
)

db = client["resume_matcher"]
resumes = db["resumes"]
jobs = db["jobs"]
matches_col = db["matches"]  # ✅ STEP 1: Initialize the matches collection

# -------------------------------
# MAIN HANDLER
# -------------------------------
def lambda_handler(event, context):
    try:
        print("EVENT:", event)

        # -------------------------------
        # PARSE BODY (API Gateway safe)
        # -------------------------------
        body = event.get("body", {})
        if isinstance(body, str):
            body = json.loads(body)

        resume_id = body.get("resume_id")

        if not resume_id:
            return response(400, {"error": "resume_id required"})

        # -------------------------------
        # FETCH RESUME
        # -------------------------------
        if resume_id == "latest":
            resume = resumes.find_one(sort=[("_id", -1)])
        else:
            resume = resumes.find_one({"resume_id": resume_id})

        if not resume:
            return response(404, {"error": "No resume found"})

        # ✅ STEP 2: Extract the exact resume ID (crucial if "latest" was requested)
        actual_resume_id = resume.get("resume_id", str(resume.get("_id")))
        embedding = resume.get("embedding")

        if not embedding:
            return response(500, {"error": "Embedding missing in resume"})

        # -------------------------------
        # VECTOR SEARCH
        # -------------------------------
        results = jobs.aggregate([
            {
                "$vectorSearch": {
                    "index": "default",
                    "path": "embedding",
                    "queryVector": embedding,
                    "numCandidates": 100,
                    "limit": 5
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "company": 1,
                    "score": {"$meta": "vectorSearchScore"}
                }
            }
        ])

        matches = list(results)
        print("Matches found:", len(matches))

        # -------------------------------
        # ✅ STEP 3: UPDATE MATCHES COLLECTION
        # -------------------------------
        if matches:
            matches_col.update_one(
                {"resume_id": actual_resume_id}, # Filter: Find existing record by resume_id
                {
                    "$set": {
                        "matches": matches,
                        "updated_at": datetime.now(timezone.utc).isoformat()
                    }
                },
                upsert=True # Upsert: Update if exists, insert if it doesn't
            )
            print(f"Successfully updated matches collection for resume_id: {actual_resume_id}")

        return response(200, {
            "resume_id": actual_resume_id,
            "matches": matches
        })

    except Exception as e:
        print("ERROR:", str(e))
        return response(500, {"error": str(e)})


# -------------------------------
# RESPONSE HELPER
# -------------------------------
def response(code, body):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }