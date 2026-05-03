import os
import json
import boto3
import certifi
from pymongo import MongoClient

# -------------------------------
# CONFIG
# -------------------------------
MONGO_URI = "mongodb+srv://varsh:Varsh2001@job-matcher.thortdu.mongodb.net/job-matcher?retryWrites=true&w=majority"

# ✅ Use SAME region where model works
BEDROCK_REGION = "us-east-1"

MODEL_ID = "amazon.titan-embed-text-v2:0"

# -------------------------------
# MONGODB CONNECTION (FIXED)
# -------------------------------
client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where()
)

db = client["resume_matcher"]
jobs = db["jobs"]

# -------------------------------
# BEDROCK CLIENT
# -------------------------------
bedrock = boto3.client(
    "bedrock-runtime",
    region_name=BEDROCK_REGION
)

# -------------------------------
# EMBEDDING FUNCTION
# -------------------------------
def get_embedding(text):
    try:
        response = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps({"inputText": text}),
            contentType="application/json",
            accept="application/json"
        )

        result = json.loads(response["body"].read())
        return result.get("embedding", [])

    except Exception as e:
        print("❌ Embedding error:", str(e))
        return []

# -------------------------------
# MAIN SCRIPT
# -------------------------------
def main():
    print("🚀 Starting job embedding...")

    count = 0
    skipped = 0
    failed = 0

    for job in jobs.find():

        # Skip if already embedded
        if "embedding" in job:
            skipped += 1
            continue

        text = f"{job.get('title', '')} {job.get('description', '')}"

        if not text.strip():
            print(f"⚠️ Empty text for job: {job['_id']}")
            failed += 1
            continue

        embedding = get_embedding(text[:8000])

        if not embedding:
            print(f"❌ Failed embedding for: {job['_id']}")
            failed += 1
            continue

        jobs.update_one(
            {"_id": job["_id"]},
            {"$set": {"embedding": embedding}}
        )

        count += 1
        print(f"✅ Embedded: {job['_id']}")

    print("\n🎯 DONE")
    print(f"✅ Success: {count}")
    print(f"⏭️ Skipped: {skipped}")
    print(f"❌ Failed: {failed}")

print("DB:", db.name)
print("Collection:", jobs.name)
print("Sample job:", jobs.find_one())
# -------------------------------
# RUN
# -------------------------------
if __name__ == "__main__":
    main()