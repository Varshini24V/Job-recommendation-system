import json
import boto3
import os
import certifi
import io
from pymongo import MongoClient
from PyPDF2 import PdfReader

# AWS Clients
textract = boto3.client("textract")
s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

# MongoDB
MONGO_URI = os.environ["MONGO_URI"]

client = MongoClient(
    MONGO_URI,
    tls=True,
    tlsCAFile=certifi.where()
)
db = client["resume_matcher"]
collection = db["resumes"]

# -------------------------------
# BEDROCK EMBEDDING
# -------------------------------
def get_embedding(text):
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": text}),
        contentType="application/json",
        accept="application/json"
    )

    result = json.loads(response["body"].read())
    return result["embedding"]

# -------------------------------
# FALLBACK PDF EXTRACTION
# -------------------------------
def extract_text_pypdf(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    pdf_bytes = obj["Body"].read()

    reader = PdfReader(io.BytesIO(pdf_bytes))

    text = ""
    for page in reader.pages:
        text += page.extract_text() or ""

    return text

# -------------------------------
# MAIN HANDLER
# -------------------------------
def lambda_handler(event, context):
    try:
        print("EVENT:", event)

        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        resume_id = key.split("/")[-1].replace(".pdf", "")
        print("Processing:", key)

        text = ""

        # -------------------------------
        # TEXTRACT
        # -------------------------------
        try:
            response = textract.detect_document_text(
                Document={"S3Object": {"Bucket": bucket, "Name": key}}
            )

            lines = [
                item["Text"]
                for item in response["Blocks"]
                if item["BlockType"] == "LINE"
            ]

            text = " ".join(lines)
            print("Textract success:", len(text))

        except Exception as e:
            print("Textract failed:", str(e))

        # -------------------------------
        # FALLBACK
        # -------------------------------
        if not text.strip():
            print("Using PyPDF fallback...")
            text = extract_text_pypdf(bucket, key)
            print("PyPDF extracted:", len(text))

        if not text.strip():
            raise Exception("No text extracted")

        # -------------------------------
        # BEDROCK EMBEDDING
        # -------------------------------
        text = text[:8000]  # limit
        embedding = get_embedding(text)

        print("Embedding size:", len(embedding))

        # -------------------------------
        # STORE
        # -------------------------------
        collection.insert_one({
            "resume_id": resume_id,
            "text": text,
            "embedding": embedding
        })

        print("Stored in MongoDB:", resume_id)

        return {"statusCode": 200}

    except Exception as e:
        print("ERROR:", str(e))
        return {"statusCode": 500, "body": str(e)}