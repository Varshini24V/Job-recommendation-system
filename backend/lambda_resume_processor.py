import boto3
import json
from datetime import datetime
from utils.mongo import resumes_col
from utils.embeddings import get_embedding

textract = boto3.client('textract', region_name="ap-south-1")

def extract_text(bucket, key):
    response = textract.detect_document_text(
        Document={'S3Object': {'Bucket': bucket, 'Name': key}}
    )

    lines = [
        item['Text'] for item in response['Blocks']
        if item['BlockType'] == 'LINE'
    ]
    return " ".join(lines)

def summarize(text):
    return text[:500]   # fallback (safe)

def lambda_handler(event, context):
    try:
        print("EVENT:", event)

        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print("Bucket:", bucket)
        print("Key:", key)

        text = extract_text(bucket, key)
        print("Extracted text length:", len(text))

        embedding = get_embedding(text)
        print("Embedding length:", len(embedding))

        resumes_col.insert_one({
            "s3_key": key,
            "text": text,
            "embedding": embedding,
            "summary": summarize(text),
            "created_at": datetime.utcnow()
        })

        print("Inserted into MongoDB")

        return {"status": "processed"}

    except Exception as e:
        print("ERROR:", str(e))
        return {"status": "error", "message": str(e)}