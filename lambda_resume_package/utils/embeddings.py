import boto3
import json

# ✅ Use same region as Lambda/S3
bedrock = boto3.client(
    "bedrock-runtime",
    region_name="ap-south-1"
)

import boto3
import json

# ✅ Use same region as Lambda/S3
bedrock = boto3.client(
    "bedrock-runtime",
    region_name="ap-south-1"
)

def get_embedding(text):
    try:
        # Safety: limit input size
        text = text[:8000]

        # ✅ Force Titan v2 to output 1536 dimensions to match your DB Index
        # Inside the Lambda that processes the resume upload
        response = bedrock.invoke_model(
            modelId="amazon.titan-embed-text-v2:0",
            body=json.dumps({
                "inputText": text,
                "dimensions": 1024, # ✅ MATCH THIS TO YOUR JOBS DATA
                "normalize": True
            }),
            contentType="application/json",
            accept="application/json"
        )

        result = json.loads(response["body"].read())
        embedding = result.get("embedding", [])

        # ✅ Validate embedding
        if not embedding:
            raise Exception("Empty embedding returned")

        print("Embedding generated:", len(embedding))  # should now be 1024

        return embedding

    except Exception as e:
        print("Embedding error:", str(e))
        return []