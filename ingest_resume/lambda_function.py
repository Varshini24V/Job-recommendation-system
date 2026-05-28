import json
import os
import traceback
from datetime import datetime

import boto3
from botocore.config import Config

from db import resumes
from extract import extract_text

from bedrock_utils import (
    titan_embedding,
    summarize_resume
)

# =====================================================
# AWS CONFIG
# =====================================================

AWS_REGION = os.getenv(
    "AWS_REGION",
    "ap-south-1"
)

S3_BUCKET = os.getenv(
    "S3_BUCKET"
)

# =====================================================
# BOTO CONFIG
# =====================================================

boto_config = Config(
    retries={
        "max_attempts": 5,
        "mode": "standard"
    },
    connect_timeout=10,
    read_timeout=300
)

# =====================================================
# S3 CLIENT
# =====================================================

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    config=boto_config
)

# =====================================================
# RESPONSE HELPERS
# =====================================================

def success_response(
    status_code,
    body
):

    return {

        "statusCode": status_code,

        "headers": {

            "Content-Type":
            "application/json",

            "Access-Control-Allow-Origin":
            "*",

            "Access-Control-Allow-Headers":
            "*",

            "Access-Control-Allow-Methods":
            "*"
        },

        "body": json.dumps(body)
    }


def error_response(
    status_code,
    message
):

    return success_response(
        status_code,
        {
            "error": message
        }
    )

# =====================================================
# VALIDATE TEXT
# =====================================================

def validate_resume_text(text):

    if not text:
        return False

    if not text.strip():
        return False

    if len(text.strip()) < 30:
        return False

    return True

# =====================================================
# MAIN LAMBDA
# =====================================================

def lambda_handler(event, context):

    try:

        print("=" * 50)
        print("Resume Upload Lambda Started")
        print("=" * 50)

        print(
            "Incoming Event:"
        )

        print(
            json.dumps(event)
        )

        # =================================================
        # PARSE REQUEST
        # =================================================

        body = json.loads(
            event.get(
                "body",
                "{}"
            )
        )

        resume_id = body.get(
            "resume_id"
        )

        if not resume_id:

            return error_response(
                400,
                "resume_id is required"
            )

        print(
            "Resume ID:",
            resume_id
        )

        # =================================================
        # DOWNLOAD PDF FROM S3
        # =================================================

        print(
            "Downloading PDF from S3..."
        )

        response = s3.get_object(
            Bucket=S3_BUCKET,
            Key=resume_id
        )

        pdf_bytes = response[
            "Body"
        ].read()

        print(
            "PDF downloaded successfully"
        )

        print(
            "PDF size:",
            len(pdf_bytes)
        )

        # =================================================
        # EXTRACT TEXT
        # =================================================

        print(
            "Extracting resume text..."
        )

        text = extract_text(
            pdf_bytes
        )

        print(
            "Extracted text length:",
            len(text)
        )

        print(
            "Preview:"
        )

        print(
            text[:500]
        )

        # =================================================
        # VALIDATE TEXT
        # =================================================

        if not validate_resume_text(text):

            return error_response(

                400,

                (
                    "Resume text extraction failed. "
                    "PDF may be empty, scanned, "
                    "corrupted, or unsupported."
                )
            )

        # =================================================
        # GENERATE EMBEDDING
        # =================================================

        print(
            "Generating embedding..."
        )

        embedding = titan_embedding(
            text[:8000]
        )

        if not embedding:

            return error_response(
                500,
                "Embedding generation failed"
            )

        print(
            "Embedding generated"
        )

        print(
            "Embedding size:",
            len(embedding)
        )

        # =================================================
        # GENERATE SUMMARY
        # =================================================

        print(
            "Generating AI summary..."
        )

        summary = summarize_resume(
            text[:4000]
        )

        if not summary:

            summary = (
                "Summary generation failed"
            )

        print(
            "Summary generated"
        )

        # =================================================
        # STORE IN MONGODB
        # =================================================

        document = {

            "resume_id":
            resume_id,

            "resume_text":
            text[:20000],

            "summary":
            summary,

            "embedding":
            embedding,

            "metadata": {

                "file_type":
                "pdf",

                "region":
                AWS_REGION,

                "uploaded_at":
                datetime.utcnow().isoformat(),

                "request_id":
                context.aws_request_id,

                "text_length":
                len(text)
            }
        }

        resumes.update_one(

            {
                "resume_id":
                resume_id
            },

            {
                "$set":
                document
            },

            upsert=True
        )

        print(
            "Resume saved to MongoDB"
        )

        # =================================================
        # SUCCESS RESPONSE
        # =================================================

        return success_response(

            200,

            {

                "message":
                "Resume processed successfully",

                "resume_id":
                resume_id,

                "summary_preview":
                summary[:300]
            }
        )

    except Exception as e:

        print(
            "FATAL ERROR:"
        )

        print(str(e))

        traceback.print_exc()

        return error_response(

            500,

            (
                "Internal server error: "
                f"{str(e)}"
            )
        )