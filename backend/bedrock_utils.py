import boto3
import json
import os
import traceback

# =====================================================
# BEDROCK CLIENT
# =====================================================

bedrock = boto3.client(
    "bedrock-runtime",
    region_name=os.getenv(
        "AWS_REGION",
        "ap-south-1"
    )
)

# =====================================================
# TITAN EMBEDDING
# =====================================================

def titan_embedding(text):

    try:

        if not text or not text.strip():

            raise ValueError(
                "Empty text provided for embedding"
            )

        response = bedrock.invoke_model(

            modelId=
            "amazon.titan-embed-text-v2:0",

            body=json.dumps({
                "inputText":
                text[:8000]
            }),

            contentType=
            "application/json",

            accept=
            "application/json"
        )

        result = json.loads(
            response["body"].read()
        )

        return result.get(
            "embedding",
            []
        )

    except Exception as e:

        print(
            "Titan embedding error:",
            str(e)
        )

        traceback.print_exc()

        return []

# =====================================================
# NOVA LITE REASONING
# =====================================================

def nova_reason(prompt):

    try:

        if not prompt or not prompt.strip():

            return (
                "No content available "
                "for analysis."
            )

        body = {

            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "text": prompt.strip()
                        }
                    ]
                }
            ],

            "inferenceConfig": {
                "max_new_tokens": 1000,
                "temperature": 0.2,
                "top_p": 0.9
            }
        }

        response = bedrock.invoke_model(

            modelId=
            "global.amazon.nova-2-lite-v1:0",

            body=json.dumps(body),

            contentType=
            "application/json",

            accept=
            "application/json"
        )

        result = json.loads(
            response["body"].read()
        )

        return result[
            "output"
        ]["message"]["content"][0]["text"]

    except Exception as e:

        print(
            "Nova Lite error:",
            str(e)
        )

        traceback.print_exc()

        return (
            "AI analysis failed."
        )

# =====================================================
# RESUME SUMMARY
# =====================================================

def summarize_resume(text):

    prompt = f"""
    Analyze this resume and extract:

    1. Technical Skills
    2. Education
    3. Work Experience
    4. Projects
    5. Certifications
    6. Career Strengths
    7. Suggested Job Roles

    Return clean structured output.

    Resume:

    {text[:4000]}
    """

    return nova_reason(prompt)

# =====================================================
# JOB MATCH ANALYSIS
# =====================================================

def analyze_job_match(
    resume_summary,
    job_description
):

    if not resume_summary:
        resume_summary = (
            "No resume summary available."
        )

    if not job_description:
        job_description = (
            "No job description available."
        )

    prompt = f"""
    Compare candidate profile with job.

    Return:

    1. Match reasoning
    2. Missing skills
    3. Skill improvement suggestions
    4. Suitability score out of 10

    Candidate:
    {resume_summary}

    Job:
    {job_description[:3000]}
    """

    return nova_reason(prompt)