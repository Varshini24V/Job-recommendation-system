from db import resumes, jobs, matches

from ranking import (
    cosine_similarity,
    keyword_overlap_score,
    recency_weight,
    popularity_score,
    final_hybrid_score
)

from rag_engine import analyze

from course_recommender import recommend

import json
from datetime import datetime

# =========================================================
# IMPORTANT SKILLS
# =========================================================

IMPORTANT_SKILLS = [

    "python",
    "sql",
    "aws",
    "machine learning",
    "deep learning",
    "ai",
    "artificial intelligence",
    "backend",
    "data engineering",
    "data analysis",
    "mongodb",
    "postgresql",
    "mysql",
    "docker",
    "kubernetes",
    "spark",
    "airflow",
    "linux",
    "api",
    "tensorflow",
    "pandas",
    "numpy",
    "cloud",
    "etl"
]

# =========================================================
# EXTRACT SKILLS
# =========================================================

def extract_skills(text):

    if not text:
        return []

    text = text.lower()

    found_skills = []

    for skill in IMPORTANT_SKILLS:

        if skill in text:

            found_skills.append(skill)

    return list(set(found_skills))


# =========================================================
# DOMAIN FILTER
# =========================================================

TARGET_DOMAINS = [

    "software",
    "cloud",
    "backend",
    "data",
    "machine learning",
    "ai",
    "developer",
    "engineer",
    "python",
    "aws"
]


def is_relevant_job(job_description):

    if not job_description:
        return False

    job_description = job_description.lower()

    for keyword in TARGET_DOMAINS:

        if keyword in job_description:
            return True

    return False


# =========================================================
# MAIN LAMBDA
# =========================================================

def lambda_handler(event, context):

    try:

        print("===================================")
        print("MATCHING LAMBDA STARTED")
        print("===================================")

        body = json.loads(
            event["body"]
        )

        resume_id = body.get(
            "resume_id"
        )

        if not resume_id:

            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error":
                    "resume_id is required"
                })
            }

        # =================================================
        # FETCH RESUME
        # =================================================

        resume = resumes.find_one({

            "resume_id":
            resume_id

        })

        if not resume:

            return {
                "statusCode": 404,
                "body": json.dumps({
                    "error":
                    "Resume not found"
                })
            }

        print("Resume Found")

        # =================================================
        # RESUME DATA
        # =================================================

        resume_summary = resume.get(
            "summary",
            ""
        )

        resume_embedding = resume.get(
            "embedding",
            []
        )

        resume_skills = extract_skills(
            resume_summary
        )

        print(
            "Resume Skills:",
            resume_skills
        )

        # =================================================
        # FETCH JOBS
        # =================================================

        all_jobs = list(

            jobs.find({
                "embedding": {
                    "$exists": True
                }
            })

        )

        print(
            "Total Jobs:",
            len(all_jobs)
        )

        # =================================================
        # FILTER RELEVANT JOBS
        # =================================================

        filtered_jobs = []

        for job in all_jobs:

            description = job.get(
                "description",
                ""
            )

            if not is_relevant_job(
                description
            ):
                continue

            description_lower = (
                description.lower()
            )

            matched = False

            for skill in resume_skills:

                if skill in description_lower:

                    matched = True
                    break

            if matched:

                filtered_jobs.append(job)

        print(
            "Filtered Jobs:",
            len(filtered_jobs)
        )

        # =================================================
        # SCORE JOBS
        # =================================================

        scored_jobs = []

        for idx, job in enumerate(filtered_jobs):

            try:

                print(
                    f"Scoring Job {idx+1}"
                )

                score_data = final_hybrid_score(

                    resume_embedding,

                    job.get(
                        "embedding",
                        []
                    ),

                    resume_summary,

                    job.get(
                        "description",
                        ""
                    ),

                    job.get(
                        "posted_date",
                        "2026-01-01"
                    ),

                    job.get(
                        "applicant_count",
                        10
                    )
                )

                required_skills = extract_skills(

                    job.get(
                        "description",
                        ""
                    )
                )

                scored_jobs.append({

                    "job":
                    job,

                    "score":
                    score_data["final_score"],

                    "score_breakdown":
                    score_data,

                    "skills":
                    required_skills
                })

            except Exception as scoring_error:

                print(
                    "Scoring Error:",
                    str(scoring_error)
                )

        # =================================================
        # SORT TOP JOBS
        # =================================================
        top_jobs = sorted(

            scored_jobs,

            key=lambda x: x["score"],

            reverse=True

        )[:5]

        print(
            "Top Jobs Selected:",
            len(top_jobs)
        )

        # =================================================
        # GENERATE OUTPUT
        # =================================================

        output = []

        for idx, item in enumerate(top_jobs):

            job = item["job"]

            score = item["score"]

            skills = item["skills"]

            score_breakdown = item[
                "score_breakdown"
            ]

            print(
                f"Analyzing Job {idx+1}"
            )

            # =============================================
            # AI REASONING
            # =============================================

            try:

                reasoning = analyze(

                resume_summary[:1500],

                job.get(
                    "description",
                    ""
                )[:1500]
            )

            except Exception as ai_error:

                print(
                    "AI Error:",
                    str(ai_error)
                )

                reasoning = (
                    "AI reasoning unavailable"
                )

            # =============================================
            # MISSING SKILLS
            # =============================================

            missing_skills = []

            for skill in skills:

                if skill not in resume_skills:

                    missing_skills.append(skill)

            # =============================================
            # COURSE RECOMMENDATION
            # =============================================

            try:

                recommended_courses = recommend(
                    missing_skills
                )

            except Exception as course_error:

                print(
                    "Course Error:",
                    str(course_error)
                )

                recommended_courses = []

            # =============================================
            # FINAL OUTPUT
            # =============================================

            output.append({

                "title":
                job.get(
                    "title",
                    "N/A"
                ),

                "company":
                job.get(
                    "company",
                    "N/A"
                ),

                "final_score":
                score,

                "semantic_similarity":
                score_breakdown[
                    "semantic_similarity"
                ],

                "keyword_overlap":
                score_breakdown[
                    "keyword_overlap"
                ],

                "recency_weight":
                score_breakdown[
                    "recency_weight"
                ],

                "popularity_score":
                score_breakdown[
                    "popularity_score"
                ],

                "skills":
                skills,

                "missing_skills":
                missing_skills,

                "reasoning":
                reasoning,

                "courses":
                recommended_courses,

                "apply_link":
                job.get(
                    "redirect_url",
                    "#"
                )
            })

        # =================================================
        # SAVE TO MONGODB
        # =================================================

        matches.update_one(

            {
                "resume_id":
                resume_id
            },

            {
                "$set": {

                    "matches":
                    output,

                    "updated_at":
                    datetime.utcnow()
                }
            },

            upsert=True
        )

        print(
            "Matches Saved Successfully"
        )

        # =================================================
        # RESPONSE
        # =================================================

        return {

            "statusCode": 200,

            "headers": {

                "Content-Type":
                "application/json",

                "Access-Control-Allow-Origin":
                "*"
            },

            "body": json.dumps({

                "resume_id":
                resume_id,

                "matches":
                output
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

                "error":
                str(e)
            })
        }