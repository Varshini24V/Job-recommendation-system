from db import resumes, jobs, matches

from ranking import (
    extract_skills,
    final_hybrid_score,
    TARGET_ROLES
)

from rag_engine import analyze
from course_recommender import recommend

import json

from datetime import datetime


# =========================================================
# FILTER RELEVANT JOBS
# =========================================================

def filter_jobs(all_jobs, resume_skills):

    filtered = []

    for job in all_jobs:

        title = job.get(
            "title",
            ""
        ).lower()

        description = job.get(
            "description",
            ""
        ).lower()

        combined_text = (
            f"{title} {description}"
        )

        # ---------------------------------------------
        # ROLE FILTER
        # ---------------------------------------------

        role_match = any(

            role in combined_text

            for role in TARGET_ROLES
        )

        if not role_match:
            continue

        # ---------------------------------------------
        # SKILL FILTER
        # ---------------------------------------------

        job_skills = extract_skills(
            combined_text
        )

        overlap = (
            resume_skills.intersection(
                job_skills
            )
        )

        # Require minimum overlap
        if len(overlap) < 2:
            continue

        filtered.append(job)

    return filtered


# =========================================================
# BUILD MATCH OUTPUT
# =========================================================

def build_output(

    job,
    score_data,
    resume_skills,
    required_skills,
    reasoning,
    recommended_courses
):

    missing_skills = list(

        required_skills.difference(
            resume_skills
        )
    )

    return {

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
        score_data["final_score"],

        "semantic_similarity":
        score_data[
            "semantic_similarity"
        ],

        "skill_overlap":
        score_data[
            "skill_overlap"
        ],

        "title_score":
        score_data[
            "title_score"
        ],

        "recency_weight":
        score_data[
            "recency_weight"
        ],

        "popularity_score":
        score_data[
            "popularity_score"
        ],

        "skills":
        list(required_skills),

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
    }


# =========================================================
# MAIN LAMBDA
# =========================================================

def lambda_handler(event, context):

    try:

        print("=" * 50)
        print("MATCHING LAMBDA STARTED")
        print("=" * 50)

        # =================================================
        # REQUEST BODY
        # =================================================

        body = json.loads(
            event.get("body", "{}")
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
        # FILTER JOBS
        # =================================================

        filtered_jobs = filter_jobs(

            all_jobs,
            resume_skills
        )

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

                job_skills = extract_skills(

                    job.get(
                        "description",
                        ""
                    )
                )

                score_data = final_hybrid_score(

                    resume_embedding,

                    job.get(
                        "embedding",
                        []
                    ),

                    resume_skills,

                    job_skills,

                    job.get(
                        "title",
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

                # -----------------------------------------
                # SCORE THRESHOLD
                # -----------------------------------------

                if (
                    score_data["final_score"]
                    < 0.55
                ):
                    continue

                scored_jobs.append({

                    "job":
                    job,

                    "score":
                    score_data["final_score"],

                    "score_data":
                    score_data,

                    "skills":
                    job_skills
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
            "Top Jobs:",
            len(top_jobs)
        )

        # =================================================
        # BUILD FINAL OUTPUT
        # =================================================

        output = []

        for idx, item in enumerate(top_jobs):

            job = item["job"]

            score_data = item[
                "score_data"
            ]

            required_skills = item[
                "skills"
            ]

            print(
                f"Analyzing Job {idx+1}"
            )

            # ---------------------------------------------
            # AI REASONING
            # ---------------------------------------------

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

            # ---------------------------------------------
            # COURSE RECOMMENDATIONS
            # ---------------------------------------------

            missing_skills = list(

                required_skills.difference(
                    resume_skills
                )
            )

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

            # ---------------------------------------------
            # FINAL OUTPUT
            # ---------------------------------------------

            output.append(

                build_output(

                    job,

                    score_data,

                    resume_skills,

                    required_skills,

                    reasoning,

                    recommended_courses
                )
            )

        # =================================================
        # SAVE MATCHES
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
        # SUCCESS RESPONSE
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