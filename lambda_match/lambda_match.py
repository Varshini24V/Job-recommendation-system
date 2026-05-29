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

TRACKED_SKILLS = ["python", "sql", "aws", "snowflake", "spark", "airflow", "docker", "kubernetes",
                   "linux", "pandas", "numpy", "tensorflow", "machine learning", "deep learning",
                   "data engineering", "etl", "api", "cloudformation", "ec2", "s3", "vpn", "sd wan",
                     "cryptography", "network security", "distributed systems", "datadog", "debugging",
                      "c programming", "networking protocols", "platform engineering", "reliability engineering"]

# FILTER RELEVANT JOBS

def filter_jobs(all_jobs, resume_skills):
    filtered = []
    fallback_jobs = []
    for job in all_jobs:
        title = job.get("title","").lower()
        description = job.get( "description","").lower()
        combined_text = ( f"{title} {description}")

        # ROLE FILTER
        role_match = any(role in combined_text for role in TARGET_ROLES)

        if not role_match:
            continue
        fallback_jobs.append(job)

        # SKILL FILTER
        job_skills = extract_skills(combined_text)
        overlap = (resume_skills.intersection(job_skills))

        # Require at least one skill overlap

        if len(overlap) >= 1:
            filtered.append(job)

    # FALLBACK

    if not filtered:
        print("Using fallback jobs")
        return fallback_jobs
    return filtered

# EXTRACT MISSING SKILLS
def extract_missing_skills(resume_skills, required_skills,reasoning):

    missing_skills = list(
        set(required_skills).difference(
            set(resume_skills)))

    # FALLBACK FROM AI REASONING
    reasoning_lower = reasoning.lower()
    for skill in TRACKED_SKILLS:
        if (
            skill in reasoning_lower
            and
            skill not in resume_skills
            and
            skill not in missing_skills
        ):
            missing_skills.append(skill)

    return list(set(missing_skills))

# BUILD OUTPUT
def build_output(job,score_data,resume_skills,required_skills,reasoning):

    # MISSING SKILLS
    missing_skills = extract_missing_skills(resume_skills,required_skills,reasoning)

    # COURSE RECOMMENDATIONS
    try:
        if missing_skills:
            recommended_courses = recommend(missing_skills)
        else:
            recommended_courses = []

    except Exception as course_error:
        print("Course Recommendation Error:",str(course_error))
        recommended_courses = []

    # FINAL OUTPUT
    return {"title":job.get('title', 'N/A'),
            "company":job.get('company', 'N/A'),
            "final_score":round(score_data["final_score"] * 100,2),
            "semantic_similarity":round(score_data["semantic_similarity"] * 100,2),
            "skill_overlap":round(score_data["skill_overlap"] * 100,2),
            "title_score":round(score_data["title_score"] * 100,2), 
            "recency_weight": round(score_data["recency_weight"] * 100,2),
            "popularity_score":round(score_data["popularity_score"] * 100,2),
            "skills":list(required_skills),
            "missing_skills":missing_skills,
            "reasoning":reasoning,
            "courses": recommended_courses,
            "apply_link": job.get("redirect_url","#")}

# MAIN LAMBDA
def lambda_handler(event, context):
    try:
        print("MATCHING LAMBDA STARTED")
        # REQUEST BODY
        body = json.loads(event.get("body","{}"))
        resume_id = body.get("resume_id")

        if not resume_id:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "resume_id is required"
                })}

        # FETCH RESUME
        resume = resumes.find_one({"resume_id": resume_id})
        if not resume:
            return {
                "statusCode": 404,
                "body": json.dumps({
                    "error":"Resume not found"
                })
            }
        print("Resume Found")

        # RESUME DATA
        resume_summary = " ".join([
            resume.get("summary",""),
            resume.get("raw_text", ""),
            " ".join(
                resume.get(
                    "skills",
                    []
                ))])

        resume_embedding = resume.get("embedding",[])
        resume_skills = extract_skills(resume_summary)
        print("Resume Skills:",resume_skills)

        # FETCH JOBS
        all_jobs = list(
            jobs.find({"embedding": {"$exists": True}})
        )

        print("Total Jobs:",len(all_jobs))

        # FILTER JOBS
        filtered_jobs = filter_jobs(all_jobs,resume_skills)

        print("Filtered Jobs:",len(filtered_jobs))

        # SCORE JOBS
        scored_jobs = []
        MIN_SCORE_THRESHOLD = 0.35
        for idx, job in enumerate(filtered_jobs):
            try:
                print(f"Scoring Job {idx+1}")
                job_skills = extract_skills(job.get("description",""))
                score_data = final_hybrid_score(
                    resume_embedding,job.get("embedding",[]),
                    resume_skills,
                    job_skills,
                    job.get("title",""),
                    job.get("posted_date","2026-01-01"),
                    job.get("applicant_count",10))

                if (score_data["final_score"]>= MIN_SCORE_THRESHOLD):

                    scored_jobs.append({
                        "job":job,
                        "score":score_data["final_score"],
                        "score_data":score_data,
                        "skills":job_skills
                    })

            except Exception as scoring_error:

                print("Scoring Error:",str(scoring_error))

        # FALLBACK IF EMPTY
        if not scored_jobs:

            print("Using fallback scoring" )

            for job in filtered_jobs[:20]:
                try:
                    job_skills = extract_skills(job.get("description",""))
                    score_data = final_hybrid_score(resume_embedding,job.get("embedding",[]),
                        resume_skills,
                        job_skills,
                        job.get("title",""),
                        job.get("posted_date","2026-01-01"),
                        job.get("applicant_count",10))
                    scored_jobs.append({

                        "job":job,
                        "score":score_data["final_score"],
                        "score_data":score_data,
                        "skills":job_skills
                    })

                except Exception as fallback_error:

                    print("Fallback Error:",str(fallback_error))

        # SORT JOBS
        top_jobs = sorted(scored_jobs,
            key=lambda x: (
                x["score_data"]["semantic_similarity"],
                x["score_data"]["skill_overlap"],
                x["score"]
            ),
            reverse=True)[:10]

        print("Top Jobs:", len(top_jobs))

        # FINAL OUTPUT
        output = []
        for idx, item in enumerate(top_jobs):
            job = item["job"]
            score_data = item["score_data"]
            required_skills = item["skills"]
            print(f"Analyzing Job {idx+1}")

            # AI REASONING
            try:
                reasoning = analyze(resume_summary[:1500],
                                    job.get("description","")[:1500])

            except Exception as ai_error:
                print("AI Error:", str(ai_error))
                reasoning = ("AI reasoning unavailable")

            # BUILD OUTPUT
            output.append(build_output(job,score_data,resume_skills,required_skills,reasoning))
        
        # SAVE MATCHES
        matches.update_one({"resume_id":resume_id},
                           {"$set": {"matches":output,
                                     "updated_at":datetime.utcnow()}},upsert=True)

        print("Matches Saved Successfully")

        # SUCCESS RESPONSE
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type":
                "application/json",
                "Access-Control-Allow-Origin":
                "*"
            },

            "body": json.dumps({
                "resume_id":resume_id,
                "matches": output})
        }
    except Exception as e:
        print("FATAL ERROR:", str(e))

        return {
            "statusCode": 500,
            "headers": {
                "Content-Type":
                "application/json",
                "Access-Control-Allow-Origin":
                "*"
            },
            "body": json.dumps({
                "error":str(e)
            })
        }
