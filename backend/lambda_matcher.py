from utils.mongo import resumes_col, jobs_col, matches_col
from utils.scorer import compute_score
import numpy as np

def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

def lambda_handler(event, context):
    resume = resumes_col.find_one(sort=[("created_at", -1)])
    resume_embedding = resume["embedding"]

    jobs = list(jobs_col.find())

    results = []

    for job in jobs:
        sim = cosine_similarity(resume_embedding, job["embedding"])
        
        keyword = 0.7  # placeholder
        recency = 0.8
        popularity = 0.6

        score = compute_score(sim, keyword, recency, popularity)

        results.append({
            "job_title": job["title"],
            "score": score,
            "description": job["description"]
        })

    results = sorted(results, key=lambda x: x["score"], reverse=True)

    matches_col.insert_one({
        "results": results[:20]
    })

    return {"matches": results[:5]}