from datetime import datetime, timezone
import math
import re

# =====================================================
# IMPORTANT SKILLS
# =====================================================

IMPORTANT_SKILLS = [

    "python",
    "sql",
    "aws",
    "snowflake",
    "spark",
    "airflow",
    "docker",
    "kubernetes",
    "linux",
    "pandas",
    "numpy",
    "tensorflow",
    "machine learning",
    "deep learning",
    "data engineering",
    "etl",
    "api",
    "cloudformation",
    "ec2",
    "s3"
]

# =====================================================
# TARGET ROLES
# =====================================================

TARGET_ROLES = [

    "data engineer",
    "python developer",
    "backend developer",
    "aws engineer",
    "cloud engineer",
    "software engineer",
    "ml engineer",
    "ai engineer",
    "devops engineer"
]

# =====================================================
# CLEAN TEXT
# =====================================================

def clean_text(text):

    if not text:
        return ""

    text = text.lower()

    text = re.sub(r"[^a-zA-Z0-9 ]", " ", text)

    return text

# =====================================================
# EXTRACT SKILLS
# =====================================================

def extract_skills(text):

    text = clean_text(text)

    found = set()

    for skill in IMPORTANT_SKILLS:

        if skill.lower() in text:
            found.add(skill)

    return found

# =====================================================
# COSINE SIMILARITY
# =====================================================

def cosine_similarity(vec1, vec2):

    if not vec1 or not vec2:
        return 0.0

    dot_product = sum(
        a * b for a, b in zip(vec1, vec2)
    )

    norm1 = math.sqrt(
        sum(a * a for a in vec1)
    )

    norm2 = math.sqrt(
        sum(b * b for b in vec2)
    )

    if norm1 == 0 or norm2 == 0:
        return 0.0

    similarity = (
        dot_product / (norm1 * norm2)
    )

    # Normalize to 0 → 1
    normalized = (
        similarity + 1
    ) / 2

    return normalized

# =====================================================
# SKILL OVERLAP SCORE
# =====================================================

def skill_overlap_score(
    resume_skills,
    job_skills
):

    if not resume_skills:
        return 0.0

    matched = (
        resume_skills.intersection(job_skills)
    )

    return len(matched) / len(resume_skills)

# =====================================================
# TITLE RELEVANCE
# =====================================================

def title_relevance_score(job_title):

    title = clean_text(job_title)

    for role in TARGET_ROLES:

        if role in title:
            return 1.0

    return 0.2

# =====================================================
# RECENCY SCORE
# =====================================================

def recency_weight(posted_date):

    try:

        posted = datetime.strptime(
            posted_date,
            "%Y-%m-%d"
        ).replace(
            tzinfo=timezone.utc
        )

        now = datetime.now(
            timezone.utc
        )

        days_old = (
            now - posted
        ).days

        if days_old <= 3:
            return 1.0

        elif days_old <= 7:
            return 0.9

        elif days_old <= 14:
            return 0.7

        elif days_old <= 30:
            return 0.5

        return 0.2

    except:
        return 0.3

# =====================================================
# POPULARITY SCORE
# =====================================================

def popularity_score(applicant_count):

    if applicant_count <= 20:
        return 1.0

    elif applicant_count <= 50:
        return 0.8

    elif applicant_count <= 100:
        return 0.6

    return 0.3

# =====================================================
# FINAL HYBRID SCORE
# =====================================================

def final_hybrid_score(

    resume_embedding,
    job_embedding,

    resume_skills,
    job_skills,

    job_title,

    posted_date,

    applicant_count
):

    # ----------------------------------------------
    # Semantic Similarity
    # ----------------------------------------------

    semantic_score = cosine_similarity(
        resume_embedding,
        job_embedding
    )

    # ----------------------------------------------
    # Skill Overlap
    # ----------------------------------------------

    skill_score = skill_overlap_score(
        resume_skills,
        job_skills
    )

    # ----------------------------------------------
    # Title Score
    # ----------------------------------------------

    title_score = title_relevance_score(
        job_title
    )

    # ----------------------------------------------
    # Recency
    # ----------------------------------------------

    recency_score = recency_weight(
        posted_date
    )

    # ----------------------------------------------
    # Popularity
    # ----------------------------------------------

    popularity = popularity_score(
        applicant_count
    )

    # ----------------------------------------------
    # FINAL SCORE
    # ----------------------------------------------

    final_score = (

        0.55 * semantic_score
        + 0.25 * skill_score
        + 0.10 * title_score
        + 0.05 * recency_score
        + 0.05 * popularity
    )

    return {

        "semantic_similarity":
        round(semantic_score, 4),

        "skill_overlap":
        round(skill_score, 4),

        "title_score":
        round(title_score, 4),

        "recency_weight":
        round(recency_score, 4),

        "popularity_score":
        round(popularity, 4),

        "final_score":
        round(final_score, 4)
    }