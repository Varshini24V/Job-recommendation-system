from datetime import datetime, timezone
import math

# =====================================================
# COSINE SIMILARITY
# =====================================================

def cosine_similarity(vec1, vec2):

    if not vec1 or not vec2:
        return 0.0

    dot_product = sum(
        a * b
        for a, b in zip(vec1, vec2)
    )

    norm1 = math.sqrt(
        sum(a * a for a in vec1)
    )

    norm2 = math.sqrt(
        sum(b * b for b in vec2)
    )

    denominator = norm1 * norm2

    if denominator == 0:
        return 0.0

    return dot_product / denominator


# =====================================================
# KEYWORD OVERLAP
# =====================================================

def keyword_overlap_score(
    resume_text,
    job_text
):

    resume_words = set(
        resume_text.lower().split()
    )

    job_words = set(
        job_text.lower().split()
    )

    if not job_words:
        return 0.0

    overlap = resume_words.intersection(
        job_words
    )

    return len(overlap) / len(job_words)


# =====================================================
# RECENCY WEIGHT
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
            return 0.8

        elif days_old <= 14:
            return 0.6

        elif days_old <= 30:
            return 0.4

        return 0.2

    except:
        return 0.3


# =====================================================
# POPULARITY SCORE
# =====================================================

def popularity_score(applicant_count):

    if applicant_count <= 10:
        return 0.3

    elif applicant_count <= 50:
        return 0.5

    elif applicant_count <= 100:
        return 0.7

    return 1.0


# =====================================================
# FINAL HYBRID SCORE
# =====================================================

def final_hybrid_score(

    resume_embedding,
    job_embedding,
    resume_text,
    job_text,
    posted_date="2026-01-01",
    applicant_count=10
):

    # -------------------------------------------------
    # Semantic Similarity
    # -------------------------------------------------

    semantic_score = cosine_similarity(
        resume_embedding,
        job_embedding
    )

    # -------------------------------------------------
    # Keyword Overlap
    # -------------------------------------------------

    keyword_score = keyword_overlap_score(
        resume_text,
        job_text
    )

    # -------------------------------------------------
    # Recency Score
    # -------------------------------------------------

    recency_score = recency_weight(
        posted_date
    )

    # -------------------------------------------------
    # Popularity Score
    # -------------------------------------------------

    popularity = popularity_score(
        applicant_count
    )

    # -------------------------------------------------
    # Final Weighted Score
    # -------------------------------------------------

    score = (

        0.55 * semantic_score
        + 0.25 * keyword_score
        + 0.10 * recency_score
        + 0.10 * popularity
    )

    return {
        "semantic_similarity":
        round(semantic_score, 4),

        "keyword_overlap":
        round(keyword_score, 4),

        "recency_weight":
        round(recency_score, 4),

        "popularity_score":
        round(popularity, 4),

        "final_score":
        round(score, 4)
    }