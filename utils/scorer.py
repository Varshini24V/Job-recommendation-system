def compute_score(similarity, keyword, recency, popularity):
    return (
        0.55 * similarity +
        0.25 * keyword +
        0.10 * recency +
        0.10 * popularity
    )