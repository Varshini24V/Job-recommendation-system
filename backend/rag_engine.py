from bedrock_utils import analyze_job_match


# =========================================================
# OPTIMIZED RAG ANALYSIS
# =========================================================

def analyze(
    resume,
    job
):

    try:

        # -------------------------------------------------
        # Reduce Input Size
        # -------------------------------------------------

        short_resume = (
            resume[:1500]
            if resume else ""
        )

        short_job = (
            job[:1500]
            if job else ""
        )

        # -------------------------------------------------
        # Optimized Prompt
        # -------------------------------------------------

        prompt = f"""

You are an AI Resume Matching Assistant.

Analyze how well the candidate profile matches the job description.

Requirements:
- Keep the response concise
- Maximum 120 words
- Use short bullet points
- Mention:
  1. Matching skills
  2. Missing skills
  3. Overall suitability
- Avoid long explanations
- Avoid tables
- Avoid markdown

Candidate Resume:
{short_resume}

Job Description:
{short_job}

"""

        # -------------------------------------------------
        # Bedrock Claude Analysis
        # -------------------------------------------------

        response = analyze_job_match(
            prompt,
            ""
        )

        # -------------------------------------------------
        # Handle Empty Response
        # -------------------------------------------------

        if not response:

            return (
                "No reasoning available"
            )

        # -------------------------------------------------
        # Limit Response Size
        # -------------------------------------------------

        return response[:1000]

    except Exception as e:

        print(
            "RAG Engine Error:",
            str(e)
        )

        return (
            "AI reasoning unavailable"
        )