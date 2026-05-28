def recommend(skills):
    return [
        {
            "title": f"Learn {skill}",
            "url": f"https://www.coursera.org/search?query={skill}"
        }
        for skill in skills
    ]