import requests
from utils.mongo import jobs_col
from utils.embeddings import get_embedding
import os

url = "https://jsearch.p.rapidapi.com/search"

headers = {
    "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY")
}

params = {"query": "data engineer", "page": "1"}

res = requests.get(url, headers=headers, params=params)

for job in res.json()["data"]:
    desc = job["job_description"]

    jobs_col.insert_one({
        "title": job["job_title"],
        "description": desc,
        "embedding": get_embedding(desc)
    })