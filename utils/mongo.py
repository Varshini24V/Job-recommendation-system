from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["resume_matcher"]

resumes_col = db["resumes"]
jobs_col = db["jobs"]
matches_col = db["matches"]