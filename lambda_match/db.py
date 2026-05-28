import os
import certifi
from pymongo import MongoClient

client = MongoClient(os.getenv("MONGO_URI"), tls=True, tlsCAFile=certifi.where())

db = client["resume_matcher"]
resumes = db["resumes"]
jobs = db["jobs"]
matches = db["matches"]