import streamlit as st
import boto3
import uuid
import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv

# -------------------------------
# LOAD ENV
# -------------------------------
load_dotenv()

BUCKET = os.getenv("S3_BUCKET")
API_URL = os.getenv("API_GATEWAY_URL")

s3 = boto3.client("s3")

st.set_page_config(page_title="AI Resume Matcher", page_icon="🚀", layout="wide")
st.title("🚀 AI Resume Matcher")

# -------------------------------
# SESSION STATE
# -------------------------------
if "resume_id" not in st.session_state:
    st.session_state.resume_id = None

# -------------------------------
# UPLOAD SECTION
# -------------------------------
st.subheader("📄 Upload Resume")

uploaded_file = st.file_uploader("Upload your resume (PDF)", type=["pdf"])

if uploaded_file:
    # Generate a unique ID for this session's resume
    resume_id = str(uuid.uuid4())
    key = f"resumes/{resume_id}.pdf"

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=uploaded_file.getvalue(),
            ContentType="application/pdf"
        )

        st.session_state.resume_id = resume_id

        st.success("✅ Resume uploaded successfully!")
        st.code(f"Resume ID: {resume_id}")

    except Exception as e:
        st.error(f"❌ Upload failed: {e}")

# -------------------------------
# MATCHING SECTION
# -------------------------------
st.subheader("🔍 Find Matching Jobs")

if st.button("Find Jobs"):

    resume_id = st.session_state.resume_id

    if not resume_id:
        st.warning("⚠️ Please upload a resume first.")
        st.stop()

    with st.spinner("⏳ Processing resume and finding matches..."):

        max_retries = 6
        retry_delay = 3
        data = None

        for attempt in range(max_retries):
            try:
                # ✅ FIX: Use the actual resume_id instead of "latest" to prevent multi-user race conditions
                res = requests.post(
                    f"{API_URL}/match",
                    json={"resume_id": resume_id} 
                )

                if res.status_code != 200 and res.status_code != 404:
                    st.error(f"API Error ({res.status_code}): {res.text}")
                    st.stop()

                data = res.json()

                # -------------------------------
                # HANDLE NOT READY / PROCESSING
                # -------------------------------
                if data.get("error") == "No resume found" or not data.get("matches"):
                    st.info(f"🔄 Processing document & generating embeddings... ({attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                    continue

                # If we have matches, break out of the retry loop
                if data.get("matches"):
                    break

            except Exception as e:
                st.error(f"❌ API connection failed: {e}")
                st.stop()

    # -------------------------------
    # FINAL CHECK
    # -------------------------------
    if not data or not data.get("matches"):
        st.error("❌ Processing took too long or no matches were found. Please try again.")
        st.stop()

    # -------------------------------
    # DISPLAY RESULTS
    # -------------------------------
    df = pd.DataFrame(data["matches"])

    st.success(f"🎯 Found {len(df)} Matches!")

    # Format the dataframe for better UI presentation
    if "score" in df.columns:
        df = df.sort_values(by="score", ascending=False).reset_index(drop=True)
        # Create a readable percentage column
        df["Match %"] = (df["score"] * 100).round(1).astype(str) + "%"
        
        # Safely select columns that actually exist in the DataFrame
        desired_cols = ["title", "company", "Match %"]
        cols_to_show = [col for col in desired_cols if col in df.columns]
        
        st.dataframe(df[cols_to_show], use_container_width=True)
        # -------------------------------
        # VISUALIZATION
        # -------------------------------
        st.subheader("📊 Match Score Visualization")
        
        # Streamlit's native bar chart is cleaner and interactive compared to matplotlib
        chart_data = df.set_index("title")["score"]
        st.bar_chart(chart_data)

    else:
        st.dataframe(df, use_container_width=True)
        st.warning("⚠️ Vector search scores were not returned by the API.")