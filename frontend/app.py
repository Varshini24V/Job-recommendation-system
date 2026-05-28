import streamlit as st
import boto3
import uuid
import os
import requests
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# LOAD ENV VARIABLES

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("S3_BUCKET")
API_URL = os.getenv("API_GATEWAY_URL")

s3 = boto3.client("s3",region_name=AWS_REGION)

# STREAMLIT PAGE CONFIG

st.set_page_config(page_title="Job recommendation System",layout="wide")
st.title("Job recommendation System")

st.caption("Resume-Based Job Suggestion and Skill-Gap Analysis System using AWS Bedrock, RAG Architecture, and Streamlit")

# SESSION STATE

if "resume_id" not in st.session_state:
    st.session_state["resume_id"] = None
if "matches_data" not in st.session_state:
    st.session_state["matches_data"] = None

# RESUME UPLOAD

st.subheader("Upload Resume")
uploaded_file = st.file_uploader(
    "Upload Resume (PDF)",
    type=["pdf"]
)

# Upload ONLY once
if uploaded_file and not st.session_state["resume_id"]:

    with st.spinner("Uploading and processing resume..."):

        try:

            # Generate Unique Resume ID
            resume_id = f"{uuid.uuid4()}.pdf"

            # Upload Resume to S3
            s3.put_object(
                Bucket=BUCKET,
                Key=resume_id,
                Body=uploaded_file.getvalue(),
                Metadata={
                    "uploaded_by": "streamlit_user",
                    "file_type": "pdf"
                }
            )

            # Trigger Upload Lambda
            upload_response = requests.post(
                f"{API_URL}/upload",
                json={"resume_id": resume_id},
                timeout=300
            )

            upload_data = upload_response.json()

            # Handle Errors
            if "error" in upload_data:
                st.error(upload_data["error"])
                st.stop()

            # Save Resume ID in session
            st.session_state["resume_id"] = resume_id

            st.success("Resume uploaded successfully")

        except Exception as upload_error:

            st.error(
                f"Upload failed: {str(upload_error)}"
            )

            st.stop()

# FIND MATCHING JOBS

if st.button("Find Matching Jobs",use_container_width=True):

    if not st.session_state["resume_id"]:
        st.warning("Please upload a resume first.")
        st.stop()

    with st.spinner("Finding best matching jobs..."):
        try:

            # MATCH API
            response = requests.post(f"{API_URL}/match",
                json={"resume_id":st.session_state["resume_id"]},
                timeout=120
            )
            data = response.json()

            # VALIDATE RESPONSE
            if "matches" not in data:
                st.error(data)
                st.stop()

            # STORE MATCHES IN SESSION
            st.session_state["matches_data"] = data
            st.success("Matching completed")

        except Exception as match_error:
            st.error(
                f"Matching failed: "
                f"{str(match_error)}"
            )

# DISPLAY MATCH RESULTS

if st.session_state["matches_data"]:

    data = st.session_state["matches_data"]
    df = pd.DataFrame(data["matches"])

    # RANKED JOB MATCHES
    st.subheader(
        "Ranked Job Matches"
    )

    table_data = []

    for _, row in df.iterrows():

        apply_link = row.get("apply_link","#")

        apply_html = (f'<a href="{apply_link}" '
            f'target="_blank">Apply Here</a>'
        )

        table_data.append({"Company Name":row.get("company","N/A"),
            "Job Title":row.get("title","N/A"),
            "Match Score":round(float(row.get("final_score",0)),2),
            "Apply":apply_html
        })

    table_df = pd.DataFrame(table_data)
    table_df = table_df.sort_values(by="Match Score",ascending=False)

    st.write(table_df.to_html(escape=False,index=False),
        unsafe_allow_html=True
    )

    # MATCH SCORE DISTRIBUTION

    st.subheader("Match Score Distribution")

    chart_df = df[["title","final_score"]].copy()
    chart_df = chart_df.set_index("title")
    st.bar_chart(chart_df)

    # SKILL GAP ANALYSIS
    st.subheader("Skill Gap Heatmap")

    try:
        reasoning_text = " ".join([job.get("reasoning","") 
            for job in data["matches"]
        ]).lower()

        tracked_skills = ["python","sql","aws","spark","docker","kubernetes","airflow","mongodb",
                          "postgresql","mysql","tableau","power bi","tensorflow","machine learning",
                          "deep learning", "etl","api","git","linux","pandas", 
                          "numpy","data engineering","data analysis"]
        skill_scores = {}

        for skill in tracked_skills:
            if skill in reasoning_text:
                skill_scores[skill.title()] = 100

            else:
                skill_scores[skill.title()] = 25

        skill_df = pd.DataFrame(list(skill_scores.items()),
            columns=["Skill","Match Score"]
        )

        skill_df = skill_df.sort_values(by="Match Score",ascending=False)

        fig, ax = plt.subplots(figsize=(10, 6))

        ax.barh(skill_df["Skill"],skill_df["Match Score"])
        ax.set_xlabel("Skill Match %")
        ax.set_ylabel("Skills")
        ax.set_title("Candidate Skill Match")
        ax.invert_yaxis()

        st.pyplot(fig)

        # Missing Skills

        missing_skills = (skill_df[skill_df["Match Score"] < 100]
                          ["Skill"].tolist())

        if missing_skills:
            st.markdown("### Missing Skills")

            st.write(", ".join(missing_skills))
    except Exception as heatmap_error:

        st.warning(
            f"Heatmap failed: "
            f"{str(heatmap_error)}"
        )

    # AI REASONING

    st.subheader("Why These Jobs Match")

    for job in data["matches"]:
        with st.expander(job.get("title","Job")):
            st.write(
                job.get("reasoning","No reasoning available"))
            
            # RECOMMENDED COURSES
            if ("courses" in job and job["courses"]):

                st.markdown("### Recommended Learning")
                for course in job["courses"]:
                    st.markdown(
                        f"- "
                        f"[{course.get('title', 'Course')}]"
                        f"({course.get('url', '#')})"
                    )


# =====================================================
# DAILY REFRESH
# =====================================================

with st.sidebar:
    st.header("Actions")
    st.markdown("---")
    st.subheader("Daily Refresh")
    st.caption("Fetch latest jobs from APIs")

    refresh_button = st.button("Refresh Jobs",
        use_container_width=True,
        key="refresh_jobs_button")

    if refresh_button:
        with st.spinner("Refreshing latest jobs..."):
            try:
                refresh_url = (f"{API_URL}/refresh")
                print("Calling Refresh API:",refresh_url)

                # CALL REFRESH API
                refresh_response = requests.post(refresh_url,
                                                 headers={"Content-Type":"application/json"},
                    json={},
                    timeout=120
                )

                # DEBUG LOGS
                print("Refresh Status:",
                    refresh_response.status_code)

                print("Refresh Response:",
                    refresh_response.text)

                # SUCCESS
                if (refresh_response.status_code== 200):
                    response_data = (refresh_response.json())
                    jobs_inserted = (response_data.get("jobs_inserted",0))

                    st.success("Job refresh completed")
                    st.info( f"Jobs Inserted: "
                        f"{jobs_inserted}"
                    )

                    if jobs_inserted == 0:

                        st.warning("No new jobs were found.")
                else:

                    st.error("Refresh API failed")
                    st.code(refresh_response.text)

            except requests.exceptions.Timeout:

                st.error("Request timed out.")

            except Exception as refresh_error:

                st.error(
                    f"Refresh Error:\n"
                    f"{str(refresh_error)}"
                )