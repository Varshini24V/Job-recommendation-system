import streamlit as st
import boto3
import uuid
import os
import requests
import pandas as pd
import matplotlib.pyplot as plt

from dotenv import load_dotenv

# =====================================================
# LOAD ENV VARIABLES
# =====================================================

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BUCKET = os.getenv("S3_BUCKET")
API_URL = os.getenv("API_GATEWAY_URL")
s3 = boto3.client("s3",region_name=AWS_REGION)

# =====================================================
# STREAMLIT CONFIG
# =====================================================

st.set_page_config(page_title="Job Recommendation System",layout="wide")
st.title("Job Recommendation System")
st.caption("Resume-Based Job Suggestion and Skill-Gap Analysis System using AWS Bedrock, RAG Architecture,and Streamlit")

# =====================================================
# SESSION STATE
# =====================================================

if "resume_id" not in st.session_state:
    st.session_state["resume_id"] = None
if "matches_data" not in st.session_state:
    st.session_state["matches_data"] = None

# =====================================================
# SIDEBAR
# =====================================================

with st.sidebar:

    st.header("Actions")
    st.markdown("---")
    # =================================================
    # CLEAR SESSION
    # =================================================
    if st.button("Clear Session",use_container_width=True):
        st.session_state.clear()
        st.rerun()
    st.markdown("---")

    # =================================================
    # REFRESH JOBS
    # =================================================
    st.subheader("Daily Refresh")
    st.caption("Fetch latest jobs from APIs")

    refresh_button = st.button("Refresh Jobs",use_container_width=True)

    if refresh_button:
        with st.spinner("Refreshing latest jobs..."):

            try:
                refresh_response = requests.post(
                    f"{API_URL}/refresh",
                    headers={
                        "Content-Type":
                        "application/json"},
                    json={},
                    timeout=120
                )
                if (refresh_response.status_code== 200):
                    response_data = (refresh_response.json())
                    jobs_inserted = (response_data.get("jobs_inserted",0))
                    st.success("Job refresh completed")

                    st.info(f"Jobs Inserted: "
                        f"{jobs_inserted}")
                else:
                    st.error("Refresh API failed")
                    st.code(refresh_response.text)

            except Exception as refresh_error:
                st.error(
                    f"Refresh Error:\n"
                    f"{str(refresh_error)}"
                )

# =====================================================
# RESUME UPLOAD
# =====================================================

st.subheader("Upload Resume")

uploaded_file = st.file_uploader("Upload Resume (PDF)",type=["pdf"])
if (uploaded_file and not st.session_state["resume_id"]):

    with st.spinner("Uploading and processing resume..."):

        try:
            resume_id = (f"{uuid.uuid4()}.pdf")

            s3.put_object(Bucket=BUCKET,
                          Key=resume_id,
                          Body=uploaded_file.getvalue(),
                          Metadata={"uploaded_by": "streamlit_user",
                                    "file_type": "pdf"})

            upload_response = requests.post(
                f"{API_URL}/upload",
                json={"resume_id": resume_id},
                timeout=300
            )
            upload_data = (upload_response.json())

            if "error" in upload_data:
                st.error(upload_data["error"])
                st.stop()

            st.session_state["resume_id"] = resume_id

            st.success("Resume uploaded successfully")

        except Exception as upload_error:

            st.error(
                f"Upload failed: "
                f"{str(upload_error)}"
            )

            st.stop()

# =====================================================
# MATCH JOBS
# =====================================================

if st.button("Find Matching Jobs",use_container_width=True):

    if not st.session_state["resume_id"]:
        st.warning("Please upload a resume first.")
        st.stop()

    with st.spinner("Finding best matching jobs..."):

        try:
            response = requests.post(
                f"{API_URL}/match",
                json={
                    "resume_id":st.session_state["resume_id"]
                }, timeout=120
            )

            data = response.json()

            if ("matches" not in data or not data["matches"]):
                st.warning("No suitable jobs found.")
                st.stop()

            st.session_state["matches_data"] = data
            st.success("Matching completed")

        except Exception as match_error:
            st.error(
                f"Matching failed: "
                f"{str(match_error)}"
            )

# =====================================================
# DISPLAY RESULTS
# =====================================================

if st.session_state["matches_data"]:
    data = st.session_state["matches_data"]
    matches = data.get("matches", [])

    df = pd.DataFrame(matches)

    if not df.empty:
        avg_score = round(df["final_score"].mean(),2)
        best_score = round(df["final_score"].max(),2)
        total_jobs = len(df)

        col1, col2, col3 = st.columns(3)
        col1.metric("Jobs Found",total_jobs)
        col2.metric("Average Match",f"{avg_score}%")
        col3.metric("Best Match",f"{best_score}%")

    st.subheader("Ranked Job Matches")

    table_rows = []
    for _, row in df.iterrows():
        apply_link = row.get("apply_link","#")
        apply_html = (f'<a href="{apply_link}" '
                      f'target="_blank">'
                      f'Apply Here</a>')

        table_rows.append({"Company":row.get("company", "N/A"),
                          "Job Title":row.get("title", "N/A"),
                          "Match Score":f"{round(float(row.get('final_score', 0)), 2)}%",
                          "Apply": apply_html})

    table_df = pd.DataFrame(table_rows)

    if table_df.empty:
        st.warning("No matching jobs found.")

    else:
        st.markdown(table_df.to_html(
            escape=False,
            index=False),
            unsafe_allow_html=True
        )

    st.subheader("Match Score Distribution")

    if ( not df.empty and "title" in df.columns and "final_score" in df.columns):

        chart_df = df[["title","final_score"]].dropna()
        chart_df = chart_df.set_index("title")
        st.bar_chart(chart_df)
    else:
        st.warning("No chart data available.")

    # =================================================
    # TOP MATCH
    # =================================================

    if not df.empty:
        best_job = df.sort_values(by="final_score",ascending=False).iloc[0]
        st.success(f"Top Recommendation: "
            f"{best_job['title']} "
            f"at "
            f"{best_job['company']} "
            f"({best_job['final_score']}%)"
        )

    st.subheader("Skill Gap Heatmap")

    try:
        all_missing_skills = []
        for job in matches:
            missing = job.get("missing_skills",[])
            if missing:
                all_missing_skills.extend(
                    [
                        skill.strip().lower()
                        for skill in missing
                    ]
                )

        if all_missing_skills:
            skill_frequency = {}
            for skill in all_missing_skills:
                skill_frequency[skill] = (skill_frequency.get(skill,0) + 1)

            skill_df = pd.DataFrame({
                "Skill": [ skill.title() for skill in skill_frequency.keys()],
                "Frequency": list( skill_frequency.values())})
            skill_df["Gap Score"] = (100-(skill_df["Frequency"]* 30))
            skill_df["Gap Score"] = (skill_df["Gap Score"].clip(lower=10))
            skill_df = skill_df.sort_values(by="Gap Score",ascending=True)
            fig, ax = plt.subplots(figsize=(10, 6))

            ax.barh(skill_df["Skill"],
                skill_df["Gap Score"])
            ax.set_xlabel("Skill Match %")
            ax.set_ylabel("Skills")
            ax.set_title("Skill Gap Analysis")
            st.pyplot(fig)

            st.markdown("### Important Missing Skills")
            st.write(", ".join(skill_df["Skill"].tolist()))

        else:
            st.success("No major skill gaps detected.")

    except Exception as heatmap_error:

        st.warning(f"Heatmap failed: "
            f"{str(heatmap_error)}")

    # =================================================
    # WHY THESE JOBS MATCH
    # =================================================

    st.subheader("Why These Jobs Match")

    for job in data["matches"]:
        with st.expander(
            job.get("title","Job")):
            st.markdown(f"### {job.get('title', 'N/A')}")
            st.write(f"**Company:** " f"{job.get('company', 'N/A')}")
            st.write(f"**Match Score:** " f"{job.get('final_score', 0)}%")

            st.markdown("### AI Match Analysis")

            reasoning = job.get("reasoning","No reasoning available")
            st.write(reasoning)

            matched_skills = job.get("skills",[])

            if matched_skills:
                st.markdown("### Matching Skills")
                st.write(", ".join([
                        skill.title()
                        for skill in matched_skills]))

            missing_skills = job.get("missing_skills",[])
            if missing_skills:
                st.markdown("### Missing Skills")
                st.write(", ".join([skill.title() for skill in missing_skills]))

            else:
                st.success("No major missing skills detected.")

            courses = job.get("courses",[])

            if courses:
                st.markdown("### Recommended Learning")
                for idx, course in enumerate(courses):
                    title = course.get("title","Course")
                    url = course.get("url","#")
                    provider = course.get("provider","Online")
                    st.markdown(f"""
                        **{idx+1}. {title}**
                        Provider: {provider}
                        [Open Course]({url})
                        """
                    )

            else:

                st.info("No course recommendations available.")

            apply_link = job.get("apply_link","#")
            st.markdown(f"[Apply for this Job]({apply_link})")