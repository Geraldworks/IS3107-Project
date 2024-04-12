import streamlit as st
from pymongo import MongoClient
import pandas as pd
import certifi
import matplotlib.pyplot as plt
import numpy as np


ca = certifi.where()

password = st.secrets.db_pswd
username = st.secrets.db_username
cluster_name = st.secrets.cluster_name

uri = f"mongodb+srv://{username}:{password}@{cluster_name}.xtz5a2z.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


def init_connection():
    return MongoClient(uri, tlsCAFile=ca)


client = init_connection()


text_search = st.text_input("Search job", value="")


def get_companyStats_data():
    db = client.indeed  # establish connection to the 'sample_guide' db
    items = db.jobDescriptions.find()
    return pd.DataFrame(list(items))


def get_summarised_descriptions():
    db = client.indeed
    items = db.summarisedDescriptions.find()
    return pd.DataFrame(list(items))


def get_top_similiar_jobs():
    db = client.indeed
    items = db.topSimilarJobs.find()
    return pd.DataFrame(list(items))


data = get_companyStats_data()
summarised_descriptions = get_summarised_descriptions()
top_similar_jobs = get_top_similiar_jobs()

data["str_id"] = data["_id"].astype(str)

job_df = data[data["jobTitle"].str.contains(text_search, case=False)]

if text_search:
    if len(job_df) == 0:
        st.write("Job not found")
    else:
        job_df["str_id"] = job_df["_id"].astype(str)
        for row in range(len(job_df)):
            st.write(job_df.iloc[row])
            date_posted = str(job_df.iloc[row]["dateCreated"]).split(" ")[0]
            company_name = job_df.iloc[row]["companyName"]
            job_title = job_df.iloc[row]["jobTitle"]
            jd = job_df.iloc[row]["jobDescription"]
            apply_url = job_df.iloc[row]["applyNowUrl"]
            job_id = job_df.iloc[row]["str_id"]
            summarised_description = summarised_descriptions[
                summarised_descriptions["_id"] == job_id
            ].iloc[0]["summarisedJobDescription"]
            similar_jobs = top_similar_jobs[top_similar_jobs["jobID"] == job_id].iloc[
                0
            ]["nearest_jobs"]

            with st.expander(f"{job_title} @ {company_name}"):
                st.write(f"Date posted : {date_posted}")
                st.write(company_name)
                st.write(job_title)
                st.write(jd)
                st.write(f"Apply [here]({apply_url})")
                st.write("\n")
                st.write("HERE BELOW IS USING LLM FOR SUMMARISED DESCRIPTIONS")
                st.write(summarised_description)
                st.write("\n")
                st.write("HERE BELOW IS THE TOP SIMILAR JOBS")
                st.write(similar_jobs)

                for similar_job_id in similar_jobs:
                    st.write(f"Job ID : {similar_job_id}")
                    similar_job = data.loc[data["str_id"] == similar_job_id]
                    similar_job_title = similar_job.iloc[0]["jobTitle"]
                    similar_job_company = similar_job.iloc[0]["companyName"]
                    st.write(f"Job Title : {similar_job_title} @ {similar_job_company}")
