from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import google.generativeai as genai
import os
import pandas as pd

MONGODB_URI = os.getenv("MONGODB_URI")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


def getScrapedJobs(date_scraped):
    client = MongoClient(MONGODB_URI, server_api=ServerApi("1"))
    db = client["indeed"]
    collection = db["jobDescriptions"]

    scraped_jobs = collection.find(
        {"dateCreated": {"$eq": date_scraped}},
        {"_id": 1, "mainJob": 1, "jobDescription": 1},
    )
    scraped_jobs_df = pd.DataFrame(scraped_jobs)

    print("retrieving data from mongodb successful")

    return scraped_jobs_df


def getHardSkills(mainJob, jobDescription, model):
    prompt = f"""
    Output the tech stack (tools and software ONLY) mentioned in this {mainJob} job description:

    {jobDescription}

    Please format the output like this (please don't use commas other than for separation)
    Skill 1, Skill 2, Skill 3

    If no tech stack mentioned, output NONE
    """

    response = model.generate_content(prompt)
    try:
        return response.text
    except:
        return "NONE"
