import os
import logging
import time
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.IndeedClients import IndeedJobClient, IndeedCompanyClient
from scripts.ProcessJobDesc import summarise_job_desc, top_similar_jobs
from scripts.ProcessHardSkills import getScrapedJobs, getHardSkills
import google.generativeai as genai

# Getting Environment Variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")


# Setting up logger if necessary
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Setting up MongoDB Connection
client = MongoClient(MONGODB_URI)
db = client["indeed"]
job_desc_collection = db["jobDescriptions"]
company_stats_collection = db["companyStats"]


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="IS3107-Project",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["IS3107-Project-Pipeline"],
)
def indeed_pipeline():
    @task(task_id="scrape_job_descriptions")
    def scrape_indeed_jobs(list_of_jobs: list[str]):
        indeed_job_client = IndeedJobClient("remote_chromedriver")
        time_scraped = indeed_job_client.scrape_job_listings(list_of_jobs)

        insert_results = job_desc_collection.insert_many(
            indeed_job_client.get_scraped_items()
        )
        job_desc_collection.create_index("expiration_date", expireAfterSeconds=0)

        logging.info(f"{__name__} has completed job scrapes with {str(insert_results)}")

        return time_scraped

    @task(task_id="scrape_company_statistics")
    def scrape_company_stats(date_scraped):

        pipeline = [
            {"$match": {"dateCreated": date_scraped}},
            {"$group": {"_id": "$companyUrl", "document": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$document"}},
            {
                "$project": {
                    "_id": 0,
                    "companyShorthand": "$companyShorthand",
                    "companyUrl": "$companyUrl",
                    "companyReviewUrl": "$companyReviewUrl",
                }
            },
        ]

        documents = list(job_desc_collection.aggregate(pipeline))

        indeed_company_client = IndeedCompanyClient("remote_chromedriver")
        indeed_company_client.scrape_companies_stats(documents)

        bulk_operations = []
        for doc in indeed_company_client.get_scraped_items():
            filter_criteria = {"companyShorthand": doc["companyShorthand"]}
            update_operation = UpdateOne(filter_criteria, {"$set": doc}, upsert=True)
            bulk_operations.append(update_operation)

        # Execute bulk write operations
        bulk_results = company_stats_collection.bulk_write(bulk_operations)
        logging.info(
            f"{__name__} has completed company scrapes with {str(bulk_results)}"
        )

        ## Return whatever you need returned
        return date_scraped

    # TODO TEXT SUMMARIZATION - MINGSHAN
    @task(task_id="summarise_job_descriptions")
    def summarise_job_desc(date_scraped):
        summarise_job_desc()

    # TODO TOP SIMILAR JOBS - MINGSHAN
    @task(task_id="identify_top_similar_jobs")
    def create_top_similar_jobs(argument_from_summarise_job_desc_func):
        ### Implement the function logic here with the intended argument (if required)
        top_similar_jobs()

    # TODO SKILLSETS SUMMARIZATION - RYU
    @task(task_id="summarise_hard_skills")
    def summarise_hard_skills(date_scraped):
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-pro")

        scraped_jobs_df = getScrapedJobs(date_scraped)
        scraped_jobs_df["hardSkills"] = scraped_jobs_df.apply(
            lambda row: getHardSkills(row["mainJob"], row["jobDescription"], model),
            axis=1,
        )

        print("summarisation of hard skills successful")

        # change the collection here too
        collection = db["hardSkills"]
        documents = scraped_jobs_df[["_id", "mainJob", "hardSkills"]].to_dict(
            orient="records"
        )
        collection.insert_many(documents)

    @task(task_id="complete_dag")
    def close_dag(*args, **kwargs):
        pass

    # data ingestion from indeed
    time_scraped = scrape_indeed_jobs(
        ["data science", "software engineer", "data analytics"]
    )
    time_scraped = scrape_company_stats(time_scraped)

    # TODO MINGSHAN
    summarise_job_output = summarise_job_desc(time_scraped)
    top_similar_output = create_top_similar_jobs(summarise_job_output)

    # TODO RYU
    summarise_skills_output = summarise_hard_skills(time_scraped)

    # closing off the dag
    close_dag(top_similar_output, summarise_skills_output)


indeed_etl = indeed_pipeline()
