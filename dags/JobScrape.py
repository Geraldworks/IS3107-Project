import os
import logging
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from typing import Any
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.IndeedClients import IndeedJobClient, IndeedCompanyClient

# Getting Environment Variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")


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
    dag_id="is3107",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["is3107-pipeline"],
)
def indeed_pipeline():
    @task(task_id="scrape_job_desc")
    def scrape_indeed_jobs(list_of_jobs: list[str]):
        indeed_job_client = IndeedJobClient("remote_chromedriver")
        time_scraped = indeed_job_client.scrape_job_listings(list_of_jobs)

        insert_results = job_desc_collection.insert_many(
            indeed_job_client.get_scraped_items()
        )
        job_desc_collection.create_index("expiration_date", expireAfterSeconds=0)

        logging.info(f"{__name__} has completed job scrapes with {str(insert_results)}")

        return time_scraped

    @task(task_id="scrape_company_stats")
    def scrape_company_stats(**kwargs):
        date_scraped = kwargs.get("time_scraped")

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

    time_scraped = scrape_indeed_jobs(["data science", "software engineer"])
    scrape_company_stats(time_scraped=time_scraped)


indeed_etl = indeed_pipeline()
