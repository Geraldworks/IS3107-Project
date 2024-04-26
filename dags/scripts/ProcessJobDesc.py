import google.generativeai as genai
import requests
import chromadb

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from datetime import datetime
import time
import random

# Google's Gemini for job summary
GOOGLE_API_KEY = "AIzaSyCTxdvLLlueDQmnMwg5HXXHIz8BvunoxXA"
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel("gemini-pro")

# HuggingFace Model for generating embeddings
model_id = "sentence-transformers/all-MiniLM-L6-v2"
hf_token = "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"
api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_id}"
headers = {"Authorization": f"Bearer {hf_token}"}

# MongoDB
MONGODB_URI = "mongodb+srv://geraldho80:WAf5hj1MNPZxrPVF@cluster0.xtz5a2z.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


def get_job_summary(job_desc):
    response = model.generate_content(
        f"""
        Summarize the following Job Description by listing down the key job description/responsibilities in no more than 10 bullet points.
        List down the tech stack required for the job, as well as the qualifications.
        
        Job Description:{job_desc}                          
        """
    )

    parts = response.parts
    if not parts:
        # Handle the case where response doesn't contain a valid part
        print(response)
        return job_desc

    return response.text


def get_embeddings(texts):
    response = requests.post(
        api_url,
        headers=headers,
        json={"inputs": texts, "options": {"wait_for_model": True}},
    )
    return response.json()


def connect_to_mongodb(mongodb_uri):
    # Create a new client and connect to the server
    client = MongoClient(mongodb_uri, server_api=ServerApi("1"))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command("ping")
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client


def get_job_description_list(mongodb_client, date_scraped):
    db = mongodb_client["indeed"]
    collection = db["jobDescriptions"]

    result = []

    count = 0

    cs = collection.find({"dateCreated": {"$eq": date_scraped}})
    joblist = [x for x in list(cs)]
    for item in joblist:
        job_id = item["_id"]
        job_description = item["jobDescription"]
        created_date = item["dateCreated"]
        time.sleep(10)
        job_summary = get_job_summary(job_description)
        count += 1
        print(count)
        # if count == 100:
        #     break
        # embedding = get_embeddings([job_summary])
        embedding = [[0]]
        result.append((job_id, job_description, embedding, job_summary, created_date))

    job_description_list = result

    return job_description_list

def load_embeddings(mongodb_client, date_scraped):
    db = mongodb_client["indeed"]
    sum_collection = db["summarisedDescriptions"]
    target_collection = db["embeddings"]
    cs = sum_collection.find({"createdDate": {"$eq": date_scraped}})
    joblist = [x for x in list(cs)]
    print(joblist)
    print("hey")
    res = []
    for item in joblist:
        embedding = get_embeddings([item["summarisedJobDescription"]])
        if isinstance(embedding, str):
            embedding = [[random.uniform(-1.0, 1.0) for _ in range(384)]]
        _id = item["_id"]
        to_append = {"_id": _id, "embedding": embedding}
        res.append(to_append)
    
    print(res)

    for doc in res:
        existing_doc = target_collection.find_one({"_id": doc["_id"]})
        if not existing_doc:
            target_collection.insert_one(doc)
            print(f"Inserted document with _id: {doc['_id']}")
        else:
            # Replace existing document with the new one
            target_collection.replace_one({"_id": doc["_id"]}, doc)
            print(f"Replaced document with _id: {doc['_id']}")
    
    print("done loading embeddings")

def load_chromadb(mongodb_client):
    client = chromadb.Client()

    db = mongodb_client["indeed"]
    emb_collection = db["embeddings"]

    try:
        client.delete_collection(name="jobs")
    except:
        pass
    collection = client.create_collection(
        name="jobs", metadata={"hnsw:space": "cosine"}  # l2 is the default
    )

    collection = client.get_collection(name="jobs")

    ids = []
    documents = []
    embeddings = []

    cs = emb_collection.find()
    all = [x for x in list(cs)]
    # loop = random.sample(all, 200)
    print(all)
    print(len(all))
    count = 0
    for item in all:
        count += 1
        # print(count)
        # print(item["embedding"])
        ids.append(item["_id"])
        documents.append(f"doc {count}")
        embeddings.append(item["embedding"][0])
    

    # documents = [i[1] for i in job_description_list]
    # job_summaries = [i[3] for i in job_description_list]
    # embeddings = [i[2][0] for i in job_description_list]
    # ids = [str(i[0]) for i in job_description_list]

    collection.add(documents=documents, embeddings=embeddings, ids=ids)

    return collection


def query_chromadb_for_similar_jobs(collection, selected_job_embedding, k=6):
    similar_jobs = collection.query(
        query_embeddings=selected_job_embedding, n_results=k
    )
    return similar_jobs


def zip_job_id_embed(collection):
    print("in zip function")
    res = collection.get(include=["embeddings"])
    zipped = list(zip(res["ids"], res["embeddings"]))

    return zipped


def get_data(zipped, collection):
    print("in get data function")
    data = []

    for id, embedding in zipped:
        res = query_chromadb_for_similar_jobs(collection, embedding)
        nearest_jobs = res["ids"][0]
        nearest_jobs.remove(id)
        to_append = {"jobID": id, "nearest_jobs": nearest_jobs}
        data.append(to_append)

    return data


def load_similar_job_into_mongodb(mongo_client, data):
    db = mongo_client["indeed"]
    collection = db["topSimilarJobs"]

    # Delete all existing documents from the collection
    collection.delete_many({})

    collection.insert_many(data)

    print("Data inserted successfully.")


def load_job_summaries(mongo_client, job_description_list):
    ids = [str(i[0]) for i in job_description_list]
    job_summaries = [i[3] for i in job_description_list]
    dates = [i[4] for i in job_description_list]

    zipped = list(zip(ids, job_summaries, dates))

    data = []

    for id, summary, d in zipped:
        to_append = {"_id": id, "summarisedJobDescription": summary, "createdDate": d}

        data.append(to_append)

    db = mongo_client["indeed"]
    collection = db["summarisedDescriptions"]

    # Delete all existing documents from the collection
    # collection.delete_many({})
    # collection.insert_many(data)

    for doc in data:
        existing_doc = collection.find_one({"_id": doc["_id"]})
        if not existing_doc:
            collection.insert_one(doc)
            print(f"Inserted document with _id: {doc['_id']}")
        else:
            # Replace existing document with the new one
            collection.replace_one({"_id": doc["_id"]}, doc)
            print(f"Replaced document with _id: {doc['_id']}")

    print("Data inserted successfully into summarisedDescriptions.")


def main(date_scraped):
    mongodb_client = connect_to_mongodb(MONGODB_URI)
    job_description_list = get_job_description_list(mongodb_client, date_scraped)
    load_job_summaries(mongodb_client, job_description_list)

    collection = load_chromadb(mongodb_client)
    zipped = zip_job_id_embed(collection)
    data = get_data(zipped, collection)
    load_similar_job_into_mongodb(mongodb_client, data)


def summarise_jobs(date_scraped):
    mongodb_client = connect_to_mongodb(MONGODB_URI)
    job_description_list = get_job_description_list(mongodb_client, date_scraped)
    load_job_summaries(mongodb_client, job_description_list)


def top_similar_jobs(date_scraped):
    mongodb_client = connect_to_mongodb(MONGODB_URI)
    load_embeddings(mongodb_client, date_scraped)
    collection = load_chromadb(mongodb_client)
    zipped = zip_job_id_embed(collection)
    data = get_data(zipped, collection)
    load_similar_job_into_mongodb(mongodb_client, data)


# if __name__ == "__main__":
#     timestamp_str = "2024-04-06T10:00:05.696+00:00"
#     timestamp_format = "%Y-%m-%dT%H:%M:%S.%f%z"

#     timestamp = datetime.strptime(timestamp_str, timestamp_format)
#     main(timestamp)
