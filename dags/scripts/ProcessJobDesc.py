import google.generativeai as genai
import requests
import chromadb
import os

from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()

# Google's Gemini for job summary
GOOGLE_API_KEY = "AIzaSyApn-BsxgFwWHaweLwaSdNVeSLKy-xsH14"
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-pro')

# HuggingFace Model for generating embeddings
model_id = "sentence-transformers/all-MiniLM-L6-v2"
hf_token = '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'
api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_id}"
headers = {"Authorization": f"Bearer {hf_token}"}

# MongoDB
MONGODB_URI = os.getenv('MONGODB_URI')

def get_job_summary(job_desc):
    response = model.generate_content(
        f'''
        Summarize the following Job Description by listing down the key job description/responsibilities in no more than 10 bullet points.
        List down the tech stack required for the job, as well as the qualifications.
        
        Job Description:{job_desc}                          
        '''
    )
    
    parts = response.parts
    if not parts:
        # Handle the case where response doesn't contain a valid part
        print(response)
        return job_desc
    
    return response.text

def get_embeddings(texts):
    response = requests.post(api_url, headers=headers, json={"inputs": texts, "options":{"wait_for_model":True}})
    return response.json()


def connect_to_mongodb(mongodb_uri):
    # Create a new client and connect to the server
    client = MongoClient(mongodb_uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        
    return client


def get_job_description_list(mongodb_client):
    db = mongodb_client["indeed"]
    collection = db['jobDescriptions']
    
    result = []
    
    for item in collection.find():
        job_id = item['_id']
        job_description = item['jobDescription']
        job_summary = get_job_summary(job_description)
        embedding = get_embeddings([job_summary])
        result.append((job_id, job_description, embedding, job_summary))

    job_description_list = result
    
    return job_description_list

def load_chromadb(job_description_list):
    client = chromadb.Client()
    
    try:
        client.delete_collection(name="jobs")
    except:
        pass
    collection = client.create_collection(
        name="jobs",
        metadata={"hnsw:space": "cosine"} # l2 is the default
    )
    
    collection = client.get_collection(name="jobs")
    
    documents = [i[1] for i in job_description_list]
    job_summaries = [i[3] for i in job_description_list]
    embeddings = [i[2][0] for i in job_description_list]
    ids = [str(i[0]) for i in job_description_list]
    
    collection.add(
        documents=documents,
        embeddings=embeddings,
        ids=ids
    )
    
    return collection

def query_chromadb_for_similar_jobs(collection, selected_job_embedding, k=6):
    similar_jobs = collection.query(query_embeddings=selected_job_embedding, n_results=k)
    return similar_jobs


def zip_job_id_embed(collection):
    res = collection.get(include=['embeddings'])
    zipped = list(zip(res['ids'], res['embeddings']))
    
    return zipped


def get_data(zipped, collection):
    
    data = []
    
    for id, embedding in zipped:
        res = query_chromadb_for_similar_jobs(collection, embedding)
        nearest_jobs = res['ids'][0]
        nearest_jobs.remove(id)
        to_append = {
            'jobID': id,
            "nearest_jobs": nearest_jobs
        }
        data.append(to_append)
    
    return data

def load_similar_job_into_mongodb(mongo_client, data):
    db = mongo_client['indeed']
    collection = db['topSimilarJobs']
    
    # Delete all existing documents from the collection
    collection.delete_many({})
    
    collection.insert_many(data)
    
    print("Data inserted successfully.")
    
def load_job_summaries(mongo_client, job_description_list):
    ids = [str(i[0]) for i in job_description_list]
    job_summaries = [i[3] for i in job_description_list]
    
    zipped = list(zip(ids, job_summaries))
    
    data = []
    
    for id, summary in zipped:
        to_append = {
            '_id': id,
            'summarisedJobDescription': summary
        }
        
        data.append(to_append)
        
    db = mongo_client['indeed']
    collection = db['summarisedDescriptions']
    
    # Delete all existing documents from the collection
    collection.delete_many({})
    collection.insert_many(data)
    
    print("Data inserted successfully.")
    
def main():
    mongodb_client = connect_to_mongodb(MONGODB_URI)
    job_description_list = get_job_description_list(mongodb_client)
    collection = load_chromadb(job_description_list)
    zipped = zip_job_id_embed(collection)
    data = get_data(zipped, collection)
    load_similar_job_into_mongodb(mongodb_client, data)
    load_job_summaries(mongodb_client, job_description_list)

if __name__ == "__main__":
    main()