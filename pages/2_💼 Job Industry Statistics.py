import streamlit as st
from pymongo import MongoClient
import pandas as pd
import certifi
import matplotlib.pyplot as plt
import numpy as np


st.title("Trending data jobs in the industry")

ca = certifi.where()

password = st.secrets.db_pswd
username = st.secrets.db_username
cluster_name = st.secrets.cluster_name

uri = f"mongodb+srv://{username}:{password}@{cluster_name}.xtz5a2z.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


def init_connection():
    return MongoClient(uri, tlsCAFile=ca)


client = init_connection()


def get_jobs_data():
    db = client.indeed  # establish connection to the 'sample_guide' db
    items = db.jobDescriptions.find()
    return pd.DataFrame(list(items))


data = get_jobs_data()
data["jobTitle"] = data["jobTitle"].str.lower()

data_analyst_counts = len(data[data["jobTitle"].str.contains("data analyst")])
data_engineer_counts = len(data[data["jobTitle"].str.contains("data engineer")])
swe_counts = len(data[data["jobTitle"].str.contains("software engineer")])
data_scientist_counts = len(data[data["jobTitle"].str.contains("data scientist")])
business_analyst_counts = len(data[data["jobTitle"].str.contains("business analyst")])


# Labels and counts
labels = [
    "Data Analyst",
    "Data Engineer",
    "Software Engineer",
    "Data Scientist",
    "Business Analyst",
]
sizes = [
    data_analyst_counts,
    data_engineer_counts,
    swe_counts,
    data_scientist_counts,
    business_analyst_counts,
]

# Filter out categories with zero counts
filtered_labels = []
filtered_sizes = []
for label, size in zip(labels, sizes):
    if size != 0:
        filtered_labels.append(label)
        filtered_sizes.append(size)

# Plotting the pie chart
fig, ax = plt.subplots()
ax.pie(
    filtered_sizes,
    labels=filtered_labels,
    autopct="%1.1f%%",
    startangle=140,
    colors=plt.cm.Paired.colors,
)
ax.axis("equal")  # Equal aspect ratio ensures that pie is drawn as a circle.
st.pyplot(fig)
