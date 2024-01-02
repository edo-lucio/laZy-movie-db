from helpers import read_files
import numpy as np

import sys
sys.path.append('')

from configurations.config import DATA_PATH

from pymongo.mongo_client import MongoClient
import certifi

from dotenv import load_dotenv
import os, logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACC_DB = os.getenv("DB_ACC")

client = MongoClient(ACC_DB, ssl_ca_certs=certifi.where())
db = client.get_database("movies_db")
collection = db.get_collection("movies_list")

def create_movie_document(movies, id):
   movie = movies[movies["id"] == id].to_dict(orient='records')
   movie_document = {key: list(set([d[key] for d in movie]))[0] if len(set([d[key] for d in movie])) == 1 else list(set([d[key] for d in movie])) for key in movie[0]}

   return movie_document

def create_review_document(reviews, id):
   reviews_document = reviews[reviews["id"] == id][["review", "reviewer", "rating"]].to_dict(orient='records')
   return reviews_document

def insert_documents(movies, reviews):
   ids = min([movies["id"], reviews["id"]], key=len)
   documents = []

   for movie_id in ids:
      movie_document  = create_movie_document(movies, movie_id)
      movie_reviews = create_review_document(reviews, movie_id)
      movie_document["reviews"] = movie_reviews
      
      documents.append(movie_document)
      # collection.insert_one(movie_document)

   collection.insert_many(documents)
   
def update_db():
   data = read_files(DATA_PATH)
   movies = data["movies"]
   reviews = data["reviews"]

   insert_documents(movies, reviews)

if __name__ == "__main__":
   update_db()



































