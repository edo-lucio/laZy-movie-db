import pandas as pd

from pymongo.mongo_client import MongoClient
import certifi

from dotenv import load_dotenv
import os, logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ACC_DB = os.getenv("DB_ACC")

def create_movie_document(movies, id):
   movie = movies[movies["id"] == id].to_dict(orient='records')
   movie_document = {key: list(set([d[key] for d in movie]))[0] if len(set([d[key] for d in movie])) == 1 else list(set([d[key] for d in movie])) for key in movie[0]}

   return movie_document

def create_review_document(reviews, id):
   reviews_document = reviews[reviews["id"] == id].to_dict(orient='records')

   return reviews_document

def create_db(collection):
   movies = pd.read_csv("./movies_df.csv", dtype={"id": str})
   reviews = pd.read_csv("./movies_reviews.csv", on_bad_lines="skip", dtype={"id": str})

   reviews = reviews[["id", "rating", "review", "reviewer"]]
   movies = movies.drop(columns=["Unnamed: 0"])

   ids = reviews["id"].unique()
   documents = []

   for movie_id in ids:
      try:
         movie_document  = create_movie_document(movies, movie_id)
         movie_reviews = create_review_document(reviews, movie_id)

         movie_document["reviews"] = movie_reviews
         movie_document["adult"] = bool(movie_document["adult"])
         
         documents.append(movie_document)

      except Exception as e:
         print(f"{e}")
         continue

   collection.insert_many(documents)
   
def main():
   client = MongoClient(ACC_DB, ssl_ca_certs=certifi.where())
   db = client.get_database("movie_db")
   collection = db.get_collection("movie_list")

   create_db(collection)

   logging.info("# done!")

if __name__ == "__main__":
   main()