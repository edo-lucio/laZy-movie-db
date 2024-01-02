from bs4 import BeautifulSoup

import requests
import pandas as pd

import sys
sys.path.append('')

from random import randint
from src.helpers import convert_star_ratings, process_review, write_to_csv

import time, random, string as s
import logging

from multiprocessing import Pool

import sys
sys.path.append('..')

from configurations.config import MOVIES_PATH, NUMBER_OF_PARALLEL_PROCESSES, REVIEWS_PATH, NUMBER_OF_REVIEWS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = 'https://letterboxd.com/film/{title}{year}/reviews/by/activity/{page_number}'

def format_url(title, year="", page_number=""):
    if page_number == 0:
        return URL.format(title=title, year=year, page_number="")
    else:
        return URL.format(title=title, year=year, page_number=f"page/{page_number}")

def send_request(url):
    review = requests.get("https://letterboxd.com"+url)

    if review.status_code == 200:
        return review.text
    elif review.status_code != 404:
        return send_request(url)

def scrape_review(url):
    time.sleep(randint(1, 4))
    response = requests.get(url)
    
    if response.status_code == 200:
        page = BeautifulSoup(response.text, "html.parser")
        data = page.find("section", class_= "viewings-list")
        data = data.find_all("li", class_= "film-detail")
        reviews = []

        for item in data:
            review = {}
            details = item.find("div", {"class": ["film-detail-content"], })

            try:
                reviewer = item.find("strong", class_= "name").text
                review["reviewer"] = reviewer
            except:
                review['reviewer'] = None

            try:
                stars = item.find("span", {"class": ['rating'], }).text
                review["rating"] = convert_star_ratings(stars)
            except:
                review['rating'] = None

            try:
                element = details.find("div", {"class": ['body-text'], })
                url = element.get("data-full-text-url")
                text = send_request(url)
                review["review"] = process_review(text)
            except:
                review['review'] = None

            reviews.append(review)

        return reviews
    
    else:
        raise Exception(response.status_code)

def collect_movie_reviews(title, year, id, number_of_reviews):
    logger.info(f"Collecting { number_of_reviews } reviews from { title } { year }")

    reviews = []
    page_number = 0

    title_name = title.lower().translate(str.maketrans('', '', s.punctuation)).replace(" ", "-")
    release_date = f"-{year}"

    while len(reviews) < number_of_reviews:
        url = format_url(title_name, release_date, page_number)

        try:
            new_reviews = scrape_review(url)
            reviews += new_reviews
        except:
            url = format_url(title=title_name, page_number=page_number)
            new_reviews = scrape_review(url)
            reviews += new_reviews

        if len(new_reviews) < 12: # max number of reviews per page
            break

        page_number += 1
    
    logger.info(f"Collected { len(reviews) } reviews for { title } { year }: New URL { url }")
    reviews_df = pd.DataFrame()

    reviews_df["id"] = [id] * len(reviews)
    reviews_df["title"] = [title] * len(reviews)
    reviews_df["year"] = [year] * len(reviews)
    reviews_df["review"] = [rev["review"] for rev in reviews]
    reviews_df["rating"] = [rev["rating"] for rev in reviews]
    reviews_df["reviewer"] = [rev["reviewer"] for rev in reviews]

    write_to_csv(reviews_df, REVIEWS_PATH)
    
    return reviews_df

def collect_movie_reviews_wrapper(movie):
    print(movie)
    return collect_movie_reviews(movie["title"], movie["year"], movie["id"], NUMBER_OF_REVIEWS)

def update_reviews(movies):
    with Pool(processes=NUMBER_OF_PARALLEL_PROCESSES) as pool:
        movies = [{ "title": x[1], "year": x[2], "id": x[3] } for x in movies.itertuples()]
        review_frames = pool.map(collect_movie_reviews_wrapper, movies)

    reviews = pd.concat(review_frames)
    return reviews

if __name__ == "__main__":
    random.seed(12)
    movies = pd.read_csv(MOVIES_PATH)
    movies = movies.sample(frac=1)
    movies = movies[['title', 'year', 'id']].drop_duplicates()

    reviews = update_reviews(movies)
