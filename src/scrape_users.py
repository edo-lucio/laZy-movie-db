from bs4 import BeautifulSoup

import requests
import pandas as pd

from random import randint
from helpers import convert_star_ratings, process_review, write_to_csv, remove_substring_between_chars

import time, random, string as s
import logging

from multiprocessing import Pool

import sys
sys.path.append('')

from configurations.config import MOVIES_PATH, NUMBER_OF_PARALLEL_PROCESSES, REVIEWS_PATH, NUMBER_OF_REVIEWS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = "https://letterboxd.com/{user_name}/films/{page_number}"

def format_url(user_name, page_number=""):
    if page_number == 0:
        return URL.format(user_name=user_name, page_number="")
    else:
        return URL.format(user_name=user_name, page_number=f"page/{page_number}")

def send_request(url):
    review = requests.get("https://letterboxd.com"+url)

    if review.status_code == 200:
        return review.text
    elif review.status_code != 404:
        print(review.status_code)
        return send_request(url)

def scrape_year(url):
    time.sleep(randint(1, 4))
    response = requests.get(url)

    if response.status_code == 200:
        page = BeautifulSoup(response.text, "html.parser")

        return page

def scrape_page(url):
    time.sleep(randint(1, 4))
    response = requests.get(url)

    if response.status_code == 200:
        page = BeautifulSoup(response.text, "html.parser")
        data = page.find("ul", class_= "poster-list -p70 -grid film-list clear")
        movies = data.find_all("li", class_= "poster-container")

        return movies

def collect_users_movies(user_name, max_movies=12):
    page_number = 0
    url = format_url(user_name, page_number)
    user_movies = pd.DataFrame({"user": user_name, "title": [], "rating": [], "year": []})
    movies = scrape_page(url)

    while len(movies) >= 72 and user_movies.shape[0] <= max_movies:
        titles = [movie.find('div', {'class': 'film-poster'})['data-film-slug'] for movie in movies]
        titles = [title.replace("-", " ") for title in titles]

        ratings = [remove_substring_between_chars(str(movie.select_one('span[class*="rated-"]')), "<", ">") for movie in movies]
        ratings = [convert_star_ratings(rating) for rating in ratings]

        urls = [movie.find('div', {'class': 'film-poster'})['data-target-link'] for movie in movies]
        years = [scrape_year("https://letterboxd.com"+url) for url in urls]

        print(years[0])
        
        return
        
        user_movies = pd.concat([user_movies, pd.DataFrame({"user": user_name, "title": titles, "rating": ratings, "year":years})])
        
        page_number += 1
        movies = scrape_page(url)

    write_to_csv(user_movies, "./data/users.csv")

if __name__ == "__main__":
    collect_users_movies("kurstboy")