import requests, logging, os, pandas as pd
from helpers import wrangle_columns, write_to_csv
from dotenv import load_dotenv

from multiprocessing import Pool

import sys
sys.path.append('')

from configurations.config import MOVIES_PATH, NUMBER_OF_PARALLEL_PROCESSES, MOVIES_PER_YEAR

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("TMDB")
BASE_URL = 'https://api.themoviedb.org/3'

def make_req(year, page=1, per_page=20):
    endpoint = '/discover/movie'
    params = {
        'api_key': API_KEY,
        'primary_release_year': year,
        'page': page,
        'per_page': per_page,
    }

    response = requests.get(BASE_URL + endpoint, params=params)
    data = response.json()

    if 'results' in data:
        return data['results']
    else:
        return []
    
def get_movies(year):
    movies_frame = pd.DataFrame()

    for page in range(1, (MOVIES_PER_YEAR // 20) + 1):
        logger.info(f"Collecting { MOVIES_PER_YEAR } Movies released in { year } (Page: { page })")

        movies = make_req(year, page=page)
        movies_frames = [pd.DataFrame(m) for m in movies]
        
        for frame in movies_frames:
            movies_frame = pd.concat([movies_frame, frame])
    
    logger.info(f"Done: { movies_frame.shape[0] } Movies released in { year } collected")

    return movies_frame

def update_movies(years: list):
    with Pool(processes=NUMBER_OF_PARALLEL_PROCESSES) as pool:
        movie_frames = pool.map(get_movies, years)
    movies = pd.concat(movie_frames)
    movies = wrangle_columns(movies)

    write_to_csv(movies, MOVIES_PATH)
    return movies

if __name__ == "__main__":
    years = [i for i in range(1940, 2024)]
    movies = update_movies(years)
