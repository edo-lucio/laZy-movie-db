from get_movies import update_movies
from scrape_reviews import update_reviews

YEAR_RANGE = [i for i in range(2024, 2025)] # next year ;)
MAX_MOVIES_PER_YEAR = 200

def main():
    new_movies = update_movies(YEAR_RANGE, MAX_MOVIES_PER_YEAR)
    new_reviews = update_reviews(new_movies)

    # store into db

if __name__ == "__main__":
    main()


    
