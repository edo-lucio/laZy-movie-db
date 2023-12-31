import re
import os
import pandas as pd

from multiprocessing import Pool

import sys
sys.path.append('')

from configurations.config import  NUMBER_OF_PARALLEL_PROCESSES

def remove_substring_between_chars(input_string, start_char, end_char):
    pattern = re.escape(start_char) + '.*?' + re.escape(end_char)
    result = re.sub(pattern, '', input_string)
    return result
 
def wrangle_columns(df):
    df["year"] = pd.to_datetime(df["release_date"], format='%Y-%m-%d').dt.year
    df = df.reset_index(drop=True)

    return df

def convert_star_ratings(rating):
    stars_count = len([i for i in rating if i == "★"])
    half_star = len([i for i in rating if i == "½"])/2

    return stars_count + half_star or None

def process_review(text):
    text = remove_substring_between_chars(text, "<", ">")
    text = text.rstrip()
    pattern = re.compile('[^a-zA-Z0-9 ]+')
    text = re.sub(pattern, " ", text)

    return text

def multiprocess(function, df, **kwargs):
    print(*kwargs)

    with Pool(processes=NUMBER_OF_PARALLEL_PROCESSES) as pool:
        results = pool.map(function, df.itertuples(), kwargs)

    new_df = pd.concat(results)
    return new_df

def write_to_csv(df, path):
    file_exists = os.path.isfile(path)

    if not file_exists or os.stat(path).st_size == 0:
        df.to_csv(path, mode="w", index=False) 
    else:
        df.to_csv(path, mode="a", index=False, header=False)
