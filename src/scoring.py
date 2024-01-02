from gensim.models import KeyedVectors
from sklearn.neighbors import NearestNeighbors

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk
from nltk import pos_tag

import pandas as pd
import numpy as np

import sys
sys.path.append('')

from configurations.config import MOVIES_PATH

from string import punctuation

# Load pre-trained Word2Vec or FastText model
# Replace 'path_to_your_model' with the actual path to your pre-trained model file
# You can download pre-trained models from Gensim or other sources
# Example for Word2Vec:
# model = Word2Vec.load('path_to_your_model')
# Example for FastText:
# model = FastText.load('path_to_your_model')

model_path = 'GoogleNews-vectors-negative300.bin'
word2vec_model = KeyedVectors.load_word2vec_format(model_path, binary=True, limit=100000)

print("loaded")

def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return 'a'  # adjective
    elif treebank_tag.startswith('V'):
        return 'v'  # verb
    elif treebank_tag.startswith('N'):
        return 'n'  # noun
    elif treebank_tag.startswith('R'):
        return 'r'  # adverb
    else:
        return 'n'  # default to noun

def preprocess_text(text):
    tokens = word_tokenize(text.lower())

    stop_words = set(stopwords.words('english') + list(punctuation))
    tokens = [token for token in tokens if token not in stop_words]

    lemmatizer = WordNetLemmatizer()
    tokens = [lemmatizer.lemmatize(token, pos=get_wordnet_pos(tag))
              for token, tag in pos_tag(tokens)]
    
    return tokens

def get_nearest_words(input_words):
    input_words = [preprocess_text(word) for word in input_words]
    input_words = list(np.concatenate(input_words))
    k_neighbors = 10

    input_vectors = [word2vec_model[word] for word in input_words if word in word2vec_model.index_to_key]

    knn_model = NearestNeighbors(n_neighbors=k_neighbors, metric='cosine')
    knn_model.fit(word2vec_model.vectors)

    distances, indices = knn_model.kneighbors(input_vectors)
    nearest_words = []

    for i, input_word in enumerate(input_words):
        for j, idx in enumerate(indices[i]):
            neighbor_word = word2vec_model.index_to_key[idx]
            nearest_words.append(neighbor_word)
        print()

    return nearest_words

def score(words_vector):
    words_vector = get_nearest_words(words_vector)
    topics = pd.read_csv("./data/topics.csv")
    topics_grouped = topics.groupby("id")

    movie_score = pd.DataFrame({"id": topics["id"].unique()})

    for id, group in topics_grouped:
        intersection = set(group["word"]).intersection(set(words_vector))
        movie_score.loc[movie_score['id'] == id, 'weight'] = topics[(topics["id"] == int(id)) & topics["word"].isin(intersection)]["weight"].sum()

    return movie_score.sort_values(by="weight", ascending=False)

def top_k_similiar_movies(input_words, k):
    scores = score(input_words)
    movies = pd.read_csv(MOVIES_PATH)

    result = pd.merge(scores, movies, how="inner", on="id")
    titles = result["title"].unique()[:k]
    
    print(titles)

if __name__ == "__main__":
    top_k_similiar_movies(["existential"], 10)










