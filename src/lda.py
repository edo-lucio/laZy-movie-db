from gensim import corpora
from gensim.models import LdaModel

import pyLDAvis
import pyLDAvis.gensim_models as gensimvis

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

import sys
sys.path.append('')

import os
import csv

import pandas as pd
from configurations.config import DATA_PATH

from helpers import read_files

NUM_TOPICS = 2

def pre_process(document):
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    
    tokenized_docs = [word_tokenize(doc) for doc in document if pd.notna(doc)]
    
    tokenized_docs = [[token for token in doc if token.isalnum() and token[0].islower() and token not in stop_words] for doc in tokenized_docs]

    return [[lemmatizer.lemmatize(token) for token in doc] for doc in tokenized_docs]

def create_dictionary(tokenized_docs):
    return corpora.Dictionary(tokenized_docs)

# # Create a Document-Term Matrix
def create_dtm(dictionary, tokenized_docs):
    return [dictionary.doc2bow(doc) for doc in tokenized_docs]

def topic_model(document):
    token_documents = pre_process(document)
    dictionary = create_dictionary(token_documents)
    dtm = create_dtm(dictionary, token_documents)

    lda_model = LdaModel(dtm, num_topics=NUM_TOPICS, id2word=dictionary, passes=15)

    prepared_data = gensimvis.prepare(lda_model, dtm, dictionary)
    pyLDAvis.save_html(prepared_data, "a.html")

    topics_dict = {i: lda_model.show_topic(i) for i in range(NUM_TOPICS)}

    return topics_dict

def update_topics(movies, reviews):
    movies = movies[["id", "title", "original_title", "overview", "year"]]
    movies = movies.drop_duplicates()
    reviews = reviews[["id", "review"]]
    text_df = pd.merge(movies, reviews, on='id', how='inner')
    text_df = text_df.groupby('id').apply(lambda group: ' '.join(map(str, group.values.flatten()))).reset_index()

    topics = pd.DataFrame()

    path = "./data/topics.csv"
    
    for index, id, text in text_df.iloc[0:].itertuples():
        topics = topic_model([text])

        write_header = not os.path.isfile(path) or os.stat(path).st_size == 0

        with open(path, 'a', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            if write_header:
                csv_writer.writerow(['id', 'word', 'weight'])
            
            for topic_id, topic in topics.items():
                for word, weight in topic:
                    csv_writer.writerow([id, word, weight])

if __name__ == "__main__":
    data_frames = read_files(DATA_PATH)

    movies = data_frames["movies"]
    reviews = data_frames["reviews"]

    update_topics(movies, reviews)


    # topics.to_csv("./data/topics.csv")
        
# def test_lda(document)
#     # new_document = "Your new document here"
#     # new_tokenized_doc = [lemmatizer.lemmatize(token) for token in word_tokenize(new_document.lower()) if token.isalnum() and token not in stop_words]
#     # new_bow = dictionary.doc2bow(new_tokenized_doc)

#     # # Infer topics for the new document
#     # topics = lda_model[new_bow]
#     # print(topics)
















