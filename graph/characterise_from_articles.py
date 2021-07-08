import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir,)))

import os 

from sklearn.feature_extraction.text import TfidfVectorizer

import re
import pandas as pd
from nltk.stem import PorterStemmer

from graph.arango_utils import connect_to_mim_database, aql_query
from utils.io import write_json

# init stemmer
porter_stemmer=PorterStemmer()

def my_stemming_preprocessor(text):
    
    text = text.lower() 
    text = re.sub("\\W", " ", text) # remove special chars
    
    # normalize certain words
#     text = re.sub("\\s+(in|the|all|for|and|on)\\s+"," _connector_ ",text) 
    
    # stem words
    words = re.split("\\s+",text)
    stemmed_words = [porter_stemmer.stem(word=word) for word in words]
    return ' '.join(stemmed_words)



def main():


    # host = "3.8.143.152"
    host = "127.0.0.1"
    port = 8529
    # username="root"
    username = "david"
    password = "c423612k"
    db_name = "mim" 

    db = connect_to_mim_database(
        host=host, 
        port=port, 
        username=username, 
        password=password, 
        db=db_name)

    # collection = connect_to_collection("Members", db,)

    # PULL ARTICLES FROM DATABASE
    get_articles_query = f'''
    FOR m IN Members
        FILTER m.articles != NULL
        RETURN {{
            "_id": m._id,
            "articles": m.articles,
        }}
    '''
    member_to_article = aql_query(db, get_articles_query)
    member_to_article = {record["_id"]: record["articles"] 
        for record in member_to_article}

    # member_to_id = name_to_id(db, "Members", "member_name")

    # article_filename = os.path.join("data_for_graph", "all_news_articles.csv")
    # news_articles = pd.read_csv(article_filename, index_col=0)

    # news_articles = news_articles.loc[news_articles["member_name"].isin(member_to_id)]

    # member_to_article = dict()

    # for _,  row in news_articles.iterrows():
    #     member_name = row["member_name"]
    #     content = row["content"]
    #     if pd.isnull(content):
    #         continue
    #     if member_name not in member_to_article:
    #         member_to_article[member_name] = []
    #     member_to_article[member_name].append(content)    

    corpus = {
        member: "\n".join(articles)
            for member, articles in member_to_article.items()
    }

    write_json(corpus, "data_for_graph/corpus.json")

    members = sorted(corpus)
    v = TfidfVectorizer(
        min_df=.2, max_df=0.8, 
        #binary=True,
        preprocessor=my_stemming_preprocessor, ngram_range=(1, 2),
        )
    print ("fitting TFIDF vectoriser")
    v.fit([corpus[m] for m in members])
    tfidf_vector = v.transform([corpus[m] for m in members])

    # ADD VECTOR TO DOCUMENT?
    # for member, vector in zip(members, tfidf_vector):
    #     member_id = member_to_id[member]

    #     document = {
    #         "_id": member_id,
    #         "tfidf_vector": vector.A[0].tolist(),
    #     }

    #     insert_document(db, collection, document, upsert_key="_id")

    article = member_to_article[members[10]][0]

    article_query = v.transform([article])
    print ("query shape", article_query.shape)

    from sklearn.metrics.pairwise import cosine_similarity

    similarities = cosine_similarity(article_query, tfidf_vector)

    print (similarities.shape)

    print (similarities.argsort(axis=-1)[:, -10:])

if __name__ == "__main__":
    main()