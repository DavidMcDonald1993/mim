import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from urllib.error import URLError


from collections import deque

import nltk 
import re

import pandas as pd

from boilerpipe.extract import Extractor

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer

from http.client import InvalidURL, RemoteDisconnected
from socket import timeout

from time import sleep

from utils.mysql_utils import mysql_query
from utils.scraping_utils import get_soup_for_url
from utils.io import write_json, read_json

def get_websites_from_mim_database(limit=None):

    query = f'''
    SELECT name, website 
    FROM members
    {f"LIMIT {limit}" if limit is not None else ""}
    '''

    return mysql_query(query)

def website_dfs(base_url, max_depth=1):
    assert base_url is not None
    visited = set([base_url])
    dq = deque([[base_url, "", 0]])

    while dq:
        base, path, depth = dq.popleft()
        assert base is not None
    
        if depth < max_depth:
            soup = get_soup_for_url(base + path)
            if soup is None:
                continue

            for link in soup.find_all("a"):
                href = link.get("href")
                if href is None:
                    continue
                if href == "":
                    continue
                if "@" in href: 
                    continue
                if href.endswith(".pdf") or href.endswith(".docx") or href.endswith(".png"): 
                    continue
                if "javascript" in href: 
                    continue
                if "tel:" in href:
                    continue
                # assert href != ""

                if href not in visited:
                    if href.startswith("http"):
                        dq.append([href, "", depth + 1])
                        visited.add(href)
                    else:
                        dq.append([base, href, depth + 1])
                        visited.add(base + href)
            # except:
            #     pass

    return visited

def get_content_from_page(url):
    try:
        print ("getting content for url", url)
        extractor = Extractor(
            extractor='LargestContentExtractor', url=url)
        return str(extractor.getText())
    except (URLError, ValueError, timeout, 
        InvalidURL, RemoteDisconnected, LookupError, ConnectionResetError) as e:
        print (e)
        return None

def stemming_preprocessor(text):
    
    text = text.lower() 
    text = re.sub("\\W"," ",text) # remove special chars
    text = re.sub("\\s+(in|the|all|for|and|on)\\s+", 
        "", text)
        # " _connector_ ", text) # normalize certain words
    
    # stem words
    # init stemmer
    porter_stemmer = nltk.stem.PorterStemmer()
    words = re.split("\\s+",text)
    stemmed_words = [porter_stemmer.stem(word=word) 
        for word in words]
    return ' '.join(stemmed_words)

def keyword_vectoriser(corpus, min_df=.1, max_df=.85):

    print ("fitting CountVectorizer for corpus size:", len(corpus),
        "using min_df =", min_df, "and max_df =", max_df)

    vectoriser = CountVectorizer(
        ngram_range=(1, 3),
        preprocessor=stemming_preprocessor,
        min_df=min_df, max_df=max_df, binary=True)
    
    vectoriser.fit(corpus)

    vectors = vectoriser.transform(corpus)
    vocabulary = vectoriser.vocabulary_

    return vectors, vocabulary

def main():


    # from database
    # websites = get_websites_from_mim_database(limit=None)

    # from good search
    websites = (
        (company, results[0])
        for company, results in 
            read_json("company_websites_from_google.json").items()
    )

    output_file = "website_content.json" 
    if os.path.exists(output_file):
        website_content = read_json(output_file)
    else:
        website_content = dict()

    max_depth = 1
    for member, home_page in websites:
        if member in website_content:
            continue
        if home_page is None:
            continue
        visited = website_dfs(home_page, max_depth=max_depth)
        print (member, f"number {max_depth}-hop pages:", len(visited))
        all_page_contents = []
        for page in filter(lambda x: x, visited):
            assert page is not None
            page_content = get_content_from_page(page)
            if page_content is not None:
                all_page_contents.append(page_content)
        website_content[member] = "/n".join(all_page_contents)
        write_json(website_content, output_file)
        print ()

    sorted_members = sorted(website_content)
    member_to_id = {member: i 
        for i, member in enumerate(sorted_members)}
    write_json(member_to_id, "member_to_id.json")

    corpus = [website_content[member] 
        for member in sorted_members]

    corpus_vectors, vocabulary = keyword_vectoriser(corpus)

    write_json(vocabulary, "vocabulary.json")

if __name__ == "__main__":
    main()