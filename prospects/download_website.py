import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import os 

import pandas as pd 

from utils.scraping_utils import get_soup_for_url, remove_html_tags
from utils.io import read_json, write_json
from utils.arango_utils import aql_query, connect_to_collection, insert_document, connect_to_mim_database

import hashlib

def get_website_urls(collection="Prospects", db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR p IN {collection}
        FILTER p.website != NULL
        FILTER NOT HAS(p, "website_content")
        RETURN {{
            _key: p._key,
            website: p.website
        }}
    '''

    return aql_query(db, query)

def main():

    db = connect_to_mim_database()

    collection_name = "Members"

    collection = connect_to_collection(collection_name, db)

    websites = get_website_urls(collection_name, db=db)

    # source = "competitors"
    # region = "yorkshire"
    # output_dir = f"{source}/{region}"

    # prospects_df_filename = os.path.join(output_dir, f"{region}_{source}_filtered_by_website.csv")
    # prospects_df = pd.read_csv(prospects_df_filename, index_col=0)

    # website_dir = os.path.join(output_dir, "website_content")
    # os.makedirs(website_dir, exist_ok=True)

    # output_file = os.path.join(website_dir, "all_content.json")

    # if os.path.exists(output_file):
    #     all_content = read_json(output_file)

    # else:
    #     all_content = {}

    # for company_name, website in prospects_df["website"].items():
    for doc in websites:
        _key = doc["_key"]
        website = doc["website"]
        # if company_name in all_content: continue

        print ("COMPANY NAME", _key)
        print ("WEBSITE", website)

        r, status_code = get_soup_for_url(website, return_request=True)
        if status_code != 200:
            continue
        website_content = r.text
        h = hashlib.md5(website_content.encode()).hexdigest()

        website_content = remove_html_tags(website_content)
        website_content = website_content.replace("/n", " ")

        document = {
            "_key": _key,
            "website_content": website_content,
            "website_hash": h,
        }

        insert_document(db, collection, document, verbose=True)

        # if company_name in all_content:
        #     if not isinstance(all_content[company_name], list):
        #         all_content[company_name] = [all_content[company_name]]
        # else:
        #     all_content[company_name] = []
        

        # if len(all_content[company_name]) == 0 or all_content[company_name][-1]["hash"] != h:
        #     print ("ADDING", company_name)
        #     content = {
        #         "content": r.text,
        #         "hash": h,
        #         "time": str(datetime.now()),
        #     }
        #     # print ("CONTENT", content)
        #     all_content[company_name].append(content)
        #     write_json(all_content, output_file)


if __name__ == "__main__":
    main()
