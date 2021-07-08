
import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import os

import pandas as pd

from urllib.parse import quote

from googlesearch import search

from utils.scraping_utils import get_soup_for_url
from utils.io import write_json, read_json

from googleapiclient.discovery import build   #Import the library
api_key = "AIzaSyDJhVAa2DEKsPVl8Wpx-RT4zsYG0nF9OvQ"
cse_id = "61a037eeb7da7fe5f"

def search_google(company_name, num_results=1, pause=2):
    print ("searching Google for company", company_name)

    '''
    maybe switch API: https://towardsdatascience.com/current-google-search-packages-using-python-3-7-a-simple-tutorial-3606e459e0d4
    '''

    # # drop limited/ltd suffix
    # print (company_name)
    # company_name = " ".join((
    #     term.lower()
    #     for term in company_name.split(" ")
    #     if term.lower() not in {"limited", "ltd"}
    # ))
    # print (company_name)


    results = []

    for url in search(company_name,        # The query you want to run
        tld = 'com',  # The top level domain
        lang = 'en',  # The language
        num = 10,     # Number of results per page
        start = 0,    # First result to retrieve
        stop = num_results,  # Last result to retrieve
        pause = pause,  # Lapse between HTTP requests
        ):
        results.append(url)
    assert len(results) == num_results
    print ("search complete")
    return results

# def search_google( # quicker but order is not guarenteed
#     query, **kwargs):
#     query_service = build("customsearch", 
#                           "v1", 
#                           developerKey=api_key
#                           )  
#     query_results = query_service.cse().list(q=query,    # Query
#                                              cx=cse_id,  # CSE ID
#                                              **kwargs    
#                                              ).execute()

#     results = []
#     for result in query_results['items']:
#         results.append(result['link'])
#     return results 

def main():

    region = "yorkshire"
    output_dir = f"prospects/{region}"

    companies = pd.read_csv(
        # "./jupyter_notebooks/WM_manufacturers_from_companies_house.csv",
        # os.path.join(output_dir, f"{region}_competitors.csv"),
        os.path.join(output_dir, f"prospects_filtered.csv"),
        index_col=0)
    companies = companies.sort_index()

    output_file = os.path.join(output_dir, f"{region}_company_websites")
    output_file_json = f"{output_file}.json"
    output_file_csv = f"{output_file}.csv"


    if os.path.exists(output_file_json):
        company_websites = read_json(output_file_json)
    else:
        company_websites = dict()
    
    for i, company in companies.iterrows():

        # company_name = i
        company_name = company["CompanyName"]
        # post_code = company["RegAddress.PostCode"]

        if company_name in company_websites:
            # print("skipping", company_name)
            continue

        website_urls = search_google(company_name, 
            # num=10
            )
        company_websites[company_name] = website_urls
        write_json(company_websites, output_file_json)

        print (i, website_urls)
        print ()

    company_websites = pd.DataFrame(company_websites, index=["website"]).T
    company_websites.to_csv(output_file_csv)

if __name__ == "__main__":
    main()