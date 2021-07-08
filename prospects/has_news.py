

from random import weibullvariate
import sys
import os.path

from requests.models import Response
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import os 
import re

import pandas as pd 

from urllib.parse import urljoin, urlparse

from utils.scraping_utils import get_soup_for_url
from utils.io import read_json, write_json

def main():
    
    source = "competitors"
    region = "midlands"
    output_dir = f"{source}/{region}"

    prospects_df_filename = os.path.join(output_dir, f"{region}_{source}_filtered_by_website.csv")
    prospects_df = pd.read_csv(prospects_df_filename, index_col=0)

    website_dir = os.path.join(output_dir, "website_content")
    os.makedirs(website_dir, exist_ok=True)

    output_file = os.path.join(website_dir, "new_pages.json")

    if os.path.exists(output_file):
        all_content = read_json(output_file)

    else:
        all_content = {} 

    for company_name, website in prospects_df["website"].items():


        if company_name in all_content:
            continue

        parsed_uri = urlparse(website)
        base = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

        soup, status_code = get_soup_for_url(website)
        if status_code != 200:
            continue

        news_links = soup.find_all("a", string=re.compile('news', re.IGNORECASE))
        if news_links is None:
            news_links = []
        print (news_links)
        for news_link in news_links:
            print (news_link, news_link.hasattr("href"))

        news_links = {
            urljoin(base, news_link["href"])
            for news_link in news_links
            if news_link.hasattr("href") 
                and re.match(r"^https?://", urljoin(base, news_link["href"]))
        }

        if len(news_links) == 0:

            # search for onclick
            onclick_elements = {e["onclick"] 
                for e in soup.select("[onclick^='location.href='][onclick*='news' i]")}
            onclick_elements = map(lambda s: s.split("'")[1], onclick_elements)

            news_links = {urljoin(base, onclick_element)
                for onclick_element in onclick_elements
                if re.match(r"^https?://", urljoin(base, onclick_element))}

        

        all_content[company_name] = sorted(news_links)
        write_json(all_content, output_file)

if __name__ == "__main__":
    main()