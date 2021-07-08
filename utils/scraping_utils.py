import requests

from bs4 import BeautifulSoup

import time
from requests.exceptions import TooManyRedirects

from urllib3.connection import ConnectionError

import re

sleep = .1

def remove_html_tags(text):
    return BeautifulSoup(text, features="html.parser").get_text()

def process_figure_string(figure_string_):
    
    figure_string = figure_string_
    
    if (figure_string is None 
        or "%" in figure_string 
        or "£" not in figure_string):
        return figure_string
    
    assert "£" in figure_string, figure_string
        
    if "k" in figure_string:
        multiplier = 1000
    elif "m" in figure_string:
        multiplier = 1000000
    elif "b" in figure_string:
        multiplier = 1000000000
    else:
        multiplier = 1
    
    figure_string = re.sub(f"(£|[a-z])", "", figure_string)
    try:
        return float(figure_string) * multiplier
    except ValueError as e:
        print (e, figure_string_, figure_string)

def strip_text(s):
    return re.sub( r"(\n|[ ]{2,})" , "", s)

def get_soup_for_url(url, sleep=1., timeout=5, retries=3, return_request=False):
    print ("getting soup for url:", url)

    # proxies = {"http": "http://10.10.1.10:3128",
    #        "https": "http://10.10.1.10:1080"}

    for _ in range(retries):
        try:
            page = requests.get(url, timeout=timeout, 
            # proxies=proxies
            )
            print ("Received status code:", page.status_code)
            time.sleep(sleep)
            status_code = page.status_code
            if status_code != 200:
                print ("STATUS CODE FAIL", status_code)
                continue
            if return_request:
                return page, status_code
            else:
                return BeautifulSoup(page.content, 'html.parser'), status_code

        except (ConnectionError, 
            requests.ConnectTimeout, 
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
            ConnectionResetError,
            TooManyRedirects) as e:
            print ("SOUP ERROR", e)
            pass
    print ("MAX RETRIES")
    # raise Exception()
    return None, None 
    

def identify_postcode(address):
    postcode_pattern = r"[A-Z]{1,2}[0-9]{1,2}[A-Z]?[ ]?[0-9][A-Z]{2}"
    matches = re.findall(postcode_pattern, address)
    if len(matches) == 1:
        postcode = matches[0]
        return postcode
    else:
        return None

def get_postcode_prefix(postcode):
    # print ("finding postcode prefix for postcode", postcode)
    if postcode is None: 
        return None
    match = re.match(r"^[A-Z]+", postcode)
    if match is None:
        return None
    return match.group(0)

def clean_directors_name(name):
    # remove title
    name = re.sub(r'(^\w{2,3} )', r'', name)
    # remove middle name
    return re.sub(r" [A-Z][a-z]* ", r" ", name)