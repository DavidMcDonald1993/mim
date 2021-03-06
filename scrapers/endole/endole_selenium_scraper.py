
import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))) 

from urllib.parse import quote

# import web driver
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException

from time import sleep

import pandas as pd 

import re

from dotenv import load_dotenv
load_dotenv()

import os

from utils.scraping_utils import (identify_postcode,
    get_postcode_prefix, process_figure_string)
from utils.geo import all_region_postcodes
from utils.selenium_utils import initialise_chrome_driver, get_url
from utils.io import read_json, write_json


def login(driver,
    email=os.getenv("ENDOLEEMAIL"),
    password=os.getenv("ENDOLEPASSWORD"),
    ):

    get_url(driver, 'https://suite.endole.co.uk/?login=Google')
    sleep(1)

    print ("logging in")

    print ("finding username")

    # locate email form by_class_name
    # username = driver.find_element_by_id("login_email")
    username = driver.find_element_by_id("identifierId")

    # send_keys() to simulate key strokes
    username.send_keys(email)
    username.send_keys(Keys.RETURN)
    sleep(3)

    print ("finding password")

    # locate password form by_class_name
    # password = driver.find_element_by_id("login_pass")
    # try:
    password_field = driver.find_element_by_css_selector('input[type="password"]')
    # except Exception:
    #     password = driver.find_element_by_css_selector('input[type="password"]')

    # send_keys() to simulate key strokes
    password_field.send_keys(password)
    password_field.send_keys(Keys.RETURN)


    # print ("finding log in button")

    # # locate submit button by_class_name
    # log_in_button = driver.find_element_by_class_name('blue-button')

    # # .click() to mimic button click
    # log_in_button.click()
    sleep(3)


def search_endole_with_selenium(driver, company_name, postcode, check_postcode):

    company_name_url = quote("+".join(company_name.split(" ")))
    url = f"https://suite.endole.co.uk/insight/search/?q={company_name_url}"

    # driver.get(url)
    get_url(driver, url)
    # sleep(1)

    postcode_prefix = get_postcode_prefix(postcode)

    try:
        results = driver.find_elements_by_css_selector("div[class='search-result']")
    except NoSuchElementException:
        return None, None

    for result in results:
        # check for post code
        try:
            header = result.find_element_by_css_selector("div[class='sr-header']")
        except NoSuchElementException:
            continue
        try:
            address = header.find_element_by_css_selector("span",)
        except NoSuchElementException:
            continue
        address = address.text 
        result_postcode = identify_postcode(address)
        if result_postcode is None:
            continue
        result_postcode_prefix = get_postcode_prefix(result_postcode)
        if result_postcode_prefix is None:
            continue
        if not check_postcode or (postcode_prefix is not None and postcode_prefix == result_postcode_prefix):
            link = header.find_element_by_css_selector("a[class='preloader']")
            return address, link.get_attribute('href')
        elif result_postcode_prefix in all_region_postcodes["midlands"]\
            or result_postcode_prefix in all_region_postcodes["yorkshire"]:
            print ("POSTCODE IN REGION", result_postcode)
            link = header.find_element_by_css_selector("a[class='preloader']")
            return address, link.get_attribute('href')

    print ("NO MATCHES")
    return None, None

def scrape_endole_with_selenium(driver, base_url):

    overview_url = base_url + "?page=overview"

    scraped_company_info = {}

    get_url(driver, overview_url)

    # company classification
    divs = driver.find_elements_by_css_selector("div[class='_item']")
    for div in divs:
        try:
            heading_div = div.find_element_by_css_selector("div[class='_heading']")
        except NoSuchElementException:
            continue
        if "Size" in heading_div.text:
            size_div = div.find_element_by_css_selector("div[class='-font-size-l']")
            if size_div is not None:
                scraped_company_info["endole_company_size_classification"] = size_div.text

    # financials
    try:
        financials = driver.find_elements_by_css_selector("div[class='financial-overview']")
    except NoSuchElementException:
        financials = None 

    if financials is not None:
        for financial in financials:
            header = financial.find_element_by_css_selector("span[class='t2']")
            header = header.text   
            figure = financial.find_element_by_css_selector("span[class='t1']")
            figure = figure.text 

            scraped_company_info.update({
                f"{header}_figure": process_figure_string(figure),
            })
            try:
                trend = financial.find_element_by_css_selector("div[class='trendingup']")
            except NoSuchElementException:
                try:
                    trend = financial.find_element_by_css_selector("div[class='trendingdown']")
                except NoSuchElementException:
                    trend = None
            if trend is not None:
                trend = trend.text
                assert "(" in trend
                trend, trend_change = trend.replace(")", "").split(" (")

                scraped_company_info.update({
                    f"{header}_trend": process_figure_string(trend),
                    f"{header}_trend_change": process_figure_string(trend_change),
            })
    else:
        print ("NO FINANCIAL INFORMATION")

    '''
    directors table
    '''
    people_contacts_url = base_url + "?page=people-contacts"
    get_url(driver, people_contacts_url)

    try:
        table_div = driver.find_element_by_id("ajax_people")
        table = table_div.find_element_by_css_selector("table")
    except NoSuchElementException:
        table = None

    if table is not None:

        table_rows = table.find_elements_by_css_selector('tr')
        l = []
        for tr in table_rows[1:]:  # skip first row since it is a header
            td = tr.find_elements_by_css_selector('td')
            row = [tr.text.rstrip() for tr in td]
            l.append(row)
        directors_table = pd.DataFrame(l, 
            columns=["name", "occupation", "period",] )
        
        # keep only active directors
        directors_table = directors_table.loc[
            directors_table["period"].map(
                lambda s: s.split(" ??? ")[1] == "Active")]

        scraped_company_info["directors"] = [
            {"name": row["name"].split("\n")[0], "occupation": row["occupation"]}
            for _, row in directors_table.iterrows()
            if row["occupation"] not in {"???", "None"}
        ]

    '''
    competition
    '''

    competition_url = base_url + "?page=competition"
    get_url(driver, competition_url)


    competition_elements = driver.find_elements_by_css_selector(
        "a") 

    pattern = r"^https://suite.endole.co.uk/insight/company/[0-9]+"

    scraped_company_info["competitors"] = []
    for competition_element in filter(
        lambda ce: re.match(pattern, ce.get_attribute("href")) and not ce.get_attribute("href").startswith(base_url),
        competition_elements):
        d = {}
        d["name"] = competition_element.text
        address = competition_element.find_element_by_xpath("..").text.split("\n")

        if address[-1].endswith(".com"):
            d["website"] = "www." + address[-1].lower()
            address = address[:-1]
        d["address"] = " ".join(address)

        scraped_company_info["competitors"].append(d)

    return scraped_company_info

def process_df(df, output_file, company_name_col=None, ):

    driver = None

    if os.path.exists(output_file):
            full_company_info = read_json(output_file)

    else:
        full_company_info = dict()

    for idx, company in df.iterrows():

        if company_name_col is None:
            company_name = idx
        else:
            company_name = company[company_name_col]


        if pd.isnull(company_name): 
            print ("skipping company", company_name, ": missing company name")
            continue
        if company_name in full_company_info:
            print ("company", company_name, "already processed")
            continue


        if driver is None:
            driver = initialise_chrome_driver()
            login(driver)

        print ("processing company", company_name)

        address, link = search_endole_with_selenium(
            driver, company_name, postcode=None, check_postcode=False)

        company_data_from_endole = {
            "endole_address": address,
            "endole_url": link,
        }
        if link is not None:
            scraped_company_data = scrape_endole_with_selenium(driver, link)
            company_data_from_endole.update(scraped_company_data)
        else:
            print("no results for company", company_name)


        full_company_info[company_name] = company_data_from_endole

        write_json(full_company_info, output_file)

        print()


def scrape_endole_for_members():

    output_dir = os.path.join("data_for_graph", "members" )

    for membership_level in {
        "Patron",
        "Platinum",
        "Gold",
        "Silver",
        "Bronze",
        "Digital",
        "Freemium",
    }:

        d = os.path.join(output_dir, membership_level)
        companies = pd.read_csv(
            os.path.join(d, f"{membership_level}_members_companies_house.csv"),
            index_col=0,
            )

        output_filename = os.path.join(d, 
            f"{membership_level}_endole.json")

        process_df(companies, output_filename)

def scrape_endole_for_prospects():
    

    output_dir = os.path.join("companies_house", )

    for region in {
        "midlands",
        "yorkshire",
    }:

        d = os.path.join(output_dir, region)
        companies = pd.read_csv(
            os.path.join(d, f"{region}_prospects_filtered_by_website.csv"),
            index_col=0,
            )

        output_filename = os.path.join(d, 
            f"{region}_endole.json")

        process_df(companies, output_filename, company_name_col="CompanyName")


def main():
    # scrape_endole_for_members()
    scrape_endole_for_prospects()

if __name__ == "__main__":
    main()