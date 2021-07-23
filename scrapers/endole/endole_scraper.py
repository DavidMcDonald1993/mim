import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import numpy as np
import pandas as pd
from urllib.parse import quote

import re
import os

from utils.scraping_utils import (get_soup_for_url, 
    get_postcode_prefix, identify_post_code, process_figure_string)
from utils.selenium_utils import initialise_chrome_driver, initialise_firefox_driver
from utils.geo import all_region_postcodes

sleep = .1

def unblock_ip_endole(url):
    print ("UNBLOCKING IP FOR URL", url)
    assert False
    unblock_url = "https://www.endole.co.uk/backend/unblock-ip/?URI=" + url

    # soup, status_code = get_soup_for_url(unblock_url)
    driver = initialise_chrome_driver()
    driver.get(unblock_url)
    # assert soup is not None

    # print (soup.prettify())

    # tickbox = soup.find({"class": "recaptcha-checkbox-checkmark"})
    tickbox = driver.find_element_by_class_name("recaptcha-checkbox-checkmark")
    # tickbox = soup.find("div", {"class": "g-recaptcha"})
    # print ("tickbox", tickbox)

    tickbox.click()
    raise Exception



def search_endole(company_name, post_code):

    '''
    https://suite.endole.co.uk/insight/search/?q=staubli
    '''
    company_name_url = quote("+".join(company_name.split(" ")))
    url = f"https://suite.endole.co.uk/insight/search/?q={company_name_url}"

    soup, status_code = get_soup_for_url(url, sleep=sleep)

    if status_code == 429:
        print ("IP BLOCK")
        unblock_ip_endole(url)
        raise Exception

    post_code_prefix = get_postcode_prefix(post_code)

    results = soup.find_all("div", {"class": 'search-result'})
    if results is None:
        return None, None
    for result in results:
        # check for post code
        header = result.find("div", {"class": "sr-header"})
        if header is None:
            continue
        address = header.find("span",)
        if address is None:
            continue
        address = address.text 
        result_post_code = identify_post_code(address)
        if result_post_code is None:
            continue
        result_post_code_prefix = get_postcode_prefix(result_post_code)
        if result_post_code_prefix is None:
            continue
        if post_code_prefix is not None and post_code_prefix == result_post_code_prefix:
            print ("POSTCODE MATCH", post_code, result_post_code,
                get_postcode_prefix(post_code), get_postcode_prefix(result_post_code))
            link = header.find("a", {"class": 'preloader'})
            return address, link["href"]
        elif result_post_code_prefix in all_region_postcodes["midlands"] or result_post_code_prefix in all_region_postcodes["yorkshire"]:
            link = header.find("a", {"class": 'preloader'})
            return address, link["href"]

        
    print ("NO MATCHES")
    return None, None

def scrape_endole(link):

    base_url = "https://suite.endole.co.uk/" + link 
    overview_url = base_url + "?page=overview"

    scraped_company_info = {}

    soup, status_code = get_soup_for_url(overview_url, sleep=sleep)
    if status_code == 200:

        # company classification
        divs = soup.find_all("div", {"class": "_item"})
        for div in divs:
            heading_div = div.find("div", {"class": "_heading"})
            if heading_div is None:
                continue
            if "Size" in heading_div.text:
                size_div = div.find("div", {"class": "-font-size-l"})
                if size_div is not None:
                    scraped_company_info["endole_company_size_classification"] = size_div.text

        # financials
        financials = soup.find_all("div", {"class": "financial-overview"})

        if financials is not None:
            for financial in financials:
                header = financial.find("span", {"class": "t2"})
                header = header.text   
                figure = financial.find("span", {"class": "t1"})
                figure = figure.text 

                scraped_company_info.update({
                    f"{header}_figure": process_figure_string(figure),
                })
                trend = financial.find("div", {"class": ["trendingup", "trendingdown"]})
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
    else:
        print ("NO FINANCIAL INFORMATION")

    '''
    directors table
    '''
    people_contacts_url = base_url + "?page=people-contacts"
    soup, status_code = get_soup_for_url(people_contacts_url, sleep=sleep)
    # assert status_code == 200

    if status_code == 429:
        print ("IP BLOCK")
        unblock_ip_endole(people_contacts_url)
        raise Exception

    table_div = soup.find(id="ajax_people")
    table = table_div.find("table")

    if table is not None:

        table_rows = table.find_all('tr')
        l = []
        for tr in table_rows[1:]:  # skip first row since it is a header
            td = tr.find_all('td')
            row = [tr.text.rstrip() for tr in td]
            l.append(row)
        directors_table = pd.DataFrame(l, 
            columns=["name", "occupation", "period",] )
        
        # keep only active directors
        directors_table = directors_table.loc[
            directors_table["period"].map(
                lambda s: s.split(" – ")[1] == "Active")]

        scraped_company_info["directors"] = [
            {"name": row["name"].split("\n")[0], "occupation": row["occupation"]}
            for _, row in directors_table.iterrows()
            if row["occupation"] not in {"–", "None"}
        ]

    '''
    competition
    '''

    competition_url = base_url + "?page=competition"
    soup, status_code = get_soup_for_url(competition_url, sleep=sleep)
    # assert status_code == 200

    if status_code == 429:
        print ("IP BLOCK")
        unblock_ip_endole(competition_url)
        raise Exception


    competition_elements = soup.find_all("a", 
        {"href": re.compile('^[0-9]+')})

    scraped_company_info["competitors"] = [{
        "name": competition_element.text,
        "address": competition_element.parent.text
    }
    for competition_element in competition_elements]

    return scraped_company_info

def main():

    companies = pd.read_csv(
        # "jupyter_notebooks/WM_manufacturers_from_companies_house.csv",
        # "members/member_summaries.tsv",
        # "prospects/yorkshire_prospects_from_companies_house.csv",
        # "competitors/yorkshire/all_competitors_yorkshire_companies_house.csv",
        "competitors/midlands/all_competitors_midlands_companies_house.csv",
        index_col=0,
        # sep="\t"
        )

    # output_filename = "member_company_info_endole"
    # output_filename = "competitors/yorkshire/all_competitors_yorkshire_endole"
    output_filename = "competitors/midlands/all_competitors_midlands_endole"
    output_filename_csv = f"{output_filename}.csv"
    output_filename_xlsx = f"{output_filename}.xlsx"

    if os.path.exists(output_filename_csv):
        full_company_info = pd.read_csv(output_filename_csv, index_col=0)
        existing_companies = set(full_company_info.index)

    else:
        full_company_info = pd.DataFrame()
        existing_companies = set()

    count = 0
    for i, company in companies.iterrows():
        count += 1
        # if count>50: break

        # company = company.to_dict()

        if False:
        
            companies_house_address = " ".join(filter(lambda v: isinstance(v, str), [
                company[col] for col in (
                    "RegAddress.AddressLine1", 
                    " RegAddress.AddressLine2",	
                    "RegAddress.PostTown",	
                    "RegAddress.County", 
                    "RegAddress.Country", 
                    "RegAddress.PostCode"
                )
            ]))

            sic_codes = [v for k, v in company.items() 
                if k.startswith("SICCode") and not pd.isnull(v)]

            relevant_companies_house_data = pd.Series({
                "companies_house_address": companies_house_address,
                "sic_codes": sic_codes
            })

        # company_name = company["CompanyName"]
        # company_name = company["member_name"]
        company_name = company["companies_house_name"]
        if company_name in existing_companies:
            print ("company", company_name, "already processed")
            continue

        if pd.isnull(company_name): 
            print ("skipping company", company_name, ": missing company name")
            continue

        # post_code = company["RegAddress.PostCode"]
        # post_code = company["postcode"]
        address = company["companies_house_address"]
        post_code = identify_post_code(address)
        if pd.isnull(post_code):
            print ("Missing postcode", company_name)
            post_code = None


        address, link = search_endole(company_name, post_code)
        company_data_from_endole = pd.Series({"endole_address": address}, 
            name=company_name,)
        if link is not None:
            scraped_company_data = scrape_endole(link)
            company_data_from_endole = pd.concat(
                [
                    # relevant_companies_house_data,
                    company,
                    company_data_from_endole, 
                    pd.Series(scraped_company_data, ),
                ]
            )
        else:
            print("no results for company", company_name)

        company_data_from_endole.name = company_name

        full_company_info = full_company_info.append(company_data_from_endole)

        full_company_info = full_company_info[sorted(full_company_info.columns)]
        full_company_info.to_csv(output_filename_csv)

        print()

    full_company_info_excel = pd.ExcelWriter(output_filename_xlsx)
    full_company_info.to_excel(full_company_info_excel, sheet_name="endole", encoding="utf-8")
    full_company_info_excel.save()

if __name__ == "__main__":
    main()