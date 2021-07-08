import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import numpy as np
import pandas as pd
from urllib.parse import quote

import os

from utils.scraping_utils import get_soup_for_url, get_postcode_prefix, identify_postcode, strip_text
from utils.geo import all_region_postcodes

import re

import ast


sleep = .1

def map_to_companies_house_name(company_name, postcode=None,):
    if company_name.lower() == "myworkwear":
        return "J.M. WORTHINGTON & CO. LIMITED"
    return search_companies_house(company_name, postcode, postcode is not None)[0]

def search_companies_house(company_name, postcode, check_postcode=True):

    company_name = company_name#.lower()

    # replace +
    company_name = company_name.replace("+", "plus")

    postcode_prefix = get_postcode_prefix(postcode)

    print ("searching companies house for company:", company_name, 
        "using postcode", postcode)

    # company_name_url = "+".join(company_name.split(" "))
    company_name_url = quote(company_name)
    url = f"https://find-and-update.company-information.service.gov.uk/search?q={company_name_url}"

    soup, status_code = get_soup_for_url(url, sleep=sleep)
    assert status_code == 200

    # postcode_prefix = get_postcode_prefix(postcode)

    results = soup.find_all("li", {"class": 'type-company'})

    if results is None:
        return None
    for result in results:

        anchor = result.find("a", {"title": 'View company'})
        link = anchor["href"]

        link_company_name = strip_text(anchor.text)
        print ("COMPANIES HOUSE NAME", link_company_name)

        # check for post code
        address = result.find("p", {"class": None}).get_text(strip=True)


        if not check_postcode:
            return link_company_name, address, link


        if address is None or address == "": 
            continue
        result_postcode = identify_postcode(address)
        if result_postcode is None:
            continue
        result_postcode_prefix = get_postcode_prefix(result_postcode)
        if result_postcode_prefix is None:
            continue
        if postcode_prefix == result_postcode_prefix:
            print ("POSTCODE MATCH", postcode, result_postcode,
                get_postcode_prefix(postcode), get_postcode_prefix(result_postcode))
            return link_company_name, address, link
        elif result_postcode_prefix in all_region_postcodes["midlands"] \
            or result_postcode_prefix in all_region_postcodes["yorkshire"]:
            print ("postcode region match", address, link)
            return link_company_name, address, link
    # raise Exception
    return None, None, None

def scrape(link):
    # pass

    base_url = "https://find-and-update.company-information.service.gov.uk" + link 
    overview_url = base_url

    soup, status_code = get_soup_for_url(overview_url, sleep=sleep)
    assert status_code == 200

    # company status
    status_element = soup.find("dd", {"id": "company-status"})
    if status_element is not None:
        # remove non alphabetic characters
        company_status = re.sub(r"[^A-Za-z]", "", status_element.text.rstrip()) 
    else:
        company_status = None
    print ("company status", company_status)

    # get SIC codes
    sic_elements = soup.find_all(id=re.compile('^sic[0-9]+'))
    sic_codes = []
    for sic_element in sic_elements:
        sic_codes.append(strip_text(sic_element.text))
    return company_status, sic_codes

def main():

    # output_dir = "member_summaries"
    output_dir = os.path.join("data_for_graph", "members")

    for membership_level in (
        "Patron", 
        "Platinum", 
        "Gold", 
        "Silver", 
        "Bronze", 
        "Digital", 
        "Freemium",
        ):

        filename = f"{membership_level}_members"

        companies = pd.read_csv(
            os.path.join(output_dir, f"{filename}.csv"), 
            index_col=0, 
            )

        company_name_col = "companies_house_name"
        assert company_name_col in companies.columns
        companies = companies.drop_duplicates(company_name_col)


        output_filename = os.path.join(output_dir, f"{filename}_companies_house")
        output_filename_csv = f"{output_filename}.csv"
        # output_filename_xlsx = f"{output_filename}.xlsx"

        if os.path.exists(output_filename_csv):
            full_company_info = pd.read_csv(output_filename_csv, index_col=0)
            existing_companies = set(full_company_info.index)

        else:
            full_company_info = pd.DataFrame()
            existing_companies = set()

        for i, company in companies.iterrows():

            company_name = company[company_name_col]
            if pd.isnull(company_name):
                continue

            if company_name in existing_companies:
                print (company_name, "already processed")
                continue

            # postcode = company["postcode"]
            # if pd.isnull(postcode):
            #     postcode = None

            # address = company["competitor_address"]
            # postcode = identify_postcode(address)

            # if pd.isnull(postcode):
            #     print ("skipping company", company_name, "missing postcode ")
            #     continue

            postcode = None

            link_company_name, address, link = search_companies_house(
                company_name, 
                postcode, 
                check_postcode=postcode is not None
                )

            if link is not None:
        
                print ("FOUND LINK", link, "for company", company_name)
                company_status, sic_codes = scrape(link)
                company_data_from_companies_house = pd.Series(
                    {
                        "found_companies_house_name": link_company_name,
                        "found_companies_house_address": address,
                        "found_companies_house_company_status": company_status,
                        "found_companies_house_sic_codes": sic_codes
                    },
                name=company_name
                )
            else:
                print ("no link found for company", company_name)
                company_data_from_companies_house = pd.Series(name=company_name, dtype=object)

            # add existing company data
            company_data_from_companies_house = company_data_from_companies_house.append(company) 
            company_data_from_companies_house.name = company_name

            full_company_info = full_company_info.append(
                company_data_from_companies_house)
            full_company_info.to_csv(output_filename_csv)

            print()

if __name__ == "__main__":
    main()

    # company_name = "Jones & Wilkinson Ltd"
    # print (map_to_companies_house_name(company_name))

    # summary_filename = os.path.join("members", "paid_member_summaries.csv")
    # summary_filename = "Freemium_with_sector_commerce_summaries_production.csv"
 
    # for membership_level in (
    #     # "Patron", 
    #     # "Platinum", 
    #     # "Gold", 
    #     # "Silver", 
    #     # "Bronze", 
    #     # "Digital", 
    #     "Freemium",
    #     ):
    #     # summary_filename = os.path.join("member_summaries", f"{membership_level}_production.csv")
    #     summary_filename = os.path.join("data_for_graph", "members", f"{membership_level}_members.csv")
    #     member_summaries = pd.read_csv(summary_filename, index_col=0)
       
    #     member_summaries["companies_house_name"] = member_summaries.apply(
    #         lambda row: map_to_companies_house_name(
    #             company_name=row["member_name"], 
    #             # postcode=row["postcode"]
    #             ),
    #         axis=1
    #     )

    #     member_summaries.to_csv(summary_filename)