import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), 
    os.path.pardir)))

import pandas as pd
from urllib.parse import quote

import os
import re

from utils.scraping_utils import (get_soup_for_url, 
    get_postcode_prefix, identify_postcode, process_figure_string)
from utils.geo import all_region_postcodes

sleep = 0.1

def search_company_check(company_name, postcode, check_postcode=True):

    '''
    https://companycheck.co.uk/search?term=%22V%22+INSTALLATIONS+MECHANICAL+HANDLING+LIMITED
    '''
    company_name_url = quote("+".join(company_name.split(" ")))
    url = f"https://companycheck.co.uk/search?term={company_name_url}"

    soup, status_code = get_soup_for_url(url, sleep=sleep)
    if status_code != 200:
        raise Exception

    '''
    <a class="result__title">
    '''

    postcode_prefix = get_postcode_prefix(postcode)

    results = soup.find_all("div", {"class": 'info-right'})
    if results is None:
        return None, None, None
    for result in results:
        # check for postcode
        address = result.find("p", {"class": "result__address"}).text 
        link = result.find("a", {"class": 'result__title'})

        if check_postcode:
            result_postcode = identify_postcode(address)
            result_postcode_prefix = get_postcode_prefix(result_postcode)
            if result_postcode_prefix is None:
                continue
            if postcode_prefix == result_postcode_prefix:
                print ("POSTCODE MATCH", postcode, result_postcode,
                    get_postcode_prefix(postcode), get_postcode_prefix(result_postcode))
                return link["title"], address, link["href"]
            elif result_postcode_prefix in all_region_postcodes["midlands"] or result_postcode_prefix in all_region_postcodes["yorkshire"]:
                print ("postcode region match")
                return link["title"], address, link["href"]
        else:
            return link["title"], address, link["href"]
  
    print ("NO MATCHES")
    return None, None, None 

def scrape_company_check(link):

    url = "https://companycheck.co.uk" + link 

    '''
    directors table
    '''

    scraped_company_info = {}

    soup, status_code = get_soup_for_url(url, sleep=sleep)

    # separate row for each director
    directors = []
    if status_code == 200:
        table = soup.find("table", {"class": "directors-table"})

        if table is not None:

            table_rows = table.find_all('tr')
            l = []
            for tr in table_rows:
                td = tr.find_all('td')
                # row = [re.sub(r"[\s+|\n]", "", tr.text.rstrip()) for tr in td]
                row = [tr.text.rstrip() for tr in td]
                l.append(row)
            directors_table = pd.DataFrame(l, 
                columns=["Name", "Role", "Date of Birth", "Appointed", "Resigned"])

            for col in ["Role", "Date of Birth", "Appointed", "Resigned"]:
                directors_table[col] = directors_table[col].map(lambda s: re.sub(r"[\s+|\n]", "", s))
            # drop resigned
            directors_table = directors_table.loc[directors_table["Resigned"]=="-"]
            directors_table = directors_table.loc[directors_table["Date of Birth"]!="-"]
            directors_table = directors_table.loc[~directors_table["Role"].isin({"-", "None"})]

           
            directors = [
                {
                    "director_name": row["Name"], 
                    "director_occupation": row["Role"], 
                    "director_date_of_birth": row["Date of Birth"]
                }
                for _, row in directors_table.iterrows()
                # if row["Role"] not in {"–", "None"}
            ]
        
    # financials
    financial_url = url + "/financials"
    soup, status_code = get_soup_for_url(financial_url, sleep=sleep)

    if status_code == 200:

        financials = soup.find("section", {"class": "Four-financials"})

        if financials is not None:
            key_financials = financials.find_all("div", {"class": "Four-financial" })
            if len(key_financials) == 4:
                for key_financial in key_financials:
                    header = key_financial.find("div", {"class": "Four-financial__header"})
                    header = header.text.replace(" ", "").replace("\n", "")      
                    figure = key_financial.find("div", {"class": "Four-financial__figure"})
                    figure = figure.text.replace(" ", "").replace("\n", "")      
                    change = key_financial.find("div", {"class": "Four-financial__change"})
                    change = change.text.replace(" ", "").replace("\n", "")      
                    scraped_company_info.update({
                        f"{header}_figure": process_figure_string(figure),
                    })
    else:
        print ("NO FINANCIAL INFORMATION")

    # address(es) -- redundant
    # addresses_url = url + "/contact"
    # soup, status_code = get_soup_for_url(addresses_url, sleep=sleep)

    return scraped_company_info, directors

def search_company_check_for_members():
    
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

        companies = pd.read_csv(
            os.path.join(output_dir, f"{membership_level}_members_companies_house.csv"),
            index_col=0,
            )

        output_filename = os.path.join(output_dir, 
            f"{membership_level}_company_check")
            # f"{region}_company_check")
        output_filename_csv = f"{output_filename}.csv"

        if os.path.exists(output_filename_csv):
            full_company_info = pd.read_csv(output_filename_csv, index_col=0)
            existing_companies = set(full_company_info.index)

        else:
            full_company_info = pd.DataFrame()
            existing_companies = set()

        i = 0
        for company_name, company in companies.iterrows():

            if company_name in existing_companies:
                print ("skipping company", company_name, ": already processed")
                continue

            if "postcode" not in company:
                assert "found_companies_house_address" in company
                if pd.isnull(company["found_companies_house_address"]):
                    continue
                company["postcode"] = company["found_companies_house_address"].split(", ")[-1]

            companies_house_data = company

            sic_codes = company["found_companies_house_sic_codes"]

            relevant_companies_house_data = pd.Series({
                **companies_house_data,
                "sic_codes": sic_codes
            })
        
            postcode = company["postcode"]

            company_check_name, registered_address, link = search_company_check(
                company_name, 
                postcode, 
                check_postcode=True,
            )

            company_data_from_company_check = pd.Series(
                {   
                    **relevant_companies_house_data,
                    "company_check_name": company_check_name,
                    "company_check_registered_address": registered_address,
                }, 
                name=company_name,
            )

            if link is not None:
                scraped_company_data, directors = scrape_company_check(link)
                company_data_from_company_check = company_data_from_company_check.append(
                    [
                        pd.Series(scraped_company_data, dtype=str), 
                    ],
                )

                if len(directors) == 0:
                    print ("NO DIRECTORS", company_name)
                    directors = [{
                        "director_name": None, 
                        "director_occupation": None,
                        "director_date_of_birth": None,
                    }]

                company_data_from_company_check = company_data_from_company_check.append(
                    pd.Series({"directors": directors}))

            company_data_from_company_check.name = company_name

            full_company_info = full_company_info.append(company_data_from_company_check)


            if i % 5 == 0: # write frequency
                full_company_info = full_company_info[sorted(full_company_info.columns)]
                print ("writing to", output_filename_csv)
                full_company_info.to_csv(output_filename_csv)

            print()

            i += 1

        full_company_info = full_company_info[sorted(full_company_info.columns)]
        print ("writing to", output_filename_csv)
        full_company_info.to_csv(output_filename_csv)

def main():
    search_company_check_for_members()

if __name__ == "__main__":
    main()