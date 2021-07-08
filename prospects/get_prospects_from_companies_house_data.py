import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import os 


import re 

import pandas as pd

from functools import partial

from utils.geo import all_region_postcodes
from graph.populate_graph import load_member_summaries

def load_current_members_from_companies_house(
    # members_companies_house_filename="members/members_companies_house.csv"
    ):
    # print ("reading current members from", members_companies_house_filename)
    # df = pd.read_csv(members_companies_house_filename, index_col=0)
    df = load_member_summaries(concat_uk_sector=False)
    return set(df["companies_house_name"])

def load_sic_codes(sic_code_file="data/Manufacturing_SIC_codes.csv"):
    sic_codes = pd.read_csv(sic_code_file, comment="#", index_col=0)
    # assert "Non-trading company" not in set(sic_codes["Name"])
    # assert "Financial intermediation not elsewhere classified" not in set(sic_codes["Name"])
    return set(sic_codes["Code"].map(str))

def row_in_codes(row, codes):
    for col in (f"SICCode.SicText_{i}" for i in range(1, 5)):
        s = row[col]
        if pd.isnull(s):
            continue
        if s.split(" - ")[0] in codes:
            return True
    return False

def row_in_region(row, region_postcodes):
    postcode = row["RegAddress.PostCode"]
    pattern = r"^({})[0-9]+".format("|".join(region_postcodes))
    if re.match(pattern, postcode):
        return True
    else:
        return False

def add_sic_codes(row):
    sic_codes = []
    for i in range(1, 5):
        code = row[f"SICCode.SicText_{i}"]
        if not pd.isnull(code):
            sic_codes.append(code)
    return sic_codes

def get_relevant_records(
    region=["midlands", "yorkshire"],
    sic_code_file="data/Manufacturing_SIC_codes.csv",
    data_filename="data/BasicCompanyDataAsOneFile-2021-02-01.csv.gz"
    ):

    print ("getting prospects from companies house for region:", region)

    # load names of all businesses in database
    current_members = load_current_members_from_companies_house()

    if isinstance(region, list):
        region_postcodes = {pc 
        for r in region
        for pc in all_region_postcodes[r]}
    else:
        assert region in all_region_postcodes
        region_postcodes = all_region_postcodes[region]
    print ("postcodes in region:", region_postcodes)

    codes = load_sic_codes(sic_code_file=sic_code_file)
    print ("number of SIC codes:", len(codes))

    print ("reading companies house data from", data_filename)
    chunks = []
    with pd.read_csv(data_filename, 
        chunksize=1000) as reader:
    
        for chunk in reader:
            chunk = chunk.loc[chunk["CompanyStatus"]=="Active"]
            if chunk.shape[0] == 0: continue
            chunk = chunk.loc[~pd.isnull(chunk["RegAddress.PostCode"])]
            if chunk.shape[0] == 0: continue
            chunk = chunk.loc[chunk["Accounts.AccountCategory"].isin(
                {
                    "FULL", 
                    "SMALL", 
                    "TOTAL EXEMPTION FULL", 
                    "UNAUDITED ABRIDGED",
                    "GROUP"
                })]
            if chunk.shape[0] == 0: continue
            chunk = chunk.loc[
                chunk.apply(
                    partial(row_in_codes, codes=codes), axis=1)]
            if chunk.shape[0] == 0: continue
            chunk = chunk.loc[
                chunk.apply(
                    partial(row_in_region, region_postcodes=region_postcodes), axis=1)]
            if chunk.shape[0] == 0: continue
            chunk = chunk.loc[
                chunk["CompanyName"].map(lambda name: name.lower() not in current_members)
            ]

            chunk["sic_codes"] = chunk.apply(add_sic_codes, axis=1)
            chunk["postcode"] = chunk["RegAddress.PostCode"]

            chunks.append(chunk)

    df = pd.concat(chunks).reset_index(drop=True)

    print ("number of relevant prospects from companies house:", df.shape[0])
    return df

def main():

    # region = ["midlands", "yorkshire"]
    region = "yorkshire"
    # sic_code_file = "members/member_and_manufacturer_SIC_codes.csv"
    sic_code_file = "member_and_manufacturing_SIC_codes.csv"

    prospects_from_companies_house = get_relevant_records(region, 
        sic_code_file=sic_code_file,
        )
    if isinstance(region, list):
        region = "_".join(region)

    output_dir = os.path.join("companies_house", region)
    os.makedirs(output_dir, exist_ok=True)
    prospects_from_companies_house.to_csv(os.path.join(output_dir,
        f"{region}_prospects_from_companies_house.csv"))

if __name__ == "__main__":
    main()