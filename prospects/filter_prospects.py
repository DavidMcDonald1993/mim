import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import os 
import pandas as pd 
import ast

import functools

from utils.scraping_utils import identify_post_code, get_postcode_prefix
from utils.geo import all_region_postcodes
from graph.populate_graph import load_member_summaries


# sic_code_filename = os.path.join("members", "member_and_manufacturer_SIC_codes.csv")
sic_code_filename = os.path.join("member_and_manufacturing_SIC_codes.csv")
sic_codes = pd.read_csv(sic_code_filename, index_col=0, comment="#")

# # filter out out-dated SIC codes
# sic_code_07_filename = os.path.join("data", "SIC07_CH_condensed_list_en.csv")
# sic_codes_07 = pd.read_csv(sic_code_07_filename, index_col=0)
# sic_codes = sic_codes.join(sic_codes_07, on="Code", how="inner")
# sic_codes = sic_codes.loc[~pd.isnull(sic_codes["Description"])]

# sic_codes[["Code", "Description"]].sort_values("Code").reset_index(drop=True).to_csv("member_and_manufacturing_SIC_codes.csv")

relevant_sic_codes = set(map(str, sic_codes["Code"]))

def filter_by_sic_code(codes):
    if isinstance(codes, str):
        codes = ast.literal_eval(codes)
    codes = [code.split(" - ")[0] for code in codes]
    for code in codes:
        if code in relevant_sic_codes:
            return True
    return False

def filter_prospects_by_company_status(prospects_df, status_col="company_status"):
    assert status_col in prospects_df.columns
    return prospects_df.loc[prospects_df[status_col] == "Active"]

def filter_prospects_by_sic_code(prospects_df, sic_code_column="sic_codes"):
    assert sic_code_column in prospects_df.columns
    return prospects_df.loc[prospects_df[sic_code_column].map(filter_by_sic_code)]

def postcode_in_region(postcode, region):
    prefix = get_postcode_prefix(postcode)
    return prefix in all_region_postcodes[region]

def filter_prospects_by_region(
    prospects_df, 
    region,
    region_col="region",
    address_col="address", 
    postcode_col="postcode"):
    
    if region_col not in prospects_df.columns:
        if postcode_col not in prospects_df.columns:
            assert address_col in prospects_df.columns
            prospects_df[postcode_col] = prospects_df[address_col].map(
                identify_post_code)

        prospects_df = prospects_df.loc[~pd.isnull(prospects_df[postcode_col])]

        prospects_df = prospects_df.loc[prospects_df[postcode_col].map(
            functools.partial(postcode_in_region,
            region=region))]
        prospects_df[region_col] = region 

    prospects_df = prospects_df.loc[prospects_df[region_col]==region]    
    return prospects_df

def filter_prospects_by_existing_members(prospects_df, registered_name_col="CompanyName"):

    # members_companies_house_filename = os.path.join("members", 
        # "members_companies_house.csv")
    # existing_members_df = pd.read_csv(members_companies_house_filename, index_col=0)
    existing_members_df = load_member_summaries(concat_uk_sector=False)
    existing_member_names = set(existing_members_df["companies_house_name"].dropna())

    print ("number of existing members", len(existing_member_names))

    # return prospects_df.loc[~prospects_df.index.isin(existing_member_names)]
    return prospects_df.loc[~prospects_df[registered_name_col].isin(existing_member_names)]

def filter_prospects_by_base(prospects_df, registered_name_col="CompanyName"):

    base_filename = os.path.join("base", "base_companies_house.csv")
    base_df = pd.read_csv(base_filename, index_col=0)

    organisations_in_base = set(base_df["companies_house_name"])

    # return prospects_df.loc[~prospects_df.index.isin(organisations_in_base)]
    return prospects_df.loc[~prospects_df[registered_name_col].isin(organisations_in_base)]

def filter_prospects_by_competitors(prospects_df, region, registered_name_col="CompanyName"):
    
    competitors_filename = os.path.join("competitors", region,
        f"{region}_competitors_filtered_by_website.csv")
    competitors = pd.read_csv(competitors_filename, index_col=0)
    region_competitors = set(competitors["companies_house_name"])

    # return prospects_df.loc[~prospects_df.index.isin(region_competitors)]
    return prospects_df.loc[~prospects_df[registered_name_col].isin(region_competitors)]

def filter_prospects_by_total_assets(prospects_df, 
    asset_col_name="TotalCurrentAssets_figure"):
    assert asset_col_name in prospects_df.columns
    return prospects_df.loc[~pd.isnull(prospects_df[asset_col_name])]

def main():

    src = "companies_house"
    region = "yorkshire"
    output_dir = f"{src}/{region}"

    # prospects_df_filename = os.path.join(output_dir, f"{region}_prospects_from_companies_house.csv")
    prospects_df_filename = os.path.join(output_dir, f"base_companies_house.csv")
    # prospects_df_filename = os.path.join(output_dir, f"{region}_competitors.csv")
    prospects_df = pd.read_csv(prospects_df_filename, index_col=0)
    print (prospects_df.shape)

    prospects_df = filter_prospects_by_region(
        prospects_df, 
        region,
        # postcode_col="RegAddress.PostCode",
        postcode_col="postcode",
        )

    print (prospects_df.shape)

    prospects_df = filter_prospects_by_company_status(
        prospects_df,
        # status_col="CompanyStatus",
        status_col="company_status",
        )

    print (prospects_df.shape)

    prospects_df = filter_prospects_by_sic_code(prospects_df,
        sic_code_column="sic_codes")

    print (prospects_df.shape)

    prospects_df = filter_prospects_by_existing_members(prospects_df,
        registered_name_col="companies_house_name"
        )

    print (prospects_df.shape)

    # prospects_df = filter_prospects_by_base(prospects_df)
    # print (prospects_df.shape)

    prospects_df = filter_prospects_by_competitors(prospects_df, region,
        registered_name_col="companies_house_name"
    )

    print (prospects_df.shape)

    # prospects_df = filter_prospects_by_total_assets(prospects_df)
    # print (prospects_df.shape)

    prospects_df.set_index("companies_house_name", drop=True, inplace=True)

    filtered_filename = os.path.join(output_dir, "prospects_filtered.csv")
    # filtered_filename = os.path.join(output_dir, f"{region}_competitors_filtered.csv")
    prospects_df.to_csv(filtered_filename)

if __name__ == "__main__":
    main()