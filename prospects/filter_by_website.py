import os
import numpy as np
import pandas as pd 

def filter_by_website(url):
    if url.startswith("https://find-and-update.company-information"):
        return False
    if url.startswith("https://www.yell.com"):
        return False
    if url.startswith("https://www.facebook.com") or "facebook" in url:
        return False
    if url.startswith("https://www.linkedin.com") or "linkedin" in url:
        return False
    if url.startswith("https://www.dnb.com/business-directory"):
        return False
    if url.startswith("https://www.theconstructionindex.co.uk"):
        return False
    if url.startswith("https://www.theaemt.com/"):
        return False
    if url.startswith("https://bifa.org"):
        return False

    return True

def filter_prospects_by_company_website(
    prospects_df, 
    websites_filename):
    assert os.path.exists(websites_filename), websites_filename

    websites_df = pd.read_csv(websites_filename, index_col=0)

    # print (prospects_df.shape, websites_df.shape)
    # print (prospects_df.head())
    # raise Exception

    prospects_df = prospects_df.join(websites_df, how="inner", on="CompanyName")

    print (prospects_df.shape)

    # remove businesses without a website
    prospects_df = prospects_df.loc[prospects_df["website"].map(filter_by_website)]

    print (prospects_df.shape)

    return prospects_df

def main():
    
    source = "companies_house"
    region = "midlands"
    output_dir = f"{source}/{region}"

    prospects_df_filename = os.path.join(output_dir, "prospects_filtered.csv")
    # prospects_df_filename = os.path.join(output_dir, f"{region}_competitors_filtered.csv")
    prospects_df = pd.read_csv(prospects_df_filename, index_col=0)

    websites_filename = os.path.join(output_dir, f"{region}_company_websites.csv")
    prospects_df = filter_prospects_by_company_website(prospects_df, 
        websites_filename)

    # sort_column = "TotalCurrentAssets_figure"
    # assert sort_column in prospects_df.columns
    # assert prospects_df[sort_column].dtype == np.float64
    # prospects_df = prospects_df.sort_values(sort_column, ascending=False)

    # prospects_df = prospects_df.sort_values(["num_relevant_members", "Total Assets_figure"], 
        # ascending=[False, False])

    output_filename = os.path.join(output_dir, f"{region}_{source}_filtered_by_website.csv")
    prospects_df.to_csv(output_filename)
 
if __name__ == "__main__":
    main()