import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from functools import partial

import pandas as pd 

from utils.string_matching import find_best_matching_member

from graph.populate_graph import load_member_summaries

def main():

    # member_summaries = pd.read_csv("members/paid_member_summaries_clean.csv", index_col=0)
    member_summaries = load_member_summaries()
    existing_members = set(member_summaries["member_name"].dropna())
    # existing_members_companies_house = set(member_summaries["companies_house_name"].dropna())
    existing_members_to_companies_house_name = {
        row["member_name"]: row["companies_house_name"] for _, row in member_summaries.iterrows()
    }

    for region in ("yorkshire", "midlands"):
        partnerships_filename = os.path.join("members", f"member_member_partnerships - {region}.csv")
        partnerships_df = pd.read_csv(partnerships_filename, index_col=None)

        # find best match
        for i in (1, 2):
            partnerships_df[f"member_{i}_best_matching_member"] = \
                partnerships_df[f"member_{i}"].map(
                    partial(find_best_matching_member, existing_members=existing_members))
            partnerships_df[f"member_{i}_best_matching_member_companies_house"] = \
                partnerships_df[f"member_{i}_best_matching_member"].map(
                    existing_members_to_companies_house_name)

        output_filename = os.path.join("members", 
            f"member_member_partnerships - {region}_matched.csv")
        partnerships_df = partnerships_df[sorted(partnerships_df.columns)]
        partnerships_df.to_csv(output_filename)

if __name__ == "__main__":
    main()
