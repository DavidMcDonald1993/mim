
import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import pandas as pd
import ast

from utils.io import read_json, write_json
from graph.populate_graph import load_member_summaries 


def main():

    members = load_member_summaries(filename="production_companies_house")

    SIC_to_sector = read_json("class_to_sector.json")

    members_to_sector = dict()

    for _, row in members.iterrows():
        member_name = row["member_name"]

        sic_codes = row["found_companies_house_sic_codes"]

        if pd.isnull(sic_codes):
            continue

        sic_codes = map(lambda code: code.split(" - ")[1], ast.literal_eval(sic_codes))
        
        sectors = set()
        divisions = set()
        groups = set()
        classes = set()

        for sic_code in filter(lambda code: code in SIC_to_sector, sic_codes):
            print ("adding class", sic_code)
            SIC_code_mapped = SIC_to_sector[sic_code]
            sectors.add(SIC_code_mapped["sector"])
            divisions.add(SIC_code_mapped["division"])
            groups.add(SIC_code_mapped["group"])
            classes.add(sic_code)

        if len(sectors) > 0:
            sectors = sorted(sectors)
            divisions = sorted(divisions)
            groups = sorted(groups)
            classes = sorted(classes)
            members_to_sector[member_name] = {
                "UK_registered_name": row["companies_house_name"],
                "UK_sectors": sectors,
                "UK_divisions": divisions,
                "UK_groups": groups,
                "UK_classes": classes,
            }

    write_json(members_to_sector, "member_summaries/members_to_sector.json")
    pd.DataFrame(members_to_sector).T.to_csv("member_summaries/members_to_sector.csv")


if __name__ == "__main__":
    main()

