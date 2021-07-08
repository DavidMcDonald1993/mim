
import ast
import pandas as pd 
import json 

import itertools

def main():

    members_df = pd.read_csv("members/members_companies_house.csv", index_col=0)

    member_to_sic_code = members_df["sic_codes"].dropna().map(ast.literal_eval)

    counts = dict()

    for _, sic_codes in member_to_sic_code.items():
        for sic_code in sic_codes:
            if sic_code not in counts:
                counts[sic_code] = 0
            counts[sic_code] += 1

    with open("members/member_SIC_code_counts.csv", "w") as f:
        f.write("code,count\n")
        for sic_code in sorted(counts, key=counts.get, 
            reverse=True):
            f.write(f'"{sic_code}","{counts[sic_code]}"\n')

    member_assignments = {}
    for member, sic_codes in member_to_sic_code.items():
        if len(sic_codes) == 0:
            continue 
        sic_codes_ordered_by_count = sorted(sic_codes, key=counts.get, reverse=True)
        member_assignments[member] = {
            "primary": sic_codes_ordered_by_count[0],
            "secondary": sic_codes_ordered_by_count[1:],
        }    

    with open("members/member_SIC_code_assignments.json", "w") as f:
        json.dump(member_assignments, f, indent=4)

    # SIC code co-occurence HEATMAP 
    sic_code_co_occurences = dict()
    for _, sic_codes in member_to_sic_code.items():
        for c1, c2 in itertools.combinations(sic_codes, 2):
            if c1 not in sic_code_co_occurences:
                sic_code_co_occurences[c1] = dict()
            if c2 not in sic_code_co_occurences:
                sic_code_co_occurences[c2] = dict()
            
            if c2 not in sic_code_co_occurences[c1]:
                sic_code_co_occurences[c1][c2] = 0
            sic_code_co_occurences[c1][c2] += 1

            if c1 not in sic_code_co_occurences[c2]:
                sic_code_co_occurences[c2][c1] = 0
            sic_code_co_occurences[c2][c1] += 1

    
    with open("members/member_SIC_code_co_occurences.json", "w") as f:
        json.dump(sic_code_co_occurences, f, indent=4)

if __name__ == "__main__":
    main()