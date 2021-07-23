import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))


import os 
import re

import pandas as pd 

from scrapers.companies_house_scraper import map_to_companies_house_name
from utils.string_matching import find_best_matching_member

from graph.populate_graph import load_member_summaries

'''
Index(['Your Company:', 'Your Name:', 'Your Email:', 'Your Job Title:',
       'Your Mobile:', 'Sales contact (NAME & EMAIL):',
       'Marketing contact (NAME & EMAIL):',
       'HR / Recruitment contact (NAME & EMAIL):',
       'Purchasing contact (NAME & EMAIL):',
       'Director(s) contact (NAME & EMAIL):',
       'What are your objectives? (choose a maximum of 3)',
       'What are you most interested in? (choose a maximum of 3)',
       'Which members do you currently work with? (either as a supplier or customer - please list their company name here)',
       'What is the ideal meeting or networking group size for member matchmaking introductions?',
       'What is your preferred time for networking?',
       'What is most important to you when attending an event',
       'When the Made in Group organises breakfast meetings we use breakout sessions for smaller groups. What themes would you like to participate in when placed into smaller groups?',
       '*Please detail OTHER from the question above here:',
       'Approximately can you tell us roughly what percentage of your suppliers are based within 30 miles of your sites?',
       'Approximately can you tell us what percentage of your customers are based within 30 miles of your sites?',
       'What percentage of your supply chain is outside of the UK?',
       'What percentage of your customers are based outside of the UK?',
       'What traits are most important in a supplier?',
       'What other considerations are important to you, in helping you make meaningful business connections?',
       'Name 1:', 'Email 1:', 'Company 1:',
       'Mobile / phone number 1 (optional):',
       'Why would they make a good member? (1)', 'Name 2:', 'Email 2:',
       'Company 2:', 'Mobile / phone number 2 (optional):',
       'Why would they make a good member? (2)', 'Name 3:', 'Email 3:',
       'Company 3:', 'Mobile / phone number 3 (optional):',
       'Why would they make a good member? (3)'],
      dtype='object')
'''

def main():

    member_summaries = load_member_summaries()
   
    existing_members = set(member_summaries["member_name"].dropna())
    existing_members_companies_house = set(member_summaries["companies_house_name"].dropna())
    existing_members_to_companies_house_name = {
        row["member_name"]: row["companies_house_name"] for _, row in member_summaries.iterrows()
    }

    # survey_filename = os.path.join("survey", "survey_results_first_52.csv")
    survey_filename = os.path.join("survey", "survey_results_final.csv")
    survey_results_df = pd.read_csv(survey_filename, index_col=0)

    # focus on current relationships
    current_business_col = "Which members do you currently work with? (either as a supplier or customer - please list their company name here)"
    survey_results_df = survey_results_df.loc[
        ~survey_results_df[current_business_col].isnull()]

    # regex for splitting write-in answers
    split_pattern = re.compile(r"[\s]*[\n,\.][\s]*")


    connections = []

    for _, row in survey_results_df.iterrows():
        submitted_member_name = row["Your Company:"]

        # find best matching member
        best_matching_member_name = find_best_matching_member(submitted_member_name, existing_members)
        best_matching_member_name_companies_house = existing_members_to_companies_house_name[best_matching_member_name]
        if best_matching_member_name_companies_house not in existing_members_companies_house:
            continue


        # split and iterate member partnerships
        partners = row[current_business_col]
        partners = split_pattern.split(partners)
        for partner in filter(lambda s: s != "", partners):

            # find best matching member
            partner_best_matching_member_name = find_best_matching_member(partner, existing_members)

            # matched_member_companies_house = map_to_companies_house_name(partner_best_matching_member_name)
            partner_best_matching_member_name_companies_house = existing_members_to_companies_house_name[partner_best_matching_member_name]

            if partner_best_matching_member_name_companies_house not in existing_members_companies_house:
                print (partner_best_matching_member_name_companies_house, "NOT IN MEMBERSHIP")
           
            print (submitted_member_name, f"({best_matching_member_name}[{best_matching_member_name_companies_house}])", "--", 
                partner, f"({partner_best_matching_member_name}[{partner_best_matching_member_name_companies_house}])")

            connections.append((submitted_member_name, best_matching_member_name, best_matching_member_name_companies_house, 
                partner, partner_best_matching_member_name, partner_best_matching_member_name_companies_house))    
        print()

    # print ("NUM MISSING SUBMITTERS:", len(missing_submitters), "/", survey_results_df.shape[0])
    # print ("MISSING SUBMITTERS:", missing_submitters)

    # print ("NUM MISSING MATCHED MEMBERS:", len(missing_connected_members), "/", len(connected_members))
    # print ("MISSING MACTCHED MEMBERS", missing_connected_members)

    output_filename = os.path.join("survey", "final_processed_connections.csv")

    connections = pd.DataFrame(connections, 
        columns=["submitted_member_name", "best_matching_member_name", "best_matching_member_name_companies_house", 
            "submitted_partner", "submitted_partner_best_matching_member_name", "submitted_partner_best_matching_member_name_companies_house"])
    connections.to_csv(output_filename)

if __name__ == "__main__":
    main()