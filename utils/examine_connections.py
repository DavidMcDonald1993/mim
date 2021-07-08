import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from geopy.distance import geodesic

import os
import pandas as pd 

from utils.io import write_json, read_json

from graph.populate_graph import load_member_summaries

def clean_up_json(d):
    return {
        k: (
            {v: sorted(d[k][v])
                for v in d[k]} 
            if isinstance(d[k], dict) 
            else sorted(d[k])
        )
        
        for k in d
    }

def build_sector_commerces_maps(
    paid_member_commerces_filename=os.path.join("directory", 
        "member_commerces_flat.csv"),
    paid_member_commerces_json_filename=os.path.join("directory", 
        "member_commerces(commerces).json"),
    paid_member_json_filename=os.path.join("directory", 
        "member_commerces(members).json"),
    paid_member_sectors_json_filename = os.path.join("directory", 
        "member_sectors.json")
    ):

    '''
    load dicts if they exist
    '''

    if os.path.exists(paid_member_commerces_filename) and os.path.exists(paid_member_json_filename)\
        and os.path.exists(paid_member_sectors_json_filename):

        commerces = read_json(paid_member_commerces_json_filename)
        sectors = read_json(paid_member_sectors_json_filename)
        members = read_json(paid_member_json_filename)

    else:

        assert os.path.exists(paid_member_commerces_filename)

        paid_member_commerces_df = pd.read_csv(paid_member_commerces_filename, index_col=0)

        sectors = dict()
        commerces = dict()
        members = dict()
        for _, row in paid_member_commerces_df.iterrows():
            commerce_name = row["commerce_name"]
            commerce_type = row["commerce_type"]
            member_name = row["member_name"]
            sector_name = row["member_sector"]
            if commerce_name not in commerces:
                commerces[commerce_name] = {"buys": set(), "sells": set()}
            commerces[commerce_name][commerce_type].add(member_name)
            if member_name not in members:
                members[member_name] = {"buys": set(), "sells": set(), "sectors": set()}
            members[member_name][commerce_type].add(commerce_name)
            members[member_name]["sectors"].add(sector_name)
            if sector_name not in sectors:
                sectors[sector_name] = set()
            sectors[sector_name].add(member_name)
        
        # convert set to list
        commerces = clean_up_json(commerces)
        members = clean_up_json(members)
        sectors = clean_up_json(sectors)

        # write to file
        write_json(commerces, paid_member_commerces_json_filename)
        write_json(sectors, paid_member_sectors_json_filename)
        write_json(members, paid_member_json_filename)

    return commerces, sectors, members

def build_messages_map(
    message_df_filename=os.path.join("messages", "production_all_messages_flat.csv"),
    all_messages_json_filename=os.path.join("messages", "production_all_messages.json"),
    ):

    if os.path.exists(all_messages_json_filename):
        all_messages = read_json(all_messages_json_filename)
    else:
        messages_df = pd.read_csv("messages/production_all_messages_flat.csv", index_col=0)

        all_messages = dict()

        for _, row in messages_df.iterrows():
            sender_member_name = row["sender_member_name"]
            recipient_member_name = row["recipient_member_name"]

            key = ":_:".join(sorted([sender_member_name, recipient_member_name]))

            if key not in all_messages:
                all_messages[key] = set()

            all_messages[key].add(row["message"])

        all_messages = clean_up_json(all_messages)
        write_json(all_messages, all_messages_json_filename)
    return all_messages

def build_user_follows_map(
    user_follows_filename=os.path.join("users", "all_user_follows_production.csv"),
    all_user_follows_json_filename=os.path.join("users", "production_all_user_follows.json"),
    ):

    if os.path.exists(all_user_follows_json_filename):
        user_follows = read_json(all_user_follows_json_filename)
    else:
        user_follows_df = pd.read_csv(user_follows_filename, index_col=0)

        user_follows = dict()

        for _, row in user_follows_df.iterrows():
            user = row["full_name"]
            company = row["company_name"]
            followed_member = row["followed_member"]

            key = ":_:".join(sorted([company, followed_member]))

            if key not in user_follows:
                user_follows[key] = set()
            user_follows[key].add(user)

        user_follows = clean_up_json(user_follows)
        write_json(user_follows, all_user_follows_json_filename)
    return user_follows

def build_event_attendee_map(
    event_attendee_filename=os.path.join("events", "all_event_attendees_production.csv"),
    event_attendee_json_filename=os.path.join("events", "all_event_attendees_production.json")):

    if os.path.exists(event_attendee_json_filename):
        event_attendees = read_json(event_attendee_json_filename)
    else:
        event_attendees_df = pd.read_csv(event_attendee_filename, index_col=0)

        event_attendees = dict()

        for _, row in event_attendees_df.iterrows():

            event_name = row["event_name"]
            starts_at = row["starts_at"]

            
            user = row["attendee_name"]
            company = row["company"]

            key = f"{event_name}_{starts_at}"

            if key not in event_attendees:
                event_attendees[key] = set()
            event_attendees[key].add(company)

        event_attendees = clean_up_json(event_attendees)
        write_json(event_attendees, event_attendee_json_filename)

    return event_attendees


def compute_geographical_distance(
    member_pairs,
    # member_summaries_filename=os.path.join("paid_member_summaries_production.csv"),
    ):

    # member_summaries = pd.read_csv(member_summaries_filename, index_col=0)
    member_summaries = load_member_summaries()
    member_to_lat_long = {
        row["member_name"]: (row["latitude"], row["longitude"])
        for _, row in member_summaries.iterrows()
        if not pd.isnull(row["latitude"]) and not pd.isnull(row["longitude"])
    }

    distances_miles = {
        ":_:".join(sorted([m1, m2])) : geodesic(member_to_lat_long[m1], member_to_lat_long[m2]).miles
        for m1, m2 in member_pairs
        if m1 in member_to_lat_long and m2 in member_to_lat_long
    }

    return distances_miles


def main():

    commerces, sectors, members = build_sector_commerces_maps()

    # look at messages
    all_messages = build_messages_map()
  
    # look at follows
    user_follows = build_user_follows_map()

    # events
    event_attendees = build_event_attendee_map()
    event_attendees = {k: set(v) for k, v in event_attendees.items()}

    output_dir = "survey"
    # output_dir = "members"
    input_filename = "processed_connections"
    # input_filename = "member_member_partnerships - midlands_matched"

    # iterate over connections 
    connections_filename = os.path.join(output_dir, f"{input_filename}.csv")
    connections_df = pd.read_csv(connections_filename, index_col=0)

    m1_col = "best_matching_member_name"
    # m1_col = "member_1_best_matching_member"
    m2_col = "submitted_partner_best_matching_member_name"
    # m2_col = "member_2_best_matching_member"

    # geographical distance
    member_pairs = [
        (row[m1_col], row[m2_col])
        for _, row in connections_df.iterrows()
    ]
    distances = compute_geographical_distance(member_pairs)

    all_directory_matches = []

    for _, row in connections_df.iterrows():
        m1 = row[m1_col]
        if m1 not in members:
            continue
        m2 = row[m2_col]
        if m2 not in members:
            continue

        print ("IDENTIFYING SOURCE OF CONNECTION FOR", m1, "AND", m2)
        directory_matches = {
            "member_1": m1,
            "member_2": m2,
        }
        
        found_match = False

        m1_commerces = members[m1]
        for commerce_type in ("buys", "sells"):
            if found_match:
                break
            if commerce_type == "buys":
                m2_commerce_type = "sells"
            else:
                m2_commerce_type = "buys"
            for commerce in m1_commerces[commerce_type]:
                if m2 in commerces[commerce][m2_commerce_type]:
                    print ("COMMERCE MATCH", commerce, )
                    print (m1, commerce_type, commerce, "--", m2, m2_commerce_type, commerce)
                    found_match = True 
                    if "matching_commerces" not in directory_matches:
                        directory_matches["matching_commerces"] = []
                    directory_matches["matching_commerces"].append(
                        {
                            "commerce_name": commerce, 
                            commerce_type: m1,
                            m2_commerce_type: m2,
                        }
                    )
                    # break

        # search based on sectors
        for sector in members[m1]["sectors"]:
            if m2 in sectors[sector]:
                print ("SECTOR MATCH", sector)
                found_match = True 
                if "matching_sectors" not in directory_matches:
                    directory_matches["matching_sectors"] = []
                directory_matches["matching_sectors"].append(sector)
                # break

        # check messages
        key = ":_:".join(sorted([m1, m2]))
        if key in all_messages:
            directory_matches["num_messages"] = len(all_messages[key])
            found_match = True

        # check follows
        if key in user_follows:
            directory_matches["num_follows"] = len(user_follows[key])
            found_match = True

        # count events that both members have attended
        events = []
        for event in event_attendees:
            if m1 not in event_attendees[event]:
                continue
            if m2 not in event_attendees[event]:
                continue 
            events.append(event)
        directory_matches["events"] = events
        directory_matches["num_events"] = len(events)

        # add distance
        if key in distances:
            directory_matches["distance(miles)"] = distances[key]

        if not found_match:
            print ("-"*10)
            print ("NO MATCH FOR", m1, "AND", m2)
            print ("-"*10)
        print ()

        all_directory_matches.append(directory_matches)
    
    all_directory_matches_filename = os.path.join(output_dir, 
        f"{input_filename}_all_directory_matches.csv")
    all_directory_matches = pd.DataFrame(all_directory_matches)
    all_directory_matches.to_csv(all_directory_matches_filename)



if __name__ == "__main__":
    main()
