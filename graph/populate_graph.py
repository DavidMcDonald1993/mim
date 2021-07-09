import pandas as pd  

import ast

import sys
import os.path

from pandas.core.algorithms import isin
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import dateutil.parser as parser

from utils.mysql_utils import separator
from utils.io import read_json
from utils.scraping_utils import remove_html_tags
from utils.user_utils import infer_role

from graph.arango_utils import *

import pgeocode

def cast_to_float(v):
    try:
        return float(v)
    except ValueError:
        return v

def convert_to_iso8601(text):
    date = parser.parse(text)
    return date.isoformat()

def load_member_summaries(
    source_dir="data_for_graph/members", 
    filename="company_check", 
    # concat_uk_sector=False
    ):

    '''
    LOAD FLAT FILES OF MEMBER DATA
    '''

    dfs = []
    for membership_level in ("Patron", "Platinum", "Gold", "Silver", "Bronze", "Digital", "Freemium"):
        summary_filename = os.path.join(source_dir, membership_level, f"{membership_level}_{filename}.csv")
        print ("reading summary from", summary_filename)
        dfs.append(pd.read_csv(summary_filename, index_col=0).rename(columns={"database_id": "id"}))
    summaries = pd.concat(dfs)

    # if concat_uk_sector:
    #     member_uk_sectors = pd.read_csv(f"{source_dir}/members_to_sector.csv", index_col=0)
    #     # for col in ("sectors", "divisions", "groups", "classes"):
    #     #     member_uk_sectors[f"UK_{col}"] = member_uk_sectors[f"UK_{col}"].map(ast.literal_eval)
    #     summaries = summaries.join(member_uk_sectors, on="member_name", how="left")

    return summaries

def populate_sectors(
    source_dir="data_for_graph", 
    db=None):

    '''
    CREATE AND ADD SECTOR(AS DEFINED IN MIM DB) NODES TO GRAPH
    '''

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("Sectors", db)

    sectors = pd.read_csv(f"{source_dir}/all_sectors.csv", index_col=0)

    i = 0

    for _, row in sectors.iterrows():
        sector_name = row["sector_name"]
        print ("creating document for sector", sector_name)

        document = {
            "_key": str(i),
            "name": sector_name,
            "sector_name": sector_name,
            "id": row["id"]
        }

        insert_document(db, collection, document)

        i += 1

def populate_commerces(
    data_dir="data_for_graph", 
    db=None):

    '''
    CREATE AND ADD COMMERCE(AS DEFINED IN MIM DB) NODES TO GRAPH
    '''

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("Commerces", db)

    commerces = pd.read_csv(f"{data_dir}/all_commerces_with_categories.csv", index_col=0)
    commerces = commerces.drop_duplicates("commerce_name")

    i = 0

    for _, row in commerces.iterrows():
        commerce = row["commerce_name"]
        category = row["commerce_category"]
       
        print ("creating document for commerce", commerce)

        document = {
            "_key": str(i),
            "name": commerce,
            "commerce": commerce,
            "category": category,
            "id": row["id"]
        }

        insert_document(db, collection, document)

        i += 1

def populate_members(
    cols_of_interest=[
        "id",
        "member_name",
        "website",
        "about_company",
        "membership_level",
        "tenancies",
        "badges",
        "accreditations",
        "sectors", # add to member as list
        "buys",
        "sells",
        "sic_codes",
        "directors",
        "Cash_figure",
        "NetWorth_figure",
        "TotalCurrentAssets_figure",
        "TotalCurrentLiabilities_figure",
    ],
    db=None):

    '''
    CREATE AND POPULATE MEMBER NODES
    '''

    if db is None:
        db = connect_to_mim_database()
  
    collection = connect_to_collection("Members", db, )

    members = load_member_summaries(concat_uk_sector=False)
    
    members = members[cols_of_interest]
    members = members.drop_duplicates("member_name") # ensure no accidental duplicates 
    members = members.loc[~pd.isnull(members["tenancies"])]
    members["about_company"] = members["about_company"].map(remove_html_tags, na_action="ignore")
    members = members.sort_values("member_name")

    i = 0

    for _, row in members.iterrows():

        member_name = row["member_name"]
        if pd.isnull(member_name):
            continue

        document = {
            "_key" : str(i),
            "name": member_name,
            **{
                k: (row[k].split(separator) if not pd.isnull(row[k]) and k in {"sectors", "buys", "sells"}
                        else ast.literal_eval(row[k]) if not pd.isnull(row[k]) and k in {
                            "UK_sectors",
                            "UK_divisions",
                            "UK_groups",
                            "UK_classes",
                            "sic_codes",
                            "directors",
                            }
                        else cast_to_float(row[k]) if k in {"Cash_figure","NetWorth_figure","TotalCurrentAssets_figure","TotalCurrentLiabilities_figure"}
                        else row[k] if not pd.isnull(row[k])
                        else None)
                    for k in cols_of_interest
            }, 
        }

        if not pd.isnull(row["directors"]):
            directors_ = ast.literal_eval(row["directors"])
            directors = []
            for director in directors_:

                if pd.isnull(director["director_name"]):
                    continue
                
                if not pd.isnull(director["director_date_of_birth"]):
                    director["director_date_of_birth"] = insert_space(director["director_date_of_birth"], 3)

                directors.append(director)

        else:
            directors = []

        document["directors"] = directors

        assert not pd.isnull(row["tenancies"])
        tenancies = []
        regions = []

        for tenancy in row["tenancies"].split(separator):
            tenancies.append(tenancy)
            if tenancy == "Made in the Midlands":
                regions.append("midlands")
            else:
                assert tenancy == "Made in Yorkshire", tenancy
                regions.append("yorkshire")
        document["tenancies"] = tenancies
        document["regions"] = regions

        for award in ("badge", "accreditation"):
            award_name = f"{award}s"
            if not pd.isnull(row[award_name]):
                awards = []
                for a in row[award_name].split(separator):
                    awards.append(a)
                document[award_name] = awards

        insert_document(db, collection, document)

        i += 1

def add_SIC_hierarchy_to_members(db=None):

    '''
    USE SIC CODES TO MAP TO SECTOR USING FILE:
    data/class_to_sector.json
    '''

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("Members", db, )

    get_sic_codes_query = f'''
    FOR m IN Members
        FILTER m.sic_codes != NULL
        RETURN {{
            _key: m._key,
            sic_codes: m.sic_codes,
        }}
    '''
    members = aql_query(db, get_sic_codes_query)

    class_to_sector_map = read_json("data/class_to_sector.json")

    for member in members:

        sic_codes = member["sic_codes"]
            
        sic_codes = [sic_code.split(" - ")[1]
            for sic_code in sic_codes]

        classes = set()
        groups = set()
        divisions = set()
        sectors = set()

        for sic_code in sic_codes:
            if sic_code not in class_to_sector_map:
                continue 
            classes.add(sic_code)
            groups.add(class_to_sector_map[sic_code]["group"])
            divisions.add(class_to_sector_map[sic_code]["division"])
            sectors.add(class_to_sector_map[sic_code]["sector"])

        document = {
            "_key" : member["_key"],
            "UK_classes": sorted(classes), 
            "UK_groups": sorted(groups), 
            "UK_divisions": sorted(divisions), 
            "UK_sectors": sorted(sectors), 
            
        }

        insert_document(db, collection, document, verbose=True)

def populate_users(
    data_dir="data_for_graph",
    cols_of_interest=[
        "id",
        "full_name",
        "email",
        "company_name",
        "company_position",
        "company_role",
    ],
    db=None):
    '''
    CREATE AND ADD USER NODES
    '''

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("Users", db, )

    user_filename = f"{data_dir}/all_users.csv"
    users = pd.read_csv(user_filename, index_col=0)
    users["company_role"] = users.apply(
        infer_role,
        axis=1
    )

    i = 0

    for _, row in users.iterrows():

        user_name = row["full_name"]
        if pd.isnull(user_name):
            continue

        document = {
            "_key" : str(i),
            "name": user_name,
            **{
                k: (row[k] if not pd.isnull(row[k]) else None)
                    for k in cols_of_interest
            }
        }

        print ("inserting data", document)

        insert_document(db, collection, document)

        i += 1

def populate_user_works_at(
    data_dir="data_for_graph",
    db=None):

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("UserWorksAt", db, className="Edges")

    user_filename = f"{data_dir}/all_users.csv"
    users = pd.read_csv(user_filename, index_col=0)
    users["company_role"] = users.apply(
        infer_role,
        axis=1
    )

    member_name_to_id = name_to_id(db, "Members", "id")
    user_name_to_id = name_to_id(db, "Users", "id")

    i = 0

    for _, row in users.iterrows():

        user_id = row["id"]
        company_id = row["company_id"]

        if user_id not in user_name_to_id:
            continue

        if company_id  not in member_name_to_id:
            continue

        document = {
            "_key" : str(i),
            "name": "works_at",
            "_from": user_name_to_id[user_id],
            "_to": member_name_to_id[company_id],
            "company_position": row["company_position"]
        }

        print ("inserting data", document)

        insert_document(db, collection, document)

        i += 1

def populate_user_follows(
    data_dir="data_for_graph",
    db=None):

    if db is None:
        db = connect_to_mim_database()
    user_follows_collection = connect_to_collection("UserFollows", db, className="Edges")
    user_follows_members_collection = connect_to_collection("MemberMemberFollows", db, className="Edges")

    user_follows_filename = os.path.join(data_dir, "all_user_follows.csv")
    users = pd.read_csv(user_follows_filename, index_col=0)

    member_name_to_id = name_to_id(db, "Members", "id")
    user_name_to_id = name_to_id(db, "Users", "id")

    i = 0

    for _, row in users.iterrows():

        user_id = row["id"]

        if user_id not in user_name_to_id:
            continue

        user_name = row["full_name"]
        employer_id = row["employer_id"]

        followed_member_id = row["followed_member_id"]

        if followed_member_id not in member_name_to_id:
            continue

        # user -> member

        document = {
            "_key" : str(i),
            "name": "follows",
            "_from":  user_name_to_id[user_id],
            "_to": member_name_to_id[followed_member_id]
        }

        print ("inserting data", document)
        insert_document(db, user_follows_collection, document)

        # member -> member

        if employer_id in member_name_to_id:

            document = {
                "_key" : str(i),
                "name": "follows",
                "_from":  member_name_to_id[employer_id],
                "_to": member_name_to_id[followed_member_id],
                "followed_by": user_name,
            }

            print ("inserting data", document)
            insert_document(db, user_follows_members_collection, document)

        i += 1

def populate_member_sectors(
    db=None):

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("InSector", db, className="Edges")

    members = load_member_summaries()

    i = 0

    member_name_to_id = name_to_id(db, "Members", "id")
    sector_name_to_id = name_to_id(db, "Sectors", "sector_name")

    for _, row in members.iterrows():

        member_id = row["id"]
        if member_id not in member_name_to_id:
            continue
        sectors = row["sectors"]
        if pd.isnull(sectors):
            continue

        sectors = sectors.split(separator)

        for sector in sectors:

            document = {
                "_key" : str(i),
                "name": "in_sector",
                "_from": member_name_to_id[member_id],
                "_to": sector_name_to_id[sector],
            }

            print ("inserting data", document)
            insert_document(db, collection, document)

            i += 1

def populate_member_commerces(
    db=None):

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("PerformsCommerce", db, className="Edges")

    members = load_member_summaries()

    i = 0

    member_name_to_id = name_to_id(db, "Members", "id")
    commerce_name_to_id = name_to_id(db, "Commerces", "commerce")

    for _, row in members.iterrows():

        member_id = row["id"]
        if member_id not in member_name_to_id:
            continue

        for commerce_type in ("buys", "sells"):
            commerce = row[commerce_type]
            if not pd.isnull(commerce):

                commerce = commerce.split(separator)

                for c in commerce:
                    if c=="": 
                        assert False
                        continue

                    document = {
                        "_key" : str(i),
                        "name": commerce_type,
                        "_from": member_name_to_id[member_id],
                        "_to": commerce_name_to_id[c],
                        "commerce_type": commerce_type
                    }

                    print ("inserting data", document)

                    insert_document(db, collection, document)

                    i += 1

def populate_messages(
    data_dir="data_for_graph",
    db=None):

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("Messages", db, className="Edges")

    message_filename = os.path.join(data_dir, "all_messages.csv")
    messages = pd.read_csv(message_filename, index_col=0)
    messages = messages.drop_duplicates()

    i = 0

    user_name_to_id = name_to_id(db, "Users", "id")

    for _, row in messages.iterrows():
        sender_id = row["sender_id"]
        if sender_id not in user_name_to_id:
            continue
        
        subject = row["subject"]
        message = row["message"]
        message = remove_html_tags(message)
        timestamp = str(row["created_at"])
       
        # TODO characterise messages

        # recipients = json.loads(row["all_recipients"])
        # for recipient in recipients:
        # receiver = recipient["name"]
      
        receiver_id = row["recipient_id"]
        # receiver_member = row["recipient_member_name"]

        if receiver_id not in user_name_to_id:
            continue

        if sender_id == receiver_id:
            continue

        document = {
            "_key":  str(i), 
            "name": "messages",
            "_from": user_name_to_id[sender_id],
            "_to": user_name_to_id[receiver_id],
            "subject": subject,
            "message": message,
            "sent_at": convert_to_iso8601(timestamp),
        }
        insert_document(db, collection, document)

        i += 1

def populate_member_member_business(
    db=None):

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("MemberMemberBusiness", db, className="Edges")

    member_name_to_id = name_to_id(db, "Members", "member_name")

    i = 0

    # articles

    for region in ("yorkshire", "midlands"):
        filename = os.path.join("members", f"member_member_partnerships - {region}_matched.csv")
        member_member_business = pd.read_csv(filename, index_col=None)

        for _, row in member_member_business.iterrows():
            member_1 = row["member_1_best_matching_member"]
            member_2 = row["member_2_best_matching_member"]

            if member_1 not in member_name_to_id:
                continue
            if member_2 not in member_name_to_id:
                continue

            article_title = row["article_title"]
            document = {
                # "_key":  sanitise_key(f"{member_1}_{member_2}_article"),
                "_key":  str(i),
                "name": "does_business",
                # "_from": f"Members/{sanitise_key(member_1)}",
                "_from": member_name_to_id[member_1],
                # "_to": f"Members/{sanitise_key(member_2)}",
                "_to": member_name_to_id[member_2],
                "source": "article",
                "article_title": article_title,
                "region": region
            }
            insert_document(db, collection, document)

            i += 1

    # survey connections
    connections_filename="survey/final_processed_connections.csv"
    survey_connections = pd.read_csv(connections_filename, index_col=0)

    for _, row in survey_connections.iterrows():
        member_1 = row["best_matching_member_name"]
        member_2 = row["submitted_partner_best_matching_member_name"]

        if member_1 not in member_name_to_id:
            continue
        if member_2 not in member_name_to_id:
            continue

        document = {
            # "_key":  sanitise_key(f"{member_1}_{member_2}_survey"),
            "_key":  str(i),
            "name": "does_business",
            # "_from": f"Members/{sanitise_key(member_1)}",
            "_from": member_name_to_id[member_1],
            "_to": f"Members/{sanitise_key(member_2)}",
            "_to": member_name_to_id[member_2],
            "source": "survey",
        }
        insert_document(db, collection, document)

        i += 1
   

def populate_events(
    data_dir="data_for_graph",
    cols_of_interest = [
        "id",
        "event_name",
        "event_type",
        "tenants",
        "members",
        "description",
        "status",
        "venue",
        "starts_at",
        "ends_at",
    ],
    db=None):
    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("Events", db,)

    events_df_filename = os.path.join(data_dir, "all_events.csv")
    events_df = pd.read_csv(events_df_filename, index_col=0)
    # events_df = events_df.drop_duplicates(["event_name", "starts_at"])

    i = 0

    for _, row in events_df.iterrows():

        event_name = row["event_name"]

        document = {
            "_key" : str(i),
            "name": event_name, 
            **{
                k: (convert_to_iso8601(row[k]) if not pd.isnull(row[k]) and k in ("starts_at", "ends_at", )
                    else row[k].split(separator) if not pd.isnull(row[k]) and k in ("tenants", "distinct_event_tags", "members")
                    else row[k] if not pd.isnull(row[k]) else None)
                    for k in cols_of_interest
            }
        }
        insert_document(db, collection, document)

        i += 1

def populate_event_sessions(
    data_dir="data_for_graph",
    db=None,
    ):

    if db is None:
        db = connect_to_mim_database()
    event_session_collection = connect_to_collection("EventSessions", db,)
    event_to_session_collection = connect_to_collection("EventHasSession", db, className="Edges",)

    event_session_filename = os.path.join(data_dir, "all_event_sessions.csv")
    event_session_df = pd.read_csv(event_session_filename, index_col=0)

    event_name_to_id = name_to_id(db, "Events", "id")

    i = 0

    for _, row in event_session_df.iterrows():

        session_name = row["session_name"]

        document = {
            "_key" : str(i),
            "name": session_name, 
            **{
                k: (row[k] if not pd.isnull(row[k]) else None)
                    for k in row.index
            }
        }
        insert_document(db, event_session_collection, document)

        # event -> session
        event_id = row["event_id"]
        if event_id in event_name_to_id:
            document = {
                "_key" : str(i),
                "_from": event_name_to_id[event_id],
                "_to": f"EventSessions/{i}",
                "name": "has_session",
            }

            insert_document(db, event_to_session_collection, document)

        i += 1

def populate_event_attendees(
    data_dir="data_for_graph",
    db=None):

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("EventAttendees", db, className="Edges")

    event_attendees_df = pd.read_csv(f"{data_dir}/all_event_attendees.csv", index_col=0)

    event_name_to_id = name_to_id(db, "Events", "id")
    user_name_to_id = name_to_id(db, "Users", "id")

    i = 0

    for _, row in event_attendees_df.iterrows():

        attendee_id = row["user_id"]

        if attendee_id not in user_name_to_id:
            continue
        
        event_id = row["event_id"]

        if event_id not in event_name_to_id:
            continue 

        document = {
            "_key": str(i),
            "name": "attends", 
            "_from": user_name_to_id[attendee_id],
            "_to": event_name_to_id[event_id],
            "attended": row["attended"],
        }
        insert_document(db, collection, document)

        i += 1

def populate_event_session_attendees(
    data_dir="data_for_graph",
    db=None):

    if db is None:
        db = connect_to_mim_database()
    collection = connect_to_collection("EventSessionAttendees", db, className="Edges")

    event_attendees_df = pd.read_csv(f"{data_dir}/all_event_session_attendees.csv", index_col=0)

    session_name_to_id = name_to_id(db, "EventSessions", "id")
    user_name_to_id = name_to_id(db, "Users", "id")

    i = 0

    for _, row in event_attendees_df.iterrows():

        attendee_id = row["user_id"]

        if attendee_id not in user_name_to_id:
            continue
        
        session_id = row["session_id"]

        if session_id not in session_name_to_id:
            continue 

        document = {
            "_key": str(i),
            "name": "attends_session", 
            "_from": user_name_to_id[attendee_id],
            "_to": session_name_to_id[session_id],
        }
        insert_document(db, collection, document)

        i += 1

# def populate_member_member_sector_connections(
#     data_dir="data_for_graph",
#     db=None):
#     if db is None:
#         db = connect_to_mim_database()

#     collection = connect_to_collection("MemberMemberSector", db, className="Edges")

#     members_in_sector = read_json(f"{data_dir}/member_sectors.json")

#     member_name_to_id = name_to_id(db, "Members", "member_name")

#     i = 0

#     for sector in members_in_sector:
        
#         num_members_in_sector = len(members_in_sector[sector])

#         for m1 in range(num_members_in_sector):
#             member_1 = members_in_sector[sector][m1]
#             if member_1 not in member_name_to_id:
#                 continue
#             for m2 in range(m1+1, num_members_in_sector):
#                 member_2 = members_in_sector[sector][m2]
#                 if member_2 not in member_name_to_id:
#                     continue

#                 document = {
#                     # "_key": sanitise_key(f"{attendee_name}_{event_name}_{starts_at}"),
#                     "_key": str(i),
#                     "name": f"in_sector_{sector}", 
#                     "_from": member_name_to_id[member_1],
#                     "_to": member_name_to_id[member_2],
#                 }
#                 insert_document(db, collection, document, verbose=True)

#                 i += 1

# def populate_member_member_commerce_connections(
#     data_dir="data_for_graph",
#     db=None):
#     if db is None:
#         db = connect_to_mim_database()

#     collection = connect_to_collection("MemberMemberCommerce", db, className="Edges")

#     members_in_commerces = read_json(f"{data_dir}/member_commerces(commerces).json")

#     member_name_to_id = name_to_id(db, "Members", "member_name")

#     i = 0

#     for commerce in members_in_commerces:
        
#         sells = members_in_commerces[commerce]["sells"]
#         buys = members_in_commerces[commerce]["buys"]

#         for member_1, member_2 in product(sells, buys):
#             if member_1 not in member_name_to_id:
#                 continue
#             if member_2 not in member_name_to_id:
#                 continue

#             document = {
#                 # "_key": sanitise_key(f"{attendee_name}_{event_name}_{starts_at}"),
#                 "_key": str(i),
#                 "name": f"commerce_{commerce}", 
#                 "_from": member_name_to_id[member_1],
#                 "_to": member_name_to_id[member_2],
#             }
#             insert_document(db, collection, document, verbose=True)

#             i += 1

# def populate_member_member_event_connections(
#     data_dir="data_for_graph",
#     db=None):
#     if db is None:
#         db = connect_to_mim_database()

#     collection = connect_to_collection("MemberMemberEvents", db, className="Edges")

#     members_at_events = read_json(f"{data_dir}/all_event_attendees_production.json")

#     member_name_to_id = name_to_id(db, "Members", "member_name")

#     i = 0

#     for event in members_at_events:
        
#         attending_members = members_at_events[event]

#         for member_1, member_2 in combinations(attending_members, 2):
#             if member_1 not in member_name_to_id:
#                 continue
#             if member_2 not in member_name_to_id:
#                 continue

#             document = {
#                 "_key": str(i),
#                 "name": f"event_{event}", 
#                 "_from": member_name_to_id[member_1],
#                 "_to": member_name_to_id[member_2],
#             }
#             insert_document(db, collection, document, verbose=True)

#             i += 1

def populate_uk_sector_hierarchy(db=None):

    '''
    USE HIERARCHY IN FILE data/SIC_hierarchy.json TO BUILD TREE OF SIC STRUCTURE
    '''

    if db is None:
        db = connect_to_mim_database()

    uk_sectors = read_json("data/SIC_hierarchy.json")

    classes_collection = connect_to_collection("UKClasses", db)
    class_hierarchy_collection = connect_to_collection("UKClassHierarchy", db, className="Edges")
    
    i = 0
    j = 0

    for sector in uk_sectors:
        current_sector_id = i 

        document = {
            "_key": str(i),
            "name": f"sector_{sector}",
            "type": "sector",
            "identifier": sector
        }
        insert_document(db, classes_collection, document, verbose=True)

        i += 1

        for division in uk_sectors[sector]:
            current_division_id = i

            document = {
                "_key": str(i),
                "name": f"division_{division}",
                "type": "division",
                "identifier": division
            }
            insert_document(db, classes_collection, document, verbose=True)

            i += 1

            # add division to sector edge
            document = {
                "_key": str(j),
                "_from": f"UKClasses/{current_division_id}",
                "_to": f"UKClasses/{current_sector_id}",
                "name": "InSector"
            }

            insert_document(db, class_hierarchy_collection, document, verbose=True)

            j += 1

            for group in uk_sectors[sector][division]:
                current_group_id = i

                document = {
                    "_key": str(i),
                    "name": f"group_{group}",
                    "type": "group",
                    "identifier": group
                }
                insert_document(db, classes_collection, document, verbose=True)

                i += 1

                # add group to division edge
                document = {
                    "_key": str(j),
                    "_from": f"UKClasses/{current_group_id}",
                    "_to": f"UKClasses/{current_division_id}",
                    "name": "InDivision"
                }

                insert_document(db, class_hierarchy_collection, document, verbose=True)

                j += 1

                for c in uk_sectors[sector][division][group]:
                    current_class_id = i

                    document = {
                        "_key": str(i),
                        "name": f"class_{c}",
                        "type": "class",
                        "identifier": c
                    }
                    insert_document(db, classes_collection, document, verbose=True)

                    i += 1

                    # add group to division edge
                    document = {
                        "_key": str(j),
                        "_from": f"UKClasses/{current_class_id}",
                        "_to": f"UKClasses/{current_group_id}",
                        "name": "InGroup"
                    }

                    insert_document(db, class_hierarchy_collection, document, verbose=True)

                    j += 1


def populate_members_to_uk_class(db=None):
    if db is None:
        db = connect_to_mim_database()

    '''
    ADD EDGES TO CONNECT MEMBER NODES TO CLASS
    '''

    collection = connect_to_collection( "MembersToClass", db, className="Edges")

    uk_class_to_id = name_to_id(db, "UKClasses", "name")

    query = f'''
    FOR m IN Members
        FILTER m.UK_classes != NULL
        FILTER LENGTH(m.UK_classes) > 0
        
        RETURN {{
            id: m._id,
            UK_classes: m.UK_classes,
            UK_groups: m.UK_groups,
            UK_divisions: m.UK_divisions,
            UK_sectors: m.UK_sectors,
        }}
    '''

    members_to_sector = aql_query(db, query)

    i = 0

    for member_assignments in members_to_sector:


        assignments = {
            "sector": member_assignments["UK_sectors"],
            "division": member_assignments["UK_divisions"],
            "group": member_assignments["UK_groups"],
            "class": member_assignments["UK_classes"],
        }
       
        # ADD ALL LEVELS
        for key in ("class", "group", "division", "sector"):
            for c in assignments[key]:

                c = f"{key}_{c}" # name is type_identifier

                document = {
                    "_key": str(i),
                    "_from": member_assignments["id"],
                    "_to": uk_class_to_id[c],
                    "name": f"in_{key}",
                }

                insert_document(db, collection, document, verbose=True)

                i += 1

def insert_space(string, integer):
    return string[0:integer] + ' ' + string[integer:]

def populate_prospects(db=None):
    
    if db is None:
        db = connect_to_mim_database()
  
    collection = connect_to_collection("Prospects", db, )

    print ("reading postcode to lat-long mapping")
    postcode_to_lat_long = pd.read_csv("postcode_to_lat_long.csv", index_col=1)
    postcode_to_lat_long = {
        postcode: [row["lat"], row["long"]]
        for postcode, row in postcode_to_lat_long.iterrows()
    }

    nomi = pgeocode.Nominatim('gb') 

    i = 0

    '''
    ENDOLE
    '''

    prospects = []
    for region in ("yorkshire", "midlands"):
        filename = os.path.join("competitors", region, f"{region}_competitors_filtered_by_website.csv",)
        region_prospects = pd.read_csv(filename, index_col=0)
        prospects.append(region_prospects)

    prospects = pd.concat(prospects)
    prospects =  prospects = prospects[~prospects.index.duplicated(keep='first')]

    for prospect, row in prospects.iterrows():

        document = {
            "_key" : str(i),
            "name": prospect,
            **{
                k.replace(".", "_"): (row[k].split(separator) if not pd.isnull(row[k]) and k in {}
                    else cast_to_float(row[k]) if not pd.isnull(row[k]) and k in {
                        "Cash in Bank_figure","Cash in Bank_trend","Cash in Bank_trend_change","Debt Ratio (%)_figure",
                        "Debt Ratio (%)_trend","Debt Ratio (%)_trend_change","Employees_figure","Employees_trend",
                        "Employees_trend_change","Net Assets_figure","Net Assets_trend","Net Assets_trend_change","Total Assets_figure",
                        "Total Assets_trend","Total Assets_trend_change","Total Liabilities_figure","Total Liabilities_trend",
                        "Total Liabilities_trend_change","Turnover _figure","Turnover _trend","Turnover _trend_change","Year Ended_figure",
                    }
                        else ast.literal_eval(row[k]) if not pd.isnull(row[k]) and k in {
                            "sic_codes",
                            "relevant_members",
                            "competitors",
                        }
                        else row[k] if not pd.isnull(row[k])
                        else None)
                    for k in set(row.index) - {"directors"}
            }, 
        }

        if not pd.isnull(row["directors"]):
            directors_ = ast.literal_eval(row["directors"])
            directors = []
            for director in directors_:
                split = director["name"].split(" • ")
                name = split[0]
                name = name.replace("Director", "")
                name = name.replace("Secretary", "")
                age = split[-1]
                age = age.replace("Born in ", "")
                if age == name:
                    # age = None
                    continue
                occupation = director["occupation"]
                director = {
                    "director_name": name,
                    "director_date_of_birth": age,
                    "director_occupation": occupation,
                }
                directors.append(director)

        else:
            directors = []

        document["directors"] = directors
       
        document["source"] = "endole"

        postcode = row["postcode"]
        latitude = None
        longitude = None
        coordinates = None

        if not pd.isnull(latitude) and not pd.isnull(longitude):
            coordinates = [latitude, longitude]
        elif not pd.isnull(postcode) and postcode in postcode_to_lat_long:
            latitude, longitude = postcode_to_lat_long[postcode]
            coordinates = [latitude, longitude]
        elif not pd.isnull(postcode):
            coords = nomi.query_postal_code(postcode)
            if not pd.isnull(coords["latitude"]):
                latitude = coords["latitude"]
                longitude = coords["longitude"]
                coordinates = [latitude, longitude]
            
        document["latitude"] = latitude
        document["longitude"] = longitude
        document["coordinates"] = coordinates
        
        insert_document(db, collection, document, verbose=False)

        i += 1

    '''
    COMPANY CHECK AND BASE
    '''

    for source in ("companies_house", "base"):

        current_prospects = name_to_id(db, "Prospects", "name")

        prospects = []
        for region in ("yorkshire", "midlands"):
            filename = os.path.join(source, region, f"{region}_company_check.csv",)
            region_prospects = pd.read_csv(filename, index_col=0)

            if "website" not in region_prospects.columns:
                websites_filename = os.path.join(source, region, f"{region}_company_websites.csv",)
                if os.path.exists(websites_filename):
                    websites = pd.read_csv(websites_filename, index_col=0)
                    region_prospects = region_prospects.join(websites, how="inner", )

            prospects.append(region_prospects)

        prospects = pd.concat(prospects)
        prospects =  prospects = prospects[~prospects.index.duplicated(keep='first')]

        prospects = prospects.loc[~prospects.index.isin(current_prospects)]

        for prospect, row in prospects.iterrows():

            document = {
                "_key" : str(i),
                "name": prospect,
                **{
                    k.replace(".", "_"): (row[k].split(separator) if not pd.isnull(row[k]) and k in {}
                        else cast_to_float(row[k]) if not pd.isnull(row[k]) and k in {
                            "Cash_figure", 
                            "NetWorth_figure", 
                            "TotalCurrentAssets_figure",
                            "TotalCurrentLiabilities_figure",
                        }
                        else ast.literal_eval(row[k]) if not pd.isnull(row[k]) and k in {
                            "sic_codes",
                        }
                        else row[k] if not pd.isnull(row[k])
                        else None)
                        for k in set(row.index) - {"directors"}
                }, 
            }

            if not pd.isnull(row["directors"]):
                directors_ = ast.literal_eval(row["directors"])
                directors = []
                for director in directors_:

                    if pd.isnull(director["director_name"]):
                        continue
                    
                    if not pd.isnull(director["director_date_of_birth"]):
                        director["director_date_of_birth"] = insert_space(director["director_date_of_birth"], 3)

                    directors.append(director)

            else:
                directors = []

            document["directors"] = directors

            document["source"] = source

            postcode = document["postcode"]
            latitude = None
            longitude = None
            coordinates = None

            if not pd.isnull(latitude) and not pd.isnull(longitude):
                coordinates = [latitude, longitude]
            elif not pd.isnull(postcode) and postcode in postcode_to_lat_long:
                latitude, longitude = postcode_to_lat_long[postcode]
                coordinates = [latitude, longitude]
            elif not pd.isnull(postcode):
                coords = nomi.query_postal_code(postcode)
                if not pd.isnull(coords["latitude"]):
                    latitude = coords["latitude"]
                    longitude = coords["longitude"]
                    coordinates = [latitude, longitude]
        
            document["latitude"] = latitude
            document["longitude"] = longitude
            document["coordinates"] = coordinates

            assert "website" in document or source == "base", prospect

            insert_document(db, collection, document, verbose=False)

            i += 1

def add_SIC_hierarchy_to_prospects(db=None):

    if db is None:
        db = connect_to_mim_database()
  
    collection = connect_to_collection("Prospects", db, )

    get_sic_codes_query = f'''
    FOR p IN Prospects
        FILTER p.sic_codes != NULL
        RETURN {{
            _key: p._key,
            sic_codes: p.sic_codes,
        }}
    '''
    prospects = aql_query(db, get_sic_codes_query)

    class_to_sector_map = read_json("class_to_sector.json")

    for prospect in prospects:

        sic_codes = prospect["sic_codes"]
            
        sic_codes = [sic_code.split(" - ")[1]
            for sic_code in sic_codes]

        classes = set()
        groups = set()
        divisions = set()
        sectors = set()

        for sic_code in sic_codes:
            if sic_code not in class_to_sector_map:
                continue 
            classes.add(sic_code)
            groups.add(class_to_sector_map[sic_code]["group"])
            divisions.add(class_to_sector_map[sic_code]["division"])
            sectors.add(class_to_sector_map[sic_code]["sector"])

        document = {
            "_key" : prospect["_key"],
            "UK_classes": sorted(classes), 
            "UK_groups": sorted(groups), 
            "UK_divisions": sorted(divisions), 
            "UK_sectors": sorted(sectors), 
            
        }

        insert_document(db, collection, document, verbose=True)


def populate_prospects_to_uk_class(db=None):
    if db is None:
        db = connect_to_mim_database()

    collection = connect_to_collection( "ProspectsToClass", db, className="Edges")

    # member_name_to_id = name_to_id(db, "Members", "member_name")
    uk_class_to_id = name_to_id(db, "UKClasses", "name")

    # members_to_sector = read_json("member_summaries/members_to_sector.json")

    query = f'''
    FOR p IN Prospects
        FILTER p.UK_classes != NULL
        FILTER LENGTH(p.UK_classes) > 0
        
        RETURN {{
            id: p._id,
            UK_classes: p.UK_classes,
            UK_groups: p.UK_groups,
            UK_divisions: p.UK_divisions,
            UK_sectors: p.UK_sectors,
        }}
    '''

    prospects_to_sector = aql_query(db, query)

    i = 0

    for prospect_assignments in prospects_to_sector:

        assignments = {
            "sector": prospect_assignments["UK_sectors"],
            "division": prospect_assignments["UK_divisions"],
            "group": prospect_assignments["UK_groups"],
            "class": prospect_assignments["UK_classes"],
        }
       
        # TODO add other levels?
        for key in ("class", "group", "division", "sector"):
            for c in assignments[key]:

                c = f"{key}_{c}" # name is type_identifier

                document = {
                    "_key": str(i),
                    # "_from": member_name_to_id[member],
                    "_from": prospect_assignments["id"],
                    "_to": uk_class_to_id[c],
                    "name": f"in_{key}",
                }

                insert_document(db, collection, document, verbose=True)

                i += 1


def populate_member_articles(db=None):

    if db is None:
        db = connect_to_mim_database()
  
    collection = connect_to_collection("Members", db, )

    member_to_id = name_to_id(db, "Members", "id")

    article_filename = os.path.join("data_for_graph", "all_news_articles.csv")
    news_articles = pd.read_csv(article_filename, index_col=0)

    news_articles = news_articles.loc[news_articles["member_id"].isin(member_to_id)]

    member_to_article = dict()

    for _,  row in news_articles.iterrows():
        member_id = row["member_id"]
        content = row["content"]
        if pd.isnull(content):
            continue
        if member_id not in member_to_article:
            member_to_article[member_id] = []
        member_to_article[member_id].append(content)    

    corpus = {
        member_id: "\n".join(articles)
            for member_id, articles in member_to_article.items()
    }

    # write_json(corpus, "data_for_graph/corpus.json")

    for member in member_to_article:
        member_id = member_to_id[member]

        document = {
            "_id": member_id,
            "articles": member_to_article[member],
            "all_articles": corpus[member], 
        }

        insert_document(db, collection, document, upsert_key="_id")

def populate_articles(db=None):

    if db is None:
        db = connect_to_mim_database()
  
    collection = connect_to_collection("Articles", db, )

    member_to_id = name_to_id(db, "Members", "id")

    article_filename = os.path.join("data_for_graph", "all_news_articles.csv")
    news_articles = pd.read_csv(article_filename, index_col=0)

    news_articles = news_articles.loc[news_articles["member_id"].isin(member_to_id)]

    i = 0
    
    for _, row in news_articles.iterrows():

        document = {
            "_key" : str(i),
            "name": row["title"],
            **{
                k: row[k]
                for k in row.index
            }, 
        }
        insert_document(db, collection, document)

        i += 1

def populate_base_contacts(
    db=None):
    if db is None:
        db = connect_to_mim_database()

    contacts_collection = connect_to_collection("BaseContacts", db, )
    contact_works_at_collection = connect_to_collection("BaseContactWorksAt", db, className="Edges")

    contacts = pd.read_csv("base/contacts.0.csv", index_col=0)
    contacts = contacts.loc[~pd.isnull(contacts["email"])]
    contacts = contacts.loc[~contacts["is_organisation"]]
    contacts = contacts.loc[~pd.isnull(contacts["organisation_name"])]

    prospect_organisations = name_to_id(db, "Prospects", "name")

    contacts = contacts.loc[contacts["organisation_name"].isin(prospect_organisations)]

    contacts = contacts.drop_duplicates()

    i = 0
    
    for _, row in contacts.iterrows():

        document = {
            "_key" : str(i),
            "name": row["name"],
            **{
                k: row[k]
                for k in row.index
            }, 
        }
        insert_document(db, contacts_collection, document)

        document = {
            "_key" : str(i),
            "name": "works_at",
            "_from": f"BaseContacts/{i}",
            "_to": prospect_organisations[row["organisation_name"]],
            **{
                k: row[k]
                for k in {"title"}
            }, 
        }

        insert_document(db, contact_works_at_collection, document)

        i += 1

def add_addresses_to_members(db=None):
    if db is None:
        db = connect_to_mim_database()


    collection = connect_to_collection("Members", db)

    member_name_to_id = name_to_id(db, "Members", "id")


    member_address_filename = os.path.join("data_for_graph", "member_addresses.csv")
    member_address_df = pd.read_csv(member_address_filename, index_col=0)
    member_address_df = member_address_df.loc[member_address_df["id"].isin(member_name_to_id)]

    nomi = pgeocode.Nominatim("gb")

    member_addresses = dict()

    for _, row in member_address_df.iterrows():

        member_id = row["id"]
        if member_id not in member_addresses:
            member_addresses[member_id] = []

        document = {
            k: row[k]
                for k in (
                    "address_line_1", 
                    "address_line_2", 
                    "postcode",
                    # "longitude",
                    # "latitude"
                    )
            if not pd.isnull(row[k])
        }

        # keep long/lat from database if it is there
        postcode = row["postcode"]
        latitude = row["latitude"]
        longitude = row["longitude"]

        if not pd.isnull(postcode):
            coords = nomi.query_postal_code(postcode)

            if not pd.isnull(latitude):
                coords["latitude"] = latitude
            else:
                latitude = coords["latitude"]
            if not pd.isnull(longitude):
                coords["longitude"] = longitude
            else:
                longitude = coords["longitude"]
            
        if not pd.isnull(longitude):    
            document["coordinates"] = [latitude, longitude]

        document = {
            **document,
            **coords
        }
        document = {
            k: v
                for k, v in document.items()
                if isinstance(v, list) or not pd.isnull(v)
        }

        member_addresses[member_id].append(document)


    for id, addresses in member_addresses.items():

        document = {
            "_id": member_name_to_id[id],
            "addresses": addresses
        }

        insert_document(db, collection, document, upsert_key="_id", verbose=True)

def populate_graph(db=None):
    if db is None:
        db = connect_to_mim_database()
    
    # populate_members(db=db)
    # add_SIC_hierarchy_to_members(db=db)
    # add_addresses_to_members(db=db)

    # populate_sectors(db=db)
    # populate_member_sectors(db=db)
  
    # populate_commerces(db=db)
    # populate_member_commerces(db=db)

    # populate_users(db=db)
    # populate_user_works_at(db=db)
    # populate_user_follows(db=db)

    # populate_messages(db=db)

    # populate_events(db=db)
    # populate_event_sessions(db=db)
    # populate_event_attendees(db=db)
    # populate_event_session_attendees(db=db)

    # populate_uk_sector_hierarchy(db=db)
    # populate_members_to_uk_class(db=db)

    # populate_member_articles(db=db) # articles as member attributes
    # populate_articles(db=db) # articles as nodes

    # populate_prospects(db=db)
    # add_SIC_hierarchy_to_prospects(db=db)
    populate_prospects_to_uk_class(db=db)


    # populate_base_contacts(db=db)

    # populate_member_member_business(db=db)

    # populate_member_member_sector_connections(db=db)
    # populate_member_member_commerce_connections(db=db)
    # populate_member_member_event_connections(db=db)

if __name__ == "__main__":

    # host = "3.8.143.152"
    host = "127.0.0.1"
    port = 8529
    # username="root"
    username = "david"
    password = "c423612k"
    db_name = "mim_updated" 

    db = connect_to_mim_database(
        host=host, 
        port=port, 
        username=username, 
        password=password, 
        db=db_name)

    populate_graph(db=db)