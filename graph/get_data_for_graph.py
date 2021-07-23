import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))


import os 

from utils.queries import *


def get_data_for_graph(data_dir="data_for_graph"):

    '''
    RUN BATCH OF QUERIES TO BUILD FLAT FILES TO POPULATE ARANGO
    '''

    # members
    member_output_dir = os.path.join(data_dir, "members")
    os.makedirs(member_output_dir, exist_ok=True)
    membership_levels = ("Patron", "Platinum", "Gold", "Silver", "Bronze", "Digital", "Freemium")
    for membership_level in membership_levels:
        # filter_by_sector_commerce = membership_level == "Freemium"
        filter_by_sector_commerce = False
        records, _ = get_member_summaries(membership_level, filter_by_commerce_sector=filter_by_sector_commerce)
        records = pd.DataFrame(records)
        records.to_csv(f"{member_output_dir}/{membership_level}_members.csv")

    # member addresses
    records, _ = get_member_addresses()
    addresses_filename = os.path.join(data_dir, "member_addresses.csv")
    records = pd.DataFrame(records)
    records = records.sort_values(["postcode", "latitude"])
    records = records.drop_duplicates(["id", "member_name", "address_line_1", "address_line_2", "postcode"], keep="first")
    records = records.loc[records["postcode"].map(lambda p: len(p)>4)]
    records["postcode"] = records["postcode"].map(correct_postcode, na_action="ignore")
    records.to_csv(addresses_filename)

    # sectors
    records = get_all_sectors()
    sectors_filename = os.path.join(data_dir, "all_sectors.csv")
    pd.DataFrame(records).to_csv(sectors_filename)

    # commerces
    records = get_all_commerces_and_commerce_categories()
    commerces_filename = os.path.join(data_dir, "all_commerces_with_categories.csv")
    pd.DataFrame(records).to_csv(commerces_filename)

    # users
    records, _ = get_all_users()
    users_filename = os.path.join(data_dir, "all_users.csv")
    pd.DataFrame(records).to_csv(users_filename)

    # user follows
    records, _ = get_all_user_follows()
    user_follows_filename = os.path.join(data_dir, "all_user_follows.csv")
    pd.DataFrame(records).to_csv(user_follows_filename)

    # messages
    records, _ = get_all_messages()
    messages_filename = os.path.join(data_dir, "all_messages.csv")
    pd.DataFrame(records).to_csv(messages_filename)

    # events
    records, _ = get_all_events()
    events_filename = os.path.join(data_dir, "all_events.csv")
    pd.DataFrame(records).to_csv(events_filename)

    # sessions
    records, _ = get_all_event_sessions()
    event_session_filename = os.path.join(data_dir, "all_event_sessions.csv")
    pd.DataFrame(records).to_csv(event_session_filename)

    # event attendees 
    records, _ = get_event_attendees()
    events_attendees_filename = os.path.join(data_dir, "all_event_attendees.csv")
    pd.DataFrame(records).to_csv(events_attendees_filename)

    # event session attendees 
    records, _ = get_event_session_attendees()
    event_session_attendees_filename = os.path.join(data_dir, "all_event_session_attendees.csv")
    pd.DataFrame(records).to_csv(event_session_attendees_filename)

    # articles
    records, _ = get_all_news_articles()
    news_article_filename = os.path.join(data_dir, "all_news_articles.csv")
    records = pd.DataFrame(records)
    records["content"] = records["content"].map(remove_html_tags, na_action="ignore")
    records.to_csv(news_article_filename)


if __name__ == "__main__":
    get_data_for_graph()