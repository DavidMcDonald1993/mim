import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from collections import defaultdict
from itertools import count, product


import matplotlib.pyplot as plt

import pandas as pd
import networkx as nx

# from utils.mysql_utils import mysql_query
from utils.io import write_json
from utils.queries import *


def main():
    
    records, cols = get_company_position_counts()
    pd.DataFrame(records).to_csv("company_position_counts.csv")


    # records, cols = get_all_commerce_relations()
    # pd.DataFrame(records).to_csv("all_member_commerce_relations.csv")

    # write_json(records, "all_member_commerce_relations.json")

    # for record in records[:5]:
    #     print (record)
    # print (len(records))

    # records, cols = get_member_prospecting_summaries()

    # print (records[0])

    # pd.DataFrame(records, ).to_csv("member_prospecting_summaries.csv")

    # unannotated_members = get_unannotated_members()
    # unannotated_members = pd.DataFrame(unannotated_members)
    # unannotated_members.to_csv("unannotated_members.tsv", sep="\t")

    # sector_counts, cols = get_sector_counts()
    # sector_counts = pd.DataFrame(sector_counts)
    # sector_counts.to_csv("sector_counts.tsv", sep="\t")

    # commerces_counts, cols = get_commerces_counts()
    # commerces_counts = pd.DataFrame(commerces_counts)
    # commerces_counts.to_csv("commerces_counts.tsv", sep="\t")

    # member_summaries, cols = get_member_summaries()
    # member_summaries = pd.DataFrame(member_summaries)
    # member_summaries.to_csv("member_summaries.tsv", sep="\t")
   
    # messages, cols = get_all_messages()
    # messages = pd.DataFrame(messages)
    # messages.to_csv("messages.tsv", sep="\t")
   
    # live_chats, cols = get_all_live_chat_messages()
    # live_chats = pd.DataFrame(live_chats)
    # live_chats.to_csv("live_chats.tsv", sep="\t")

    # all_news_articles, cols = get_all_news_articles()
    # all_news_articles = pd.DataFrame(all_news_articles)
    # all_news_articles.to_csv("all_news_articles.tsv", sep="\t")

    # news_article_summary, cols = get_news_article_summary()
    # news_article_summary = pd.DataFrame(news_article_summary)
    # news_article_summary.to_csv("news_article_summary.tsv", sep="\t")

    # users, cols = get_all_users()
    # users = pd.DataFrame(users)
    # users.to_csv("all_users.tsv", sep="\t")

    # for s in member_summaries[:5]:
        # print (s)
    # member_summaries = pd.DataFrame(member_summaries,
    #       columns=["member", "sectors", "buys", "sells"])

    # g = construct_member_commerce_bipartite_graph()
    # g = construct_member_member_graph()

    # print (len(g), len(g.edges), nx.number_weakly_connected_components(g))

    # nx.write_gml(g, "current_member_member_links.gml")
    # nx.write_edgelist(g, "current_member_member_links.tsv", 
    #     delimiter="\t", data=["commerce"])

    # from networkx.drawing.nx_agraph import to_agraph

    # a = to_agraph(g)

    # a.layout("dot")
    # a.graph_attr.update(dpi=200)
    # a.draw("current_member_commerces.png", )

if __name__ == "__main__":
    main()