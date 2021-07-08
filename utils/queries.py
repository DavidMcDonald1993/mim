import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir,)))

import pandas as pd

from utils.mysql_utils import mysql_query, sanitise_args, separator
from utils.scraping_utils import remove_html_tags


# def get_all_commerce_relations():

#     sells_query = '''
#     SELECT m.name, 
#         JSON_ARRAYAGG(
#             JSON_OBJECT(
#                 'commerce_name', c.name,
#                 'commerce_category', mcc.name
#             )
#         ) AS 'sells'
#     FROM members AS `m`
#     INNER JOIN member_sells_commerce AS msc
#         ON (m.id=msc.member_id)
#     INNER JOIN member_commerces AS `c` 
#         ON (msc.member_commerce_id=c.id)
#     INNER JOIN member_commerce_categories AS `mcc`
#         ON (c.category_id=mcc.id)
#     GROUP BY m.name
#     '''

#     buys_query = '''
#     SELECT m.name, 
#         JSON_ARRAYAGG(
#             JSON_OBJECT(
#                 'commerce_name', c.name,
#                 'commerce_category', mcc.name
#             )
#         ) AS 'buys'
#     FROM members AS `m`
#     INNER JOIN member_buys_commerce AS mbc
#         ON (m.id=mbc.member_id)
#     INNER JOIN member_commerces AS `c` 
#         ON (mbc.member_commerce_id=c.id)
#     INNER JOIN member_commerce_categories AS `mcc`
#         ON (c.category_id=mcc.id)
#     GROUP BY m.name
#     '''

#     query = f'''
#     SELECT 
#         sells_table.name,
#         sells_table.sells,
#         buys_table.buys
#     FROM members AS m
#     LEFT JOIN ({sells_query}) AS sells_table
#         ON (m.name=sells_table.name)
#     LEFT JOIN ({buys_query}) AS buys_table
#         ON (m.name=buys_table.name)
#     WHERE sells IS NOT NULL OR buys IS NOT NULL 
#     '''

#     records, cols = mysql_query(query, to_json=True, return_cols=True)
#     return records, cols

def get_member_commerces_flat(membership_levels):
    membership_levels = sanitise_args(membership_levels)

    commerce_queries = {}
    for commerce_type in ("buys", "sells"):
        commerce_queries[commerce_type] = f'''
        SELECT 
            s.name AS member_sector,
            m.name AS member_name,
            '{commerce_type}' AS commerce_type,
            c.name AS commerce_name,
            cat.name AS commerce_category
        FROM members AS m
        INNER JOIN member_{commerce_type}_commerce AS mb
            ON (m.id=mb.member_id)
        INNER JOIN member_commerces AS c
            ON (mb.member_commerce_id=c.id)
        INNER JOIN member_commerce_categories AS cat
            ON (c.category_id=cat.id)
        INNER JOIN member_sector AS ms
            ON (m.id=ms.member_id)
        INNER JOIN sectors AS s 
            ON (ms.sector_id=s.id)
        '''

        if membership_levels is not None:
            commerce_queries[commerce_type] = f'''
            {commerce_queries[commerce_type]}
            INNER JOIN memberships 
                ON (m.membership_id=memberships.id)
            WHERE {f"memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
            else f"memberships.name IN {membership_levels}"}
            '''

    query = f'''
    {commerce_queries["buys"]}
    UNION
    {commerce_queries["sells"]}
    '''

    records, cols = mysql_query(query, to_json=True, return_cols=True)
    return records, cols

def get_member_sectors(membership_levels):
    membership_levels = sanitise_args(membership_levels)

    query = '''
    SELECT m.name AS member_name, sectors.name AS sector_name
    FROM members AS m
    INNER JOIN member_sector 
        ON (m.id=member_sector.member_id) 
    INNER JOIN sectors 
        ON (member_sector.sector_id=sectors.id)
    '''
    if membership_levels is not None:
        query = f'''
        {query}
        INNER JOIN memberships 
            ON (m.membership_id=memberships.id)
        WHERE {f"memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
        else f"memberships.name IN {membership_levels}"}
        '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_member_sector_counts(membership_levels):
    '''
    number of sectors each member is tagged for
    '''
    membership_levels = sanitise_args(membership_levels)
          
        #     JSON_ARRAYAGG(
        #     JSON_OBJECT(
        #         'sector_name', s.name
        #     )
        # ) AS sectors

    query = f'''
    SELECT 
        m.name AS member_name, 
        COUNT(ms.sector_id) AS num_sectors,
        GROUP_CONCAT(
            s.name 
            ORDER BY s.name
            SEPARATOR {separator}) AS sectors
    FROM members AS m
    INNER JOIN member_sector AS ms
        ON (m.id=ms.member_id) 
    INNER JOIN sectors AS s
        ON (ms.sector_id=s.id)
    '''
    if membership_levels is not None:
        query = f'''
        {query}
        INNER JOIN memberships 
            ON (m.membership_id=memberships.id)
        WHERE {f"memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
        else f"memberships.name IN {membership_levels}"}
        '''
    query = f'''
    {query}
    GROUP BY m.name
    ORDER BY num_sectors DESC
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_member_summaries(
    membership_levels=None, 
    filter_by_commerce_sector=False):
   
    membership_levels = sanitise_args(membership_levels)

    query = f'''
    SELECT DISTINCT
        m.id,
        m.name AS member_name, 
        m.employees, 
        m.turnover,
        memberships.name AS `membership_level`,
        m.nature_of_business,
        m.about_company,
        m.website,
        m.services,
        m.seo_description,
        m.youtube_video_id,
        m.facebook_page_link,
        m.twitter_page_link,
        m.google_page_link,
        tenancies,
        sector_link.sectors, 
        buys, 
        sells,
        badges,
        accreditations
    FROM members as `m`
    INNER JOIN memberships
        ON (m.membership_id=memberships.id)
    LEFT JOIN (
        SELECT ms.member_id AS member_id, 
            GROUP_CONCAT(
                    s.name
                    ORDER BY s.name
                    SEPARATOR '{separator}'
            ) as sectors
        FROM member_sector AS `ms`
        INNER JOIN sectors AS `s`
            ON (ms.sector_id=s.id)
        GROUP BY member_id
    ) AS sector_link ON (m.id=sector_link.member_id) 
    LEFT JOIN (
        SELECT mbc.member_id AS member_id, 
            GROUP_CONCAT(
                    bc.name
                    ORDER BY bc.name
                    SEPARATOR '{separator}'
            ) AS `buys`
        FROM member_buys_commerce AS mbc
        INNER JOIN member_commerces AS `bc` 
            ON (mbc.member_commerce_id=bc.id)
        GROUP BY member_id
    ) AS buys_link ON (m.id=buys_link.member_id)
    LEFT JOIN (
        SELECT msc.member_id AS member_id, 
            GROUP_CONCAT(
                sc.name
                ORDER BY sc.name
                SEPARATOR '{separator}'
            ) AS `sells`
        FROM member_sells_commerce AS msc
        INNER JOIN member_commerces AS `sc` 
            ON (msc.member_commerce_id=sc.id)
        GROUP BY member_id
    ) AS sells_link ON (m.id=sells_link.member_id)
    LEFT JOIN (
        SELECT mt.member_id, 
            GROUP_CONCAT(
                    t.name
                    ORDER BY t.name
                    SEPARATOR '{separator}'
            ) AS `tenancies`
        FROM member_tenant AS mt
        INNER JOIN tenants AS t
            ON (mt.tenant_id=t.id)
        WHERE mt.status='active'
        AND t.name <> 'Made Futures'
        GROUP BY member_id
    ) AS tenancy_link ON (m.id=tenancy_link.member_id)
    LEFT JOIN (
        SELECT mb.member_id, 
            GROUP_CONCAT(
                b.name
                ORDER BY b.name
                SEPARATOR '{separator}'
            ) AS badges
        FROM badge_member AS mb
        INNER JOIN badges as b
            ON (b.id=mb.badge_id)
        GROUP BY member_id
    ) AS badge_link ON (m.id=badge_link.member_id)
    LEFT JOIN (
        SELECT ma.member_id, 
            GROUP_CONCAT(
                a.name
                ORDER BY a.name
                SEPARATOR '{separator}'
            ) AS `accreditations`
        FROM accreditation_member as ma
        INNER JOIN accreditations as a
            ON (a.id=ma.accreditation_id)
        GROUP BY member_id
    ) AS accreditation_link ON (m.id=accreditation_link.member_id)
    WHERE 
        m.name IS NOT NULL
        AND m.name NOT LIKE '%Made Futures%'
        AND tenancies IS NOT NULL
        {f" AND memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
        else f"AND memberships.name IN {membership_levels}" if isinstance(membership_levels, tuple)
        else ""}
        {"AND (sectors IS NOT NULL OR buys IS NOT NULL OR sells IS NOT NULL)" if filter_by_commerce_sector else ""}
    ORDER BY member_name
    '''


    # query = f'''
    # SELECT 
    #     m.id AS database_id,
    #     m.name AS member_name, 
    #     c.address_line_1,
    #     c.address_line_2,
    #     c.postcode,
    #     c.latitude,
    #     c.longitude,
    #     m.employees, 
    #     m.turnover,
    #     memberships.name AS `membership_level`,
    #     m.nature_of_business,
    #     m.about_company,
    #     m.website,
    #     m.services,
    #     m.seo_description,
    #     m.youtube_video_id,
    #     m.facebook_page_link,
    #     m.twitter_page_link,
    #     m.google_page_link,
    #     tenancies,
    #     sectors, 
    #     buys, 
    #     sells,
    #     badges,
    #     accreditations
    # FROM members as `m`
    # INNER JOIN memberships
    #     ON (m.membership_id=memberships.id)
    # INNER JOIN member_contact_details AS c
    #     ON (m.id=c.member_id)
    # LEFT JOIN (
    #     SELECT ms.member_id AS member_id, 
    #         JSON_ARRAYAGG(
    #             JSON_OBJECT(
    #                 'sector_name', s.name
    #             )
    #         ) as sectors
    #     FROM member_sector AS `ms`
    #     INNER JOIN sectors AS `s`
    #         ON (ms.sector_id=s.id)
    #     GROUP BY member_id
    # ) AS sector_link ON (m.id=sector_link.member_id) 
    # LEFT JOIN (
    #     SELECT mbc.member_id AS member_id, 
    #         JSON_ARRAYAGG(
    #             JSON_OBJECT(
    #                 'buys_commerce_name', bc.name
    #             )
    #         ) AS `buys`
    #     FROM member_buys_commerce AS mbc
    #     INNER JOIN member_commerces AS `bc` 
    #         ON (mbc.member_commerce_id=bc.id)
    #     GROUP BY member_id
    # ) AS buys_link ON (m.id=buys_link.member_id)
    # LEFT JOIN (
    #     SELECT msc.member_id AS member_id, 
    #         JSON_ARRAYAGG(
    #             JSON_OBJECT(
    #                 'sells_commerce_name', sc.name
    #             )
    #         ) AS `sells`
    #     FROM member_sells_commerce AS msc
    #     INNER JOIN member_commerces AS `sc` 
    #         ON (msc.member_commerce_id=sc.id)
    #     GROUP BY member_id
    # ) AS sells_link ON (m.id=sells_link.member_id)
    # LEFT JOIN (
    #     SELECT mt.member_id, 
    #         JSON_ARRAYAGG(
    #             JSON_OBJECT(
    #                 'tenancy', t.name
    #             )
    #         ) AS `tenancies`
    #     FROM member_tenant AS mt
    #     INNER JOIN tenants AS t
    #         ON (mt.tenant_id=t.id)
    #     WHERE mt.status='active'
    #     AND t.name <> 'Made Futures'
    #     GROUP BY member_id
    # ) AS tenancy_link ON (m.id=tenancy_link.member_id)
    # LEFT JOIN (
    #     select mb.member_id, 
    #         JSON_ARRAYAGG(
    #             JSON_OBJECT(
    #                 'badge_name', b.name
    #             )
    #         ) AS badges
    #     FROM badge_member AS mb
    #     INNER JOIN badges as b
    #         ON (b.id=mb.badge_id)
    #     GROUP BY member_id
    # ) AS badge_link ON (m.id=badge_link.member_id)
    # LEFT JOIN (
    #     SELECT ma.member_id, 
    #         JSON_ARRAYAGG(
    #             JSON_OBJECT(
    #                 'accreditation_name', a.name
    #             )
    #         ) AS `accreditations`
    #     FROM accreditation_member as ma
    #     INNER JOIN accreditations as a
    #         ON (a.id=ma.accreditation_id)
    #     GROUP BY member_id
    # ) AS accreditation_link ON (m.id=accreditation_link.member_id)
    # WHERE 
    #     m.name IS NOT NULL
    #     AND c.postcode IS NOT NULL
    #     AND tenancies IS NOT NULL
    #     {f" AND memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
    #     else f"AND memberships.name IN {membership_levels}" if isinstance(membership_levels, tuple)
    #     else ""}
    # ORDER BY member_name
    # '''


    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_member_addresses():

    query = f'''
    SELECT 
        m.id,
        m.name AS member_name, 
        c.address_line_1,
        c.address_line_2,
        c.postcode,
        c.latitude,
        c.longitude
    FROM 
        members AS m
        INNER JOIN member_contact_details AS c
            ON (m.id=c.member_id)
    WHERE c.postcode IS NOT NULL
        AND m.name NOT LIKE '%Made Futures%'
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_all_messages():

    # query = '''
    # SELECT 
    #     m.name AS sender_member_name,
    #     u.full_name AS sender_name,
    #     u.company_position AS sender_company_position,
    #     JSON_ARRAYAGG(
    #         JSON_OBJECT(
    #             'name', recipients.full_name,
    #             'company', recipient_members.name,
    #             'position', recipients.company_position
    #         )
    #     ) AS all_recipients,
    #     messages.subject,
    #     messages.message,
    #     messages.created_at
    # FROM members AS m
    # INNER JOIN users AS u
    #     ON (m.id=u.member_id)
    # INNER JOIN messages
    #     ON (u.id=messages.sender_id)
    # INNER JOIN message_user AS `mu`
    #     ON (messages.id=mu.message_id)
    # INNER JOIN users AS recipients
    #     ON (mu.user_id=recipients.id)
    # INNER JOIN members AS recipient_members
    #     ON (recipients.member_id=recipient_members.id)
    # WHERE m.name <> "test"
    # GROUP BY sender_member_name, sender_name,
    #     sender_company_position, subject, message, created_at
    # '''

    query = '''
    SELECT 
        messages.id,
        m.id AS sender_member_id,
        m.name AS sender_member_name,
        u.id AS sender_id,
        CONCAT(u.first_name, " ", u.last_name) AS sender_name,
        u.company_position AS sender_company_position,
        recipients.id AS recipient_id,
        CONCAT(recipients.first_name, " ", recipients.last_name) AS recipient_name,
        recipient_members.id AS recipient_member_id,
        recipient_members.name AS recipient_member_name,
        recipients.company_position AS recipient_company_position,
        messages.subject,
        messages.message,
        messages.created_at
    FROM members AS m
    INNER JOIN users AS u
        ON (m.id=u.member_id)
    INNER JOIN messages
        ON (u.id=messages.sender_id)
    INNER JOIN message_user AS `mu`
        ON (messages.id=mu.message_id)
    INNER JOIN users AS recipients
        ON (mu.user_id=recipients.id)
    INNER JOIN members AS recipient_members
        ON (recipients.member_id=recipient_members.id)
    WHERE m.name <> "test"
    AND m.name <> recipient_members.name
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

# def get_all_live_chat_messages():

#     query = '''
#     SELECT
#         m.live_chat_conversation_id AS chat_id,
#         COUNT(DISTINCT(sender.member_id)) AS num_participating_members,
#         GROUP_CONCAT(DISTINCT(sender_members.name)) AS participating_members,
#         COUNT(DISTINCT(m.user_id)) AS num_participating_users,
#         GROUP_CONCAT(DISTINCT(CONCAT(sender.full_name, 
#             " (", sender.company_position,  "@", sender_members.name, ")" ))) AS participating_users,
#         COUNT(m.message) AS num_messages_in_chat,
#         JSON_ARRAYAGG(
#             JSON_OBJECT(
#                 'sender', sender.full_name,
#                 'sender_company', sender_members.name,
#                 'sender_role', sender.company_position,
#                 'message', m.message,
#                 'sent_at', m.created_at
#             )
#         ) AS all_messages
        
#     FROM live_chat_messages AS m
#     INNER JOIN users AS sender
#         ON (m.user_id=sender.id)
#     INNER JOIN members AS sender_members
#         ON (sender.member_id=sender_members.id)
#     INNER JOIN live_chat_conversations AS c
#         ON (m.live_chat_conversation_id=c.id)
#     GROUP BY chat_id
#     HAVING COUNT(DISTINCT m.user_id) > 1
#     ORDER BY num_participating_members DESC, num_messages_in_chat DESC
#     '''

#     records, cols = mysql_query(query, return_cols=True, to_json=True)
#     return records, cols

def get_all_news_articles():

    query = f'''
    SELECT 
        a.id,
        m.id AS member_id,
        m.name AS member_name,
        c.title AS category_name,
        tag_link.all_tags,
        a.title,
        a.content,
        a.seo_title,
        a.seo_description
    FROM members AS m
    INNER JOIN news_articles AS a
        ON (m.id=a.member_id)
    LEFT JOIN article_categories AS c
        ON (a.category_id=c.id)
    LEFT JOIN (
        SELECT 
            article_id, 
            GROUP_CONCAT(
                title
                ORDER BY title
                SEPARATOR '{separator}') AS all_tags
        FROM article_tags
        GROUP BY article_id
    ) AS tag_link ON (tag_link.article_id=a.id)
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_news_article_summary():

    query = f'''
    SELECT m.name AS member_name,
        GROUP_CONCAT(
            DISTINCT c.title
            ORDER BY c.title
            SEPARATOR '{separator}') AS all_category_names,
        COUNT(a.title) AS num_articles
    FROM members AS m
    INNER JOIN news_articles AS a
        ON (m.id=a.member_id)
    LEFT JOIN article_categories AS c
        ON (a.category_id=c.id)
    GROUP BY member_name
    ORDER BY num_articles DESC
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_all_users():

    # LEFT JOIN (
    #     SELECT mu.user_id, 
    #         JSON_ARRAYAGG(
    #             JSON_OBJECT(
    #                 'member_name', mem.name
    #             )
    #         ) AS follows_companies
    #     FROM member_user AS mu
    #     INNER JOIN members AS mem
    #         ON (mu.member_id=mem.id)
    #     GROUP BY user_id
    # ) AS follows ON (u.id=follows.user_id)

    query = '''
    SELECT
        u.id,
        CONCAT(u.first_name, " ", u.last_name) AS full_name,
        u.email AS email,
        m.id AS company_id,
        m.name AS company_name,
        u.company_position,
        u.company_role
    FROM users as u
    INNER JOIN members AS m
        ON (u.member_id=m.id)
    WHERE u.deleted_at IS NULL
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_all_user_follows():

    query = '''
    SELECT 
        u.id,
        CONCAT(u.first_name, " ", u.last_name) AS full_name,
        u.member_id AS employer_id,
        members.name AS company_name,
        m.id AS followed_member_id,
        m.name AS followed_member
    FROM users AS u
    INNER JOIN members
        ON (u.member_id=members.id)
    INNER JOIN member_user AS mu 
        ON (u.id=mu.user_id)
    INNER JOIN members AS m
        ON (m.id=mu.member_id)

    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols



# def get_member_prospecting_summaries():

#     query = f'''
#     SELECT m.name, m.employees, m.turnover,
#         c.address_line_1, c.address_line_2, c.postcode
#     FROM members AS m
#     INNER JOIN member_contact_details AS c
#         ON (m.id=c.member_id)
#     WHERE name IS NOT NULL 
#     AND name <> '' 
#     AND name <> 'test'
#     AND name <> 'nan'
#     AND postcode IS NOT NULL
#     '''

#     records, cols = mysql_query(query, return_cols=True, to_json=True)
#     return records, cols

def get_sector_counts(membership_levels=None):
    membership_levels = sanitise_args(membership_levels)

    query = f'''
    SELECT s.name AS sector_name, 
        count(DISTINCT m.member_name) AS num_members,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'member_name', m.member_name,
                'tenant_name', m.tenant_name,
                'membership_level', m.membership_level
            )
        ) AS members
    FROM sectors AS s
    LEFT JOIN (
        SELECT ms.sector_id,
            m.name AS member_name,
            t.name AS tenant_name, 
            memberships.name AS membership_level
        FROM member_sector AS ms
        INNER JOIN members AS m
            ON (ms.member_id=m.id)
        INNER JOIN member_tenant AS mt
            ON (m.id=mt.member_id)
        INNER JOIN tenants AS t
            ON (t.id=mt.tenant_id)
        INNER JOIN memberships 
            ON (m.membership_id=memberships.id)
        WHERE m.name IS NOT NULL
            AND m.name <> 'test'
            AND mt.status = 'active'
            {f" AND memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
                else f"AND memberships.name IN {membership_levels}" if isinstance(membership_levels, tuple)
                else ""}

    ) AS m
        ON (s.id=m.sector_id)
    
    GROUP BY sector_name
    ORDER BY num_members DESC
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

# def get_sells_commerces_counts():

#     query = '''
#     SELECT c.name AS commerce_name, 
#         COUNT(DISTINCT m.name) AS num_members_sell,
#         JSON_ARRAYAGG(
#             JSON_OBJECT(
#                 'member_name', m.name,
#                 'tenant_name', m.tenant_name,
#                 'membership_level', m.membership_level
#             )
#         ) AS members
#     FROM member_commerces AS c
#     LEFT JOIN (
#         SELECT ms.member_commerce_id,
#             m.name,
#             t.name AS tenant_name,
#             memberships.name AS membership_level
#         FROM member_sells_commerce AS ms
#         INNER  JOIN members AS m
#             ON (ms.member_id=m.id)
#         INNER JOIN member_tenant AS mt
#             ON (m.id=mt.member_id)
#         INNER JOIN tenants AS t
#             ON (t.id=mt.tenant_id)
#         INNER JOIN memberships 
#             ON (m.membership_id=memberships.id)
#         WHERE m.name IS NOT NULL
#             AND m.name <> 'test'
#             AND mt.status = 'active'
#     ) AS m 
#         ON (c.id=m.member_commerce_id)
#     GROUP BY commerce_name
#     ORDER BY num_members_sell DESC

#     '''

#     return mysql_query(query, to_json=True)


# def get_sells_commerce_category_counts():

#     query = '''
#     SELECT c.name AS commerce_category, 
#         COUNT(DISTINCT m.member_name) AS num_members_sell,
#         JSON_ARRAYAGG(
#             JSON_OBJECT(
#                 'commerce_name', m.commerce_name,
#                 'member_name', m.member_name,
#                 'tenant_name', m.tenant_name,
#                 'membership_level', m.membership_level
#             )
#         ) AS members
#     FROM member_commerce_categories AS c
#     LEFT JOIN (
#         SELECT c.category_id,
#             c.name AS commerce_name,
#             m.name AS member_name,
#             t.name AS tenant_name,
#             memberships.name AS membership_level
#         FROM member_commerces AS c
#         INNER JOIN member_sells_commerce AS ms
#             ON(c.id=ms.member_commerce_id)
#         INNER JOIN members AS m
#             ON (ms.member_id=m.id)
#         INNER JOIN member_tenant AS mt
#             ON (m.id=mt.member_id)
#         INNER JOIN tenants AS t
#             ON (t.id=mt.tenant_id)
#         INNER JOIN memberships 
#             ON (m.membership_id=memberships.id)
#         WHERE m.name IS NOT NULL
#             AND m.name <> 'test'
#             AND mt.status = 'active'
#     ) AS m 
#         ON (c.id=m.category_id)
#     GROUP BY commerce_category
#     ORDER BY num_members_sell DESC

#     '''

#     return mysql_query(query, to_json=True)


# def get_buys_commerces_counts():

#     query = '''
#     SELECT c.name AS commerce_name, 
#         COUNT(DISTINCT m.name) AS num_members_buy,
#         JSON_ARRAYAGG(
#             JSON_OBJECT(
#                 'member_name', m.name,
#                 'tenant_name', m.tenant_name,
#                 'membership_level', m.membership_level
#             )
#         ) AS members
#     FROM member_commerces AS c
#     LEFT JOIN (
#         SELECT ms.member_commerce_id,
#             m.name,
#             t.name AS tenant_name,
#             memberships.name AS membership_level
#         FROM member_buys_commerce AS ms
#         INNER  JOIN members AS m
#             ON (ms.member_id=m.id)
#         INNER JOIN member_tenant AS mt
#             ON (m.id=mt.member_id)
#         INNER JOIN tenants AS t
#             ON (t.id=mt.tenant_id)
#         INNER JOIN memberships 
#             ON (m.membership_id=memberships.id)
#         WHERE m.name IS NOT NULL
#             AND m.name <> 'test'
#             AND mt.status = 'active'
#     ) AS m 
#         ON (c.id=m.member_commerce_id)
#     GROUP BY commerce_name
#     ORDER BY num_members_buy DESC

#     '''

#     return mysql_query(query, to_json=True)

# def get_buys_commerce_category_counts():

#     query = '''
#     SELECT c.name AS commerce_category, 
#         COUNT(DISTINCT m.member_name) AS num_members_buy,
#         JSON_ARRAYAGG(
#             JSON_OBJECT(
#                 'commerce_name', m.commerce_name,
#                 'member_name', m.member_name,
#                 'tenant_name', m.tenant_name,
#                 'membership_level', m.membership_level
#             )
#         ) AS members
#     FROM member_commerce_categories AS c
#     LEFT JOIN (
#         SELECT c.category_id,
#             c.name AS commerce_name,
#             m.name AS member_name,
#             t.name AS tenant_name,
#             memberships.name AS membership_level
#         FROM member_commerces AS c
#         INNER JOIN member_buys_commerce AS ms
#             ON(c.id=ms.member_commerce_id)
#         INNER JOIN members AS m
#             ON (ms.member_id=m.id)
#         INNER JOIN member_tenant AS mt
#             ON (m.id=mt.member_id)
#         INNER JOIN tenants AS t
#             ON (t.id=mt.tenant_id)
#         INNER JOIN memberships 
#             ON (m.membership_id=memberships.id)
#         WHERE m.name IS NOT NULL
#             AND m.name <> 'test'
#             AND mt.status = 'active'
#     ) AS m 
#         ON (c.id=m.category_id)
#     GROUP BY commerce_category
#     ORDER BY num_members_buy DESC

#     '''

#     return mysql_query(query, to_json=True)

def get_commerces_counts(membership_levels=None):
    membership_levels = sanitise_args(membership_levels)

    commerce_queries = {}
    for commerce_type in ("sells", "buys"):

        commerce_queries[commerce_type] = f'''
        SELECT 
            c.id, 
            IFNULL(counts.num_members_{commerce_type}, 0) AS num_members_{commerce_type},
            counts.members_that_{commerce_type}
        FROM member_commerces AS c
        LEFT JOIN (
            SELECT  
                ms.member_commerce_id,
                COUNT(ms.member_id) AS num_members_{commerce_type},
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'member_name', m.name,
                        'tenant_name', t.name,
                        'membership_level', memberships.name
                    )
                ) AS members_that_{commerce_type}
            FROM 
                member_{commerce_type}_commerce AS ms
            INNER JOIN members AS m
                ON (m.id=ms.member_id)
            INNER JOIN member_tenant AS mt
                ON (m.id=mt.member_id)
            INNER JOIN tenants AS t
                ON (t.id=mt.tenant_id)
            INNER JOIN memberships 
                ON (m.membership_id=memberships.id)
            WHERE m.name IS NOT NULL
                AND m.name <> 'test'
                AND mt.status = 'active'
                {f" AND memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
                    else f"AND memberships.name IN {membership_levels}" if isinstance(membership_levels, tuple)
                else ""}
            GROUP BY ms.member_commerce_id
        ) AS counts
            ON (c.id=counts.member_commerce_id)
        '''

    query = f'''
    SELECT 
        c.name AS commerce_name,
        cat.name AS category_name,
        sells.num_members_sells + buys.num_members_buys AS total_count, 
        sells.num_members_sells, 
        buys.num_members_buys,
        sells.members_that_sells,
        buys.members_that_buys

    FROM 
        member_commerces AS c
    LEFT JOIN member_commerce_categories AS cat
        ON (c.category_id=cat.id)
    INNER JOIN ({commerce_queries["sells"]}) AS sells
        ON (c.id=sells.id)
    INNER JOIN ({commerce_queries["buys"]}) AS buys
        ON (c.id=buys.id)
    ORDER BY total_count DESC
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_commerces_category_counts(membership_levels=None):
    membership_levels = sanitise_args(membership_levels)

    commerce_queries = {}
    for commerce_type in ("sells", "buys"):

        commerce_queries[commerce_type] = f'''
        SELECT 
            cat.id, 
            IFNULL(counts.num_unique_members_{commerce_type}_category, 0) AS num_unique_members_{commerce_type}_category,
            counts.members_that_{commerce_type}_category
        FROM member_commerce_categories AS cat
        LEFT JOIN (
            SELECT  
                cat.id,
                COUNT(DISTINCT ms.member_id) AS num_unique_members_{commerce_type}_category,
                JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'commerce_name', c.name,
                        'member_name', m.name,
                        'tenant_name', t.name,
                        'membership_level', memberships.name
                    )
                ) AS members_that_{commerce_type}_category
            FROM 
                member_commerce_categories AS cat
            INNER JOIN member_commerces AS c
                ON (cat.id=c.category_id)
            INNER JOIN member_{commerce_type}_commerce AS ms
                ON (c.id=ms.member_commerce_id)
            INNER JOIN members AS m
                ON (m.id=ms.member_id)
            INNER JOIN member_tenant AS mt
                ON (m.id=mt.member_id)
            INNER JOIN tenants AS t
                ON (t.id=mt.tenant_id)
            INNER JOIN memberships 
                ON (m.membership_id=memberships.id)
            WHERE m.name IS NOT NULL
                AND m.name <> 'test'
                AND mt.status = 'active'
                {f" AND memberships.name='{membership_levels}'" if isinstance(membership_levels, str)
                    else f"AND memberships.name IN {membership_levels}" if isinstance(membership_levels, tuple)
                else ""}
            GROUP BY cat.id
        ) AS counts
            ON (cat.id=counts.id)
        '''

    query = f'''
    SELECT 
        cat.name AS category_name,
        sells.num_unique_members_sells_category + buys.num_unique_members_buys_category AS total_count, 
        sells.num_unique_members_sells_category, 
        buys.num_unique_members_buys_category,
        sells.members_that_sells_category,
        buys.members_that_buys_category

    FROM  member_commerce_categories AS cat
    INNER JOIN ({commerce_queries["sells"]}) AS sells
        ON (cat.id=sells.id)
    INNER JOIN ({commerce_queries["buys"]}) AS buys
        ON (cat.id=buys.id)
    ORDER BY total_count DESC
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_unannotated_members():

    query = '''
    SELECT m.name
    FROM members AS m
    WHERE m.id NOT IN (
        SELECT member_id FROM member_sells_commerce
        UNION
        SELECT member_id FROM member_buys_commerce
    )
    '''
    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_company_position_counts():

    query = f'''
    SELECT company_position,
    COUNT(id) AS count
    FROM users
    WHERE company_position IS NOT NULL
    GROUP BY company_position
    ORDER BY count DESC
    '''

    return mysql_query(query, return_cols=True, to_json=True)


def get_member_articles_last_six_months(num_months=6):

    query = f'''
    SELECT 
        m.name AS member_name,
        memberships.name AS `membership_level`,
        COUNT(c.title) AS num_articles_in_six_months,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'article_category', c.title,
                'article_tags', tag_link.all_tags,
                'article_title', a.title,
                'content', a.content
            )
        )
       
    FROM members AS m
    INNER JOIN memberships
        ON (m.membership_id=memberships.id)
    LEFT JOIN news_articles AS a
        ON (m.id=a.member_id)
    LEFT JOIN article_categories AS c
        ON (a.category_id=c.id)
    LEFT JOIN (
        SELECT 
            article_id, 
            GROUP_CONCAT(
                title
                ORDER BY title
                SEPARATOR '{separator}') AS all_tags
        FROM article_tags
        GROUP BY article_id
    ) AS tag_link ON (tag_link.article_id=a.id)
    WHERE a.publish_at > DATE_SUB(now(), INTERVAL {num_months} MONTH)
    GROUP BY member_name, membership_level
    ORDER BY num_articles_in_six_months DESC
    '''

    return mysql_query(query, to_json=True)

def get_members_not_publish(num_months=6):

    query = f'''
    SELECT 
        m.name AS member_name,
        memberships.name AS `membership_level`
       
    FROM members AS m
    INNER JOIN memberships
        ON (m.membership_id=memberships.id)

    WHERE m.id NOT IN (
        SELECT m.id
        FROM members AS m
        INNER JOIN  news_articles AS a
            ON (m.id=a.member_id)
        WHERE a.publish_at > DATE_SUB(now(), INTERVAL {num_months} MONTH)
    )   
    '''

    return mysql_query(query, to_json=True)

def get_member_regions():

    query = '''
    SELECT 
        m.name AS 'member_name',
        t.name AS 'tenant'
    FROM members AS m
    INNER JOIN member_tenant as mt
        ON (m.id=mt.member_id)
    INNER JOIN tenants AS t
        ON (mt.tenant_id=t.id)
    WHERE mt.status='active'

    '''

    return mysql_query(query, to_json=True)

def get_all_commerces_and_commerce_categories():

    query = '''
    SELECT 
        c.id,
        c.name AS commerce_name,
        cat.name AS commerce_category
    FROM member_commerces AS c
    LEFT JOIN member_commerce_categories AS cat
        ON (c.category_id=cat.id)
    '''
    return mysql_query(query, to_json=True)

def get_all_sectors():

    query = '''
    SELECT 
        id,
        name AS sector_name 
    FROM sectors
    '''
    return mysql_query(query)

# def get_case_study_articles():

#     query = '''
#     SELECT 
#     '''
#     return mysql_query(query)

def get_all_events():

    query = f'''
        SELECT 
            e.id,
            e.title AS event_name,
            et.title AS event_type,
            t.tenants,
            members.members,
            e.description,
            e.status,
            e.venue,
            e.starts_at,
            e.ends_at
        FROM events AS e
        LEFT JOIN event_types AS et
            ON (e.event_type_id=et.id)
        LEFT JOIN (
            SELECT 
                event_tenant.event_id, 
                GROUP_CONCAT(
                    tenants.name
                    ORDER BY tenants.name
                    SEPARATOR '{separator}'
            ) AS tenants
            FROM event_tenant
            INNER JOIN tenants 
                ON (event_tenant.tenant_id=tenants.id)
            GROUP BY event_tenant.event_id
         ) AS t
             ON (e.id=t.event_id)
        LEFT JOIN (
            SELECT 
                event_member.event_id, 
                GROUP_CONCAT(
                    members.name
                    ORDER BY members.name
                    SEPARATOR '{separator}'
                ) AS members
            FROM event_member
            INNER JOIN members 
                ON (event_member.member_id=members.id)
            GROUP BY event_member.event_id
        ) AS members
            ON (e.id=members.event_id)
    '''

    records, cols = mysql_query(query, return_cols=True)
    return records, cols

def get_all_event_sessions():

    query = '''
    SELECT  
        et.id,
        e.id AS event_id,
        e.title AS event_name,
        et.name AS session_name
    FROM event_tags AS et
    INNER JOIN events AS e
        ON (e.id=et.event_id)

    '''

    records, cols = mysql_query(query, return_cols=True)
    return records, cols

def get_event_attendees():

    query = f'''
        SELECT 
            eu.id,
            e.id AS event_id,
            e.title AS event_name,
            e.starts_at,
            e.ends_at,
            u.id AS user_id,
            CONCAT(u.first_name, " ", u.last_name) AS attendee_name,
            m.name AS company_name,
            eu.attended
        FROM events AS e
        INNER JOIN event_users AS eu
            ON (e.id=eu.event_id)
        INNER JOIN users AS u
            ON (eu.user_id=u.id)
        INNER JOIN members AS m 
            ON (u.member_id=m.id)
    '''

    records, cols = mysql_query(query, return_cols=True)
    return records, cols

def get_event_session_attendees():

    query = '''
    SELECT 
        et.id AS session_id,
        et.name AS session_name,
        e.title AS event_name,
        u.id AS user_id,
        CONCAT(u.first_name, " ", u.last_name) AS attendee_name,
        m.name AS company_name
    FROM 
        event_tags AS et
    INNER JOIN 
        events AS e
        ON (et.event_id=e.id)
    INNER JOIN 
        event_tag_event_user AS eteu
        ON (et.id=eteu.event_tag_id)
    INNER JOIN
        event_users AS eu
        ON (eu.id=eteu.event_user_id)
    INNER JOIN users AS u
        ON (eu.user_id=u.id)
    INNER JOIN members AS m
        ON (u.member_id=m.id)
    '''

    records, cols = mysql_query(query, return_cols=True)
    return records, cols

def get_users_with_missing_company_role():

    query = '''
    SELECT 
        u.first_name,
        u.last_name,
        u.email,
        u.company_position,
        m.name AS member_name,
        memberships.name AS membership_level,
        u.deleted_at
    FROM users AS u
    INNER JOIN 
        members AS m
        ON (u.member_id=m.id)
    INNER JOIN memberships 
        ON (m.membership_id=memberships.id)
    WHERE memberships.name <> 'Freemium'
        AND u.company_role IS NULL
        AND u.email IS NOT NULL
        AND u.deleted_at IS NULL
    ORDER BY member_name
    '''

    records, cols = mysql_query(query, return_cols=True)
    return records, cols

def correct_postcode(postcode):
    if postcode[-4] != " ":
        postcode = postcode[:-3] + " " + postcode[-3:]
    return postcode.upper()

def get_data_for_graph(data_dir="data_for_graph"):

    # members
    # member_output_dir = os.path.join(data_dir, "members")
    # os.makedirs(member_output_dir, exist_ok=True)
    # membership_levels = ("Patron", "Platinum", "Gold", "Silver", "Bronze", "Digital", "Freemium")
    # for membership_level in membership_levels:
    #     # filter_by_sector_commerce = membership_level == "Freemium"
    #     filter_by_sector_commerce = False
    #     records, _ = get_member_summaries(membership_level, filter_by_commerce_sector=filter_by_sector_commerce)
    #     records = pd.DataFrame(records)
    #     records.to_csv(f"{member_output_dir}/{membership_level}_members.csv")

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
   
    # records, _ = get_member_summaries()
    # records = get_member_articles_last_six_months()
    # records = get_members_not_publish()
    # records = get_member_regions()
    # records, cols = get_sector_counts()
    # records = get_sells_commerces_counts()
    # records = get_buys_commerces_counts()
    # records = get_sells_commerce_category_counts()
    # records = get_buys_commerce_category_counts()
    # records = get_all_commerces_and_commerce_categories()
    # records = get_all_sectors()
    # records, _ = get_all_users()
    # records, _ = get_all_user_follows()
    # records, _ = get_all_messages()

    # membership_levels = ("Patron", "Platinum", "Gold", "Silver", "Bronze", "Digital", "Freemium")
    # for membership_level in membership_levels:
    #     filter_by_sector_commerce = membership_level == "Freemium"
    #     records, _ = get_member_summaries(membership_level, filter_by_commerce_sector=filter_by_sector_commerce)
    #     records = pd.DataFrame(records)
    #     records.to_csv(f"member_summaries/{membership_level}_production.csv")
   
    # records, _ = get_sector_counts(membership_levels)
    # records, _ = get_commerces_counts(None)
    # records, _ = get_commerces_category_counts(membership_levels)
    # records, _ = get_member_sectors(membership_levels)
    # records, _ = get_member_sector_counts(membership_levels)
    # records, _ = get_member_commerces_flat(None)
    # records, _ = get_all_messages()

    # records, _ = get_all_events()
    # records, _ = get_event_attendees()

    # records, _ = get_all_news_articles()

    # records, _ = get_users_with_missing_company_role()

    # # map id to int
    # # id_to_int = {}
    # # for record in records:
    # #     record_id = record["database_id"]
    # #     if record_id not in id_to_int:
    # #         id_to_int[record_id] = len(id_to_int)
    # #     record["id"] = id_to_int[record_id]

    # records = pd.DataFrame(records)
    # # print (records.head())
    # # print (records.shape)
    # # records = records[["id"] + list(records.columns)[:-1]]
    # records.to_csv(f"paid_users_without_company_role.csv")