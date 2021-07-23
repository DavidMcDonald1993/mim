import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir,)))

import pandas as pd

from utils.mysql_utils import mysql_query, sanitise_args, separator
from utils.scraping_utils import remove_html_tags


def get_member_commerces_flat(membership_levels):
    '''
    get all of the commerces that members buy/sell in a flat form
    '''
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
    '''
    get all of the sectors that a member works in
    '''
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

    '''
    produce a flat file describing the members in the database
    '''
   
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

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_member_addresses():

    '''
    get all of the addresses for all of the members in the database
    '''

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

def get_all_live_chat_messages():

    query = '''
    SELECT
        m.live_chat_conversation_id AS chat_id,
        COUNT(DISTINCT(sender.member_id)) AS num_participating_members,
        GROUP_CONCAT(DISTINCT(sender_members.name)) AS participating_members,
        COUNT(DISTINCT(m.user_id)) AS num_participating_users,
        GROUP_CONCAT(DISTINCT(CONCAT(sender.full_name, 
            " (", sender.company_position,  "@", sender_members.name, ")" ))) AS participating_users,
        COUNT(m.message) AS num_messages_in_chat,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'sender', sender.full_name,
                'sender_company', sender_members.name,
                'sender_role', sender.company_position,
                'message', m.message,
                'sent_at', m.created_at
            )
        ) AS all_messages
        
    FROM live_chat_messages AS m
    INNER JOIN users AS sender
        ON (m.user_id=sender.id)
    INNER JOIN members AS sender_members
        ON (sender.member_id=sender_members.id)
    INNER JOIN live_chat_conversations AS c
        ON (m.live_chat_conversation_id=c.id)
    GROUP BY chat_id
    HAVING COUNT(DISTINCT m.user_id) > 1
    ORDER BY num_participating_members DESC, num_messages_in_chat DESC
    '''

    records, cols = mysql_query(query, return_cols=True, to_json=True)
    return records, cols

def get_all_news_articles():

    '''
    get all news articles in the database, as well as the article tags and relevant member
    '''

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

    '''
    summariese members by the number of news articles that they have posted 
    '''

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

    '''
    pull all the non-deleted users from the database
    '''

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

    '''
    get all user-follows relationships from the database
    '''

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


def get_sector_counts(membership_levels=None):

    '''
    summarise sectors by the numbers of members tagged to them
    '''
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

def get_commerces_counts(membership_levels=None):
    '''
    summarise commerces by the numbers of members that buy/sell them
    '''
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
    '''
    summarise the number of members in each commerce category
    '''
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

    '''
    get members not tagged with a buy or sell commerce
    '''

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

def get_company_role_counts():

    '''
    summarise company_role
    '''

    query = f'''
    SELECT 
        company_role,
        COUNT(id) AS count
    FROM users
    WHERE company_role IS NOT NULL
    GROUP BY company_role
    ORDER BY count DESC
    '''

    return mysql_query(query, return_cols=True, to_json=True)


def get_member_articles_last_months(num_months=6):

    '''
    summarise members by the number of articles that they have posted over the last num_months months
    '''

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

def get_members_no_publish(num_months=6):

    '''
    identify members who have not posted an article for num_months months
    
    '''

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

    '''
    pair members with their tenancy
    '''

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

    '''
    pull all commerces(and their categories) from the database
    '''

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

    '''
    pull all sectors from the database
    '''

    query = '''
    SELECT 
        id,
        name AS sector_name 
    FROM sectors
    '''
    return mysql_query(query)

def get_all_events():

    '''
    pull all events from the database
    '''

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

    '''
    pull all event_sessions from the database
    '''

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

    '''
    pull all event attendees
    '''

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

    '''
    pull all event session attendees
    '''

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

    '''
    identify users whoe have not yet filled out their company role
    '''

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


if __name__ == "__main__":

    get_users_with_missing_company_role()
   