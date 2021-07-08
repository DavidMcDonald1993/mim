import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from graph.arango_utils import aql_query, connect_to_mim_database

import pandas as pd

def same_sector(member, max_distance=None, min_sectors=None, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR doc IN Members
        FILTER doc.member_name == '{member}'
        
        FOR u, e, p IN 2 ANY doc InSector 
            FILTER u.latitude != NULL AND u.longitude != NULL
            FILTER u.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
            
            LET d = DISTANCE(doc.latitude, doc.longitude,
                u.latitude, u.longitude)
        
            LET match = {{
                name: u.member_name,
                d: d,
                matching_sector: Document( e._to ).name
            }}
            
            COLLECT name = match.name INTO elements

            LET d = elements[0].d
            LET num_matching_sectors = LENGTH(elements) 

            {f"FILTER d < {max_distance} * 1000" if max_distance is not None else ""}
            {f"FILTER num_matching_sectors >= {min_sectors}" if min_sectors is not None else ""}
            SORT d ASC

            RETURN {{
                name: name,
                distance_to_source: elements[0].d,
                num_matching_sectors: num_matching_sectors,
                matching_sectors: elements[*].match.matching_sector,
    
            }}
    '''

    return aql_query(db, query)

def commerce_match(member, max_distance, min_commerces, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR doc IN Members
        FILTER doc.member_name == '{member}'
        
        FOR u, e, p IN 2 ANY doc PerformsCommerce
            FILTER u.latitude != NULL AND u.longitude != NULL
            FILTER u.membership_level IN ["Patron", "Platinum", 'Gold', 'Silver', 'Bronze']
            
            FILTER p.edges[0].commerce_type != p.edges[1].commerce_type

            
            LET d = DISTANCE(doc.latitude, doc.longitude,
                u.latitude, u.longitude)
        
            LET match = {{
                name: u.member_name,
                d: d,
                matching_commerce_name: Document( e._to ).commerce,
                commerce_type: e.commerce_type,
            }}
            
            COLLECT name = match.name INTO elements

            LET d = elements[0].d
            LET num_matching_commerces = LENGTH(elements)
           
            {f"FILTER d < {max_distance} * 1000" if max_distance is not None else ""}
            {f"FILTER num_matching_commerces >= {min_commerces}" if min_commerces is not None else ""}
           
            SORT num_matching_commerces DESC

            RETURN {{
                name: name,
                distance_to_source: elements[0].d,
                num_matching_commerces: num_matching_commerces,
                matching_commerces: INTERLEAVE(elements[*].match.commerce_type, elements[*].match.matching_commerce_name),
            }}
    '''

    return aql_query(db, query)

def shortest_path_of_users(member_1, member_2, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR member_1 IN Members
        FILTER member_1.member_name == '{member_1}'
        
        FOR member_2 IN Members
            FILTER member_2.member_name == '{member_2}'    
        
            FOR u, e IN ANY SHORTEST_PATH member_1 TO member_2 UserWorksAt, Messages
                
                FILTER IS_SAME_COLLECTION(u, 'Users')

                LET m = FIRST( Members[* FILTER m.name == u.company_name ] )
                FILTER m.membership_level IN ["Patron", "Platinum", 'Gold', 'Silver', 'Bronze']

                RETURN u
    '''
    return aql_query(db, query)


def best_common_neighbour(member_1, member_2, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR member_1 IN Members
        FILTER member_1.member_name == '{member_1}'
        
        FOR member_2 IN Members
            FILTER member_2.member_name == '{member_2}'    
        
            FOR u, e IN ANY SHORTEST_PATH member_1 TO member_2 UserWorksAt, Messages
                
                FILTER IS_SAME_COLLECTION(u, 'Users')
                FILTER u.company_name != member_1.member_name

                LET m = FIRST( Members[* FILTER m.name == u.company_name ] )
                FILTER m.membership_level IN ["Patron", "Platinum", 'Gold', 'Silver', 'Bronze']
                
                LIMIT 1
                RETURN {{
                    name: u.name,
                    company: u.company_name,
                    company_position: u.company_position,
                    email: u.email
                }}
    '''
    return aql_query(db, query)

def most_followed_by_staff(member, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m IN Members
        FILTER m.member_name == '{member}'
        FOR u, e, p IN 2 INBOUND m UserWorksAt, OUTBOUND UserFollows OPTIONS {{uniqueVertices: "path"}}
        
            LET who_follows = Document(e._from).full_name
            LET r = {{
                member_name: u.member_name,
                who_follows: who_follows,
            }}
            
            
            COLLECT name = r.member_name INTO elements
            LET num_follows = LENGTH(elements)
            SORT num_follows DESC
            RETURN {{
                name: name,
                num: num_follows,
                who_follows: elements[*].who_follows,
            }}
    '''

    return aql_query(db, query)

def followed_by_followed(member_name, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m IN Members
        FILTER m.member_name == '{member_name}'
        // find who staff follow
        FOR u IN 2 INBOUND m UserWorksAt, OUTBOUND UserFollows OPTIONS {{uniqueVertices: "path"}}
        
            LET followed_member = u
            
            // iterate over who their staff follow  
            FOR v, e, p IN 2 INBOUND followed_member UserWorksAt, OUTBOUND UserFollows OPTIONS {{uniqueVertices: "path"}}
                
                LET who_follows = Document(e._from)//.full_name
                LET r = {{
                    member_name: v.member_name,
                    who_follows: who_follows,
                }}
                
                COLLECT name = r.member_name INTO elements
                
                LET num_follows = LENGTH(elements)
                SORT num_follows DESC
                RETURN {{
                    name: name,
                    num: num_follows,
                    who_follows: elements[* RETURN CONCAT(
                        CURRENT.who_follows.full_name, " works at ", 
                        CURRENT.who_follows.company_name)]
                }}
    '''
    return aql_query(db, query)

def followed_by_followers(member_name, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m IN Members
        FILTER m.member_name == '{member_name}'
        // find who follow member
        FOR u, e, p IN 2 ANY m UserFollows OPTIONS {{uniqueVertices: "path"}}
                
            LET who_follows = Document(e._from)//.full_name
            LET r = {{
                member_name: u.member_name,
                who_follows: who_follows,
            }}
            
            COLLECT name = r.member_name INTO elements
            
            LET num_follows = LENGTH(elements)
            SORT num_follows DESC
            RETURN {{
                name: name,
                num: num_follows,
                who_follows: elements[* RETURN CONCAT(
                    CURRENT.who_follows.full_name, " works at ", 
                    CURRENT.who_follows.company_name)]
            }}
    '''
    return aql_query(db, query)

def most_followed_of_followers(member_name, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m IN Members
        FILTER m.member_name == '{member_name}'
        
        // find who follow member
        FOR u, e, p IN 2 ANY m UserFollows OPTIONS {{uniqueVertices: "path"}}
            
        // count number of users who follow this business
        LET num_follows = LENGTH(for v IN 1 INBOUND u UserFollows RETURN 1)
        
        LET r = {{
            member_name: u.member_name,
            num_follows: num_follows
        }}
        
        SORT num_follows DESC
        
        RETURN DISTINCT r
            
    '''

    return aql_query(db, query)
    
def future_events(db=None):
    if db is None:
        db = connect_to_mim_database()

    query = '''
    FOR e IN Events

        LET now = DATE_ISO8601(DATE_NOW())
        LET in_future = now < e.starts_at
        
        FILTER in_future

        RETURN {
            event_name: e.event_name,
            event_type: e.event_type,
            event_date: e.starts_at,
            in_future: in_future
        }
    '''
    return aql_query(db, query)

def future_events_for_same_sector(member_name, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m in Members
        FILTER m.member_name == '{member_name}'
        
        // iterate over sectors
        FOR u, sector IN 2 ANY m InSector
        
            LET sector_name = Document(sector._to).sector_name
            
            
            // iterate over events
            FOR v, e in 2 INBOUND u UserWorksAt, OUTBOUND EventAttendees OPTIONS {{bfs:true, uniqueVertices: "global"}}
                FILTER v.starts_at > DATE_ISO8601(DATE_NOW())
                
                LET attendee = Document(e._from)
                
                LET attendee_data = {{
                    event_name: v.event_name,
                    starts_at: v.starts_at,
                    ends_at: v.ends_at,
                    attendee_name:  attendee.full_name,
                    company_name: attendee.company_name,
                    //sector: sector_name
                }}
                
                COLLECT event=attendee_data.event_name INTO elements
                LET num_relevant_attendees = LENGTH(elements)
                SORT num_relevant_attendees DESC
            
                RETURN {{
                    event_name: event,
                    starts_at: elements[0].attendee_data.starts_at,
                    ends_at: elements[0].attendee_data.ends_at,
                    num_relevant_attendees,
                    attendees: elements[* RETURN CONCAT(
                        CURRENT.attendee_data.attendee_name, " works at ", 
                        CURRENT.attendee_data.company_name) ]
                }}
                
    '''
    return aql_query(db, query)

def future_events_same_attendees_as_past_events(member_name, db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m in Members
        FILTER m.member_name == '{member_name}'
        
        FOR u, e, p in 3 INBOUND m UserWorksAt, ANY EventAttendees OPTIONS {{uniqueVertices: "path"}}
            
            LET event = p.vertices[2]
            FILTER event != null
            FILTER event.ends_at < DATE_ISO8601(DATE_NOW())
            
            COLLECT event_attendee_distinct=u INTO attended_events
            
        // SORT LENGTH(attended_events) DESC

            FOR future_event IN 1 OUTBOUND event_attendee_distinct EventAttendees
                FILTER future_event.starts_at > DATE_ISO8601(DATE_NOW())
                
                LET attendee_data = {{
                    event_name: future_event.event_name,
                    starts_at: future_event.starts_at,
                    ends_at: future_event.ends_at,
                    attendee_name:  event_attendee_distinct.full_name,
                    company_name: event_attendee_distinct.company_name,
                    attended_events: attended_events[*].event
                }}
                
                COLLECT event=attendee_data.event_name INTO elements
                LET num_relevant_attendees = LENGTH(elements)
                SORT num_relevant_attendees DESC, elements[0].attendee_data.starts_at ASC
            
                RETURN {{
                    event_name: event,
                    starts_at: elements[0].attendee_data.starts_at,
                    ends_at: elements[0].attendee_data.ends_at,
                    num_relevant_attendees,
                    attendees: elements[* RETURN CONCAT(
                        CURRENT.attendee_data.attendee_name, " works at ", 
                        CURRENT.attendee_data.company_name, " ", 
                        CURRENT.attendee_data.attended_events[*].event_name) ]
                }}
    '''

    return aql_query(db, query)

def explore_local_area(member_name, max_steps=3, max_results=5, db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m in Members
        FILTER m.member_name == '{member_name}'
        
        FOR u, e, path in 1..{max_steps} ANY m 
            MemberMemberBusiness, InSector, PerformsCommerce, UserWorksAt, UserFollows, EventAttendees 
            PRUNE u == NULL 
            OPTIONS {{uniqueVertices: "path"}}
            
            FILTER IS_SAME_COLLECTION(u, "Members")

            COLLECT local_member=u.member_name INTO details
            
            LET num_paths = LENGTH(details)
            SORT num_paths DESC

            LIMIT {max_results}

            RETURN {{
                member_name:local_member,
                num_paths: num_paths,
                paths: details[*].path.edges[* RETURN CONCAT(
                    DOCUMENT(CURRENT._from).name, 
                    "<-[", DOCUMENT(CURRENT._id).name,"]->", 
                    DOCUMENT(CURRENT._to).name)]
            }}
        '''
    return aql_query(db, query)

def build_commerce_chains(member_name, limit=None, db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m in Members
        FILTER m.member_name == '{member_name}'
        FOR u, e, p IN 2..4 ANY m PerformsCommerce 
            PRUNE (LENGTH(p.edges) > 1 && p.edges[LENGTH(p.edges)-1].commerce_type == p.edges[LENGTH(p.edges)-2].commerce_type )
            
            FILTER IS_SAME_COLLECTION(u, "Members")

            FILTER u.membership_level IN ["Patron", "Platinum", "Gold", "Silver", "Bronze"]
            FILTER u.regions ANY IN m.regions

            FILTER p.edges[LENGTH(p.edges)-1].commerce_type != p.edges[LENGTH(p.edges)-2].commerce_type
            
            SORT LENGTH(p.edges) DESC
            {f"LIMIT {limit}" if limit is not None else ""}
            
            RETURN {{
                all_edges:p.edges[* RETURN CONCAT(
                    DOCUMENT(CURRENT._from).name, " ", 
                    CURRENT.name, " ", 
                    Document(CURRENT._to).name) ]
            }}
            
            
            //return p
    '''
    return aql_query(db, query)

def uk_class_match(member_name, class_type="classes", db=None):
    assert class_type in {"classes", "groups", "divisions", "sectors"}
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR m in Members
        FILTER m.UK_{class_type} != NULL
        FILTER m.member_name == "{member_name}"
        FOR doc IN Members
            FILTER m.UK_{class_type} ANY IN doc.UK_{class_type}
            FILTER m != doc
            FILTER m.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
            FILTER doc.regions ANY IN m.regions
            LET d = DISTANCE(doc.latitude, doc.longitude,
                m.latitude, m.longitude)
            SORT d ASC
            RETURN {{
                member_name: doc.member_name,
                {class_type}: doc.UK_{class_type},
                distance: d, //metres
            }}
    '''

    return aql_query(db, query)

def closest_prospects_to_event_hosts(event_name, max_km=10, db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR e IN Events
        FILTER e.event_name=="{event_name}"
            
        LET hostCoordinates = Members[* 
            FILTER CURRENT.member_name IN e.members 
            RETURN {{
                host_name: CURRENT.member_name,
                coordinates: CURRENT.coordinates
            }}
        ]
        
        FOR p in Prospects
            FILTER p.coordinates != NULL
                    
            LET dists = hostCoordinates[* 
                RETURN {{
                    host_name: CURRENT.host_name,
                    distance_km: DISTANCE(CURRENT.coordinates[0], CURRENT.coordinates[1], p.coordinates[0], p.coordinates[1]) / 1000 
                }}
            ]
            
            LET minDist = MIN(dists[*].distance_km )
            LET minHost = dists[* FILTER CURRENT.distance_km==minDist RETURN CURRENT.host_name ]
            
            SORT minDist ASC
            
            FILTER minDist < {max_km}
            
            LET sic_codes = p.sic_codes[* RETURN SPLIT(CURRENT, " - ")[1] ]
          
            LET matching_UK_classes = UKClasses[*
                FILTER CURRENT.type == "class"
                    AND CURRENT.identifier IN sic_codes
            ]
            
            LET nodes = (
            FOR class IN matching_UK_classes
                FOR u IN 1..3 OUTBOUND class UKClassHierarchy
                    RETURN DISTINCT u
            )
            
            LET groups = nodes[*
                FILTER CURRENT.type=="group"
                RETURN CURRENT.identifier
            ]
            
            LET divisions = nodes[*
                FILTER CURRENT.type=="division"
                RETURN CURRENT.identifier
            ]
            
            LET sectors = nodes[*
                FILTER CURRENT.type=="sector"
                RETURN CURRENT.identifier
            ]
              
            LET members_same_class = Members[*
                FILTER ("Made in the Midlands" IN CURRENT.tenancies AND p.region=="midlands"
                    OR "Made in Yorkshire" IN CURRENT.tenancies AND p.region=="yorkshire")
                    AND CURRENT.UK_classes ANY IN sic_codes
                RETURN CURRENT.member_name
            ]
            
            
            LET members_same_group = Members[*
                FILTER ("Made in the Midlands" IN CURRENT.tenancies AND p.region=="midlands"
                    OR "Made in Yorkshire" IN CURRENT.tenancies AND p.region=="yorkshire")
                    AND CURRENT.UK_groups ANY IN groups
                RETURN CURRENT.member_name
            ]
            
            LET members_same_division = Members[*
                FILTER ("Made in the Midlands" IN CURRENT.tenancies AND p.region=="midlands"
                    OR "Made in Yorkshire" IN CURRENT.tenancies AND p.region=="yorkshire")
                    AND CURRENT.UK_divisions ANY IN divisions
                RETURN CURRENT.member_name
            ]
            
            LET members_same_sector = Members[*
                FILTER ("Made in the Midlands" IN CURRENT.tenancies AND p.region=="midlands"
                    OR "Made in Yorkshire" IN CURRENT.tenancies AND p.region=="yorkshire")
                    AND CURRENT.UK_sectors ANY IN sectors
                RETURN CURRENT.member_name
            ]

            FILTER LENGTH(members_same_class) > 0
            
            RETURN {{
                "prospect_name": p.name, 
                "website": p.website,
                "postcode": p.postcode,
                "region": p.region,
                "relevant_members_in_same_region(Endole)": p.relevant_members,
                "companies_house_sic_codes": sic_codes,
                "members_same_SIC": members_same_class,
                "number_members_same_SIC": LENGTH(members_same_class),
                "UK_groups": groups,
                "members_same_group": members_same_group,
                "number_members_same_group": LENGTH(members_same_group),
                "UK_divisions": divisions,
                "members_same_division": members_same_division,
                "number_members_same_division": LENGTH(members_same_division),
                "UK_sectors": sectors,
                "members_same_sector": members_same_sector,
                "number_members_same_sector": LENGTH(members_same_sector),
                "event": e.event_name,
                "event_starts_at": e.starts_at,
                "event_hosts": hostCoordinates[*].host_name,
                "all_distances(km)": dists, 
                "minimum_distance(km)": minDist,
                "closest_host": minHost,
                
            }}
    '''
    return aql_query(db, query)

def prospects_in_same_class_as_event_hosts(event_name, db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR e IN Events
        FILTER e.event_name=="{event_name}"
            
        LET hostClasses = Members[* 
            FILTER CURRENT.member_name IN e.members 
            RETURN {{
                host_name: CURRENT.member_name,
                host_classes: CURRENT.UK_classes,
                regions: CURRENT.regions
            }}
        ]
    
        
        LET classes = hostClasses[*].host_classes[**]
        LET regions =  hostClasses[*].regions[**]
        
        
        FOR p IN Prospects 
            FILTER p.UK_classes != NULL
            FILTER p.region IN regions
            FILTER p.UK_classes ANY IN classes
            
            
            LET relevantHosts = hostClasses[*
                FILTER CURRENT.host_classes ANY IN p.UK_classes
                RETURN CURRENT.host_name
            ]
            LET numRelevantHosts = LENGTH(relevantHosts)
            
            SORT numRelevantHosts DESC
            
            RETURN {{
                prospect_name:p.name,
                prospect_website: p.website,
                prospect_region: p.region,
                prospect_directors: p.directors,
                prospect_SIC_codes: p.UK_classes,
                num_relevant_hosts: numRelevantHosts,
                relevant_hosts: relevantHosts,
            }}
            
    '''
    return aql_query(db, query)

def prospects_with_commerce_connected_SIC_codes_to_event_hosts(event_name, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR e IN Events
        FILTER e.name == '{event_name}'
       LET commerce_related_code_data = (FOR host IN e.members
        LET host_doc = FIRST(Members[* FILTER CURRENT.name==host ])
        FOR host_code IN host_doc.UK_classes
            LET host_code_node = FIRST(UKClasses[* 
                FILTER CURRENT.identifier==host_code AND CURRENT.type=="class" 
            ])
            
            FOR code, edge, path IN 3 ANY host_code_node
                GRAPH "SIC_business_connections"
                FILTER IS_SAME_COLLECTION(code, UKClasses)
                COLLECT relevant_code=code.identifier INTO elements
                RETURN {{
                    relevant_code,
                    hosts: elements[* RETURN CONCAT(CURRENT.host, ":", CURRENT.host_code) ]
                }}
    )
    
    LET commerce_related_codes = commerce_related_code_data[*].relevant_code
   
    FOR p IN Prospects
        FILTER p.UK_classes != NULL
        FILTER p.UK_classes ANY IN commerce_related_codes
        
        LET matches = (
        commerce_related_code_data[*
            FILTER CURRENT.relevant_code IN p.UK_classes
            RETURN CONCAT(CURRENT.relevant_code, ": ", CURRENT.hosts)
        ]
        )
        
        LET num_matches = LENGTH(matches)
        SORT num_matches DESC
        
        RETURN {{
            prospect_name: p.name,
            prospect_website: p.website,
            prospect_postcode: p.postcode,
            prospect_region: p.region, 
            prospect_directors: p.directors,
            prospect_SIC_codes: p.UK_classes,
            num_matches: num_matches,
            matches,
            //matched_host_codes: matches[*].host_codes,
            //matched_hosts: matches[*].hosts,
        }}
        
    '''

    return aql_query(db, query)

def group_prospects(region, min_group_size=5, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR p IN Prospects
        
        FILTER p.region == "{region}"

        LET classes = p.UK_classes
        
        for class IN classes
            COLLECT unique_class = class INTO prospects
            
            LET group = (
                FOR c IN UKClasses
                    FILTER c.identifier == unique_class
                    FOR u IN 1 OUTBOUND c UKClassHierarchy
                        RETURN u
            )
            
            LET num_prospects_in_class = LENGTH(prospects)
            FILTER num_prospects_in_class >= {min_group_size}
            
            SORT group[0].identifier ASC, num_prospects_in_class DESC
            
            RETURN {{
                group: group[0].identifier,
                class: unique_class, 
                num_prospects_in_class: num_prospects_in_class,
                prospects: prospects[*].p.name
            }}

    '''
    return aql_query(db, query)


def recommend_members_based_on_SIC_commerce(
    name=None,
    limit=None,
    region=None,
    collection="Prospects",
    db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR p in {collection}
        
        FILTER p.UK_classes != NULL
        FILTER p.coordinates != NULL

        SORT p.name

        {f"FILTER p.region == '{region}'" if collection=="Prospects" and region is not None 
        else f"FILTER '{region}' in p.regions" if collection=="Members" and region is not None
        else ""}
        {f"FILTER p.name == '{name}'" if name is not None else ""}
        {f"LIMIT {limit}" if limit is not None else ""}
        
        LET matched_codes = (
        FOR s IN p.UK_classes
            LET s_node = FIRST(UKClasses[*
                FILTER CURRENT.identifier==s AND CURRENT.type=="class"
            ])
        
            FOR code, e, path IN 3 ANY s_node
                GRAPH "SIC_business_connections"
                FILTER IS_SAME_COLLECTION(code, UKClasses)
                LET relevant_business = {{
                    "member_1": path.vertices[1].name,
                    "member_1_SIC": path.vertices[0].identifier,
                    "member_2": path.vertices[2].name,
                    "member_2_SIC": code.identifier,
                }}
                COLLECT class = code.identifier INTO evidence=relevant_business
                RETURN {{
                    class,
                    "num_evidence": LENGTH(evidence),
                    evidence, 
                }}
                
        )
        
        LET classes =matched_codes[*].class
        
        FOR m IN Members
            FILTER m.coordinates != NULL
            FILTER {"p.region in m.regions" if collection=="Prospects"
            else "p.regions ANY IN m.regions"}
            FILTER m.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
          
            FILTER m.UK_classes != NULL
            FILTER m.UK_classes ANY IN classes 
            
            // count commerce SIC matched 
            LET code_matches = (
            FOR uk_class IN m.UK_classes
                FILTER uk_class IN classes
                RETURN {{
                    relevant_class: uk_class,
                    evidence: matched_codes[* FILTER CURRENT.class==uk_class ].evidence
                }}
            )
            LET num_code_matches = LENGTH(code_matches)
           
            LET dist = DISTANCE(p.latitude, p.longitude, m.latitude, m.longitude) / 1000
            
            //SORT num_code_matches DESC
            SORT p.name, dist ASC
            
            RETURN {{
                {collection}_name: p.name,
                {collection}_website: p.website,
                {collection}_postcode: p.postcode,
                {collection}_sic_codes: p.UK_classes,
                recommended_member_name: m.name,
                recommended_member_website: m.website,
                recommended_member_sic_codes: m.UK_classes,
                recommended_member_postcode: m.postcode,
                code_matches: code_matches,
                num_code_matches, num_code_matches,
                "distance(km)": dist,
            }}
    '''

    return aql_query(db, query, verbose=True)


def recommend_members_based_on_SIC_similarity(
    name=None,
    limit=None,
    region=None,
    collection="Prospects",
    include=["classes", "groups"],
    db=None):

    if db is None:
        db = connect_to_mim_database()

    match_str = "\n".join((
        f'''
            LET {c}_matches = (\
                FOR c IN p.UK_{c}\
                    FILTER c IN m.UK_{c}\
                    RETURN c\
            )'''
         for c in include
    ))

    count_matches_str = '\n'.join((
    f'''
    LET num_{c}_matches = LENGTH({c}_matches)
    '''
    for c in include))

    query = f'''
    FOR p in {collection}
        
        FILTER p.UK_classes != NULL
        FILTER p.coordinates != NULL

        SORT p.name

        {f"FILTER p.region == '{region}'" if collection=="Prospects" and region is not None 
        else f"FILTER '{region}' ON p.regions" if collection=="Members" and region is not None
        else ""}
        {f"FILTER p.name == '{name}'" if name is not None else ""}

        FOR m IN Members

            FILTER p != m
            FILTER m.coordinates != NULL
            FILTER {"p.region in m.regions" if collection=="Prospects"
            else "p.regions ANY IN m.regions"}

            FILTER m.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']

            FILTER m.UK_classes != NULL
            FILTER {" OR ".join((f"m.UK_{c} ANY IN p.UK_{c}"  
                for c in include))}
          
            {match_str}
            {count_matches_str}
           
            LET dist = DISTANCE(p.latitude, p.longitude, m.latitude, m.longitude) / 1000
            SORT {",".join((f"num_{c}_matches DESC" for c in include))}
                , dist ASC

            {f"LIMIT {limit}" if limit is not None else ""}
            
            RETURN {{
                {collection}_name: p.name,
                {collection}_website: p.website,
                {collection}_postcode: p.postcode,
                {collection}_sic_codes: p.UK_classes,
                recommended_member_name: m.name,
                recommended_member_website: m.website,
                recommended_member_sic_codes: m.UK_classes,
                recommended_member_postcode: m.postcode,
                {",".join((
                    f"{c}_matches, num_{c}_matches"
                    for c in include))},
                "distance(km)": dist,
            }}
    '''

    return aql_query(db, query, verbose=True)

def recommend_prospects(region, member_name=None, max_km=5, limit=10, source="Prospects", db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    LET region_members = (
        FOR m IN Members 
            FILTER "{region}" IN m.regions
            FILTER m.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
            RETURN {{
                name: m.name,
                UK_classes: m.UK_classes,
                longitude: m.longitude,
                latitude: m.latitude,
            }}
    )

    FOR p IN {source}
        FILTER {f"p.region == '{region}'" if source=="Prospects" 
            else f"'{region}' IN p.regions"}
        {"FILTER p.membership_level=='Freemium'" if source != "Prospects" else "" } 
        FILTER p.UK_classes != NULL
        FILTER p.coordinates != NULL
        
        LET prospect_classes = p.UK_classes
        LET prospect_sectors = p.UK_sectors
        
        LET members_in_region_with_same_class = region_members[*
            FILTER CURRENT.UK_classes ANY IN prospect_classes
            RETURN CURRENT.name
        ]

        {f"FILTER '{member_name}' IN members_in_region_with_same_class" if member_name is not None else ""}
        
        LET number_of_members_in_region_with_same_class = LENGTH(members_in_region_with_same_class)
        
        SORT number_of_members_in_region_with_same_class DESC
        LIMIT {limit}
        
        
        LET members_in_region_in_same_sector = region_members[*
            FILTER CURRENT.UK_sectors ANY IN prospect_sectors
            RETURN CURRENT.name
        ]
        
        LET number_of_members_in_region_in_same_sector = LENGTH(members_in_region_in_same_sector)
        
        LET members_in_region_close_to_prospect = (
        FOR m IN region_members
            FILTER m.coordinates != NULL
            LET d = DISTANCE(p.latitude, p.longitude, 
                m.latitude, m.longitude) / 1000
            FILTER d < {max_km}
            RETURN {{member: m.name, "distance(km)": d}}
        )
        LET num_members_in_region_close_to_prospect = LENGTH(members_in_region_close_to_prospect)
        
    
        LET supply_chain_classes = (
        FOR code in prospect_classes
            LET code_node = FIRST(UKClasses[* FILTER CURRENT.identifier==code AND CURRENT.type=="class"])
            FOR c, edge, path IN 3 ANY code_node
                GRAPH "SIC_business_connections"
                FILTER IS_SAME_COLLECTION(c, UKClasses)
                COLLECT relevant_code=c.identifier
                RETURN relevant_code
        )
        LET members_in_region_relevant_for_supply_chain = (
        FOR m IN region_members
            FILTER m.UK_classes ANY IN supply_chain_classes
            LET matching_codes = (
            FOR c in m.UK_classes
                FILTER c IN supply_chain_classes
                RETURN c
            )
            RETURN {{
                member: m.name,
                matching_codes,
            }}
        )
        LET num_members_in_region_relevant_for_supply_chain = LENGTH(members_in_region_relevant_for_supply_chain)
        
        RETURN {{
            prospect_name: p.name,
            prospect_SIC_codes: p.UK_classes,
            prospect_sectors: p.UK_sectors,
            prospect_postcode: p.postcode,
            prospect_region: p.region,
            prospect_website: p.website,
            prospect_directors: p.directors,
            number_of_members_in_region_with_same_class,
            members_in_region_with_same_class,
            number_of_members_in_region_in_same_sector,
            //members_in_region_in_same_sector,
            members_in_region_close_to_prospect,
            num_members_in_region_close_to_prospect,
            supply_chain_classes,
            members_in_region_relevant_for_supply_chain,
            num_members_in_region_relevant_for_supply_chain,
        }}
    '''

    return aql_query(db, query, verbose=True)

def recommend_prospects_to_events(event_name, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''

FOR e IN Events
    FILTER e.event_name=="{event_name}"
    
    LET hostCoordinates = Members[* 
        FILTER CURRENT.member_name IN e.members 
        RETURN {{
            host_name: CURRENT.member_name,
            coordinates: CURRENT.coordinates
        }}
    ]
    
    LET host_code_data = (
    FOR host IN e.members
        LET host_doc = FIRST(Members[* FILTER CURRENT.name==host ])
        FOR host_code IN host_doc.UK_classes
            COLLECT unique_host_code=host_code INTO elements
            RETURN {{
                unique_host_code,
                hosts_with_code: elements[*].host
            }}
    )
    
    LET host_codes = host_code_data[*].unique_host_code
        
    LET commerce_related_code_data = (
    FOR host IN e.members
        LET host_doc = FIRST(Members[* FILTER CURRENT.name==host ])
        FOR host_code IN host_doc.UK_classes
            LET host_code_node = FIRST(UKClasses[* 
                FILTER CURRENT.identifier==host_code AND CURRENT.type=="class" 
            ])
            
            FOR code, edge, path IN 3 ANY host_code_node
                GRAPH "SIC_business_connections"
                FILTER IS_SAME_COLLECTION(code, UKClasses)
                COLLECT relevant_code=code.identifier INTO elements
                RETURN {{
                    relevant_code,
                    hosts: elements[* RETURN CONCAT(CURRENT.host, ":", CURRENT.host_code) ]
                }}
    )
    
    LET commerce_related_codes = commerce_related_code_data[*].relevant_code
   
    FOR p IN Prospects
        FILTER p.UK_classes != NULL
        FILTER p.UK_classes ANY IN commerce_related_codes
        FILTER p.UK_classes ANY IN host_codes
        FILTER p.coordinates != NULL
        
        LET commerce_matches = (
        FOR c IN commerce_related_code_data
            FILTER c.relevant_code IN p.UK_classes
            //RETURN CONCAT(c.relevant_code, ": ", c.hosts)
            FOR host IN c.hosts
                RETURN DISTINCT host
        )
        
        LET num_commerce_matches = LENGTH(commerce_matches)
        
        
        LET host_SIC_matches = (
        FOR c IN host_code_data
            FILTER c.unique_host_code IN p.UK_classes
            //RETURN CONCAT(c.unique_host_code, ": ", c.hosts_with_code)
            FOR host IN c.hosts_with_code
                RETURN DISTINCT host
        )
        LET num_host_SIC_matches = LENGTH(host_SIC_matches)
        
        LET dists = hostCoordinates[* 
            RETURN {{
                host_name: CURRENT.host_name,
                distance_km: DISTANCE(CURRENT.coordinates[0], CURRENT.coordinates[1], p.coordinates[0], p.coordinates[1]) / 1000 
            }}
        ]
        
        LET minDist = MIN(dists[*].distance_km )
        LET minHost = FIRST(dists[* FILTER CURRENT.distance_km==minDist RETURN CURRENT.host_name ])
        
        SORT num_commerce_matches DESC, minDist ASC
        
        // add relevant members
        LET prospect_classes = p.UK_classes
        LET members_same_region_same_class = Members[*
            FILTER CURRENT.UK_classes ANY IN prospect_classes
                AND p.region IN CURRENT.regions
                AND CURRENT.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']

            RETURN CURRENT.name
        ]
        LET num_members_same_region_same_class = LENGTH(members_same_region_same_class)
        
        FILTER num_members_same_region_same_class > 0
        
        LET prospect_sectors = p.UK_sectors
        LET num_members_same_region_same_sector = LENGTH(Members[*
            FILTER CURRENT.UK_sectors ANY IN prospect_sectors
                AND p.region IN CURRENT.regions
                AND CURRENT.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
            RETURN CURRENT.name
        ])
        
        // members with commerce SIC codes
        LET commerce_SIC_codes = (
        FOR code IN p.UK_classes
            LET code_node = FIRST(UKClasses[* 
                FILTER CURRENT.identifier==code AND CURRENT.type=="class" 
            ])
            
            FOR c, edge, path IN 3 ANY code_node
                GRAPH "SIC_business_connections"
                FILTER IS_SAME_COLLECTION(c, UKClasses)
                COLLECT relevant_code=c.identifier
                RETURN relevant_code
        )

        LET relevant_members_in_region_for_commerce = Members[*
            FILTER CURRENT.UK_classes ANY IN commerce_SIC_codes
                AND p.region IN CURRENT.regions
                AND CURRENT.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
            RETURN CURRENT.name
        ]
        LET num_relevant_members_in_region_for_commerce = LENGTH(relevant_members_in_region_for_commerce)
        
        RETURN {{
            prospect_name: p.name,
            prospect_directors: p.directors,
            prospect_website: p.website,
            prospect_postcode: p.postcode,
            prospect_region: p.region, 
            prospect_SIC_codes: p.UK_classes,
            num_host_commerce_matches: num_commerce_matches,
            host_commerce_matches: commerce_matches,
            num_host_SIC_matches,
            host_SIC_matches,
            "minimum_dist_to_host(km)": minDist,
            closest_host: minHost,
            members_same_region_same_class,
            num_members_same_region_same_class,
            prospect_sectors,
            num_members_same_region_same_sector,
            commerce_SIC_codes,
            relevant_members_in_region_for_commerce,
            num_relevant_members_in_region_for_commerce,
            event_name: e.name,
            event_starts_at: e.starts_at,
            event_description: e.description,
            event_hosts: e.members,
        }}
    '''

    return aql_query(db, query)

def get_prospects_with_directors_over_age(region, age=66, limit=50, source="Prospects", db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''

    LET region_members = (
        FOR m IN Members
            FILTER '{region}' IN m.regions
                AND m.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
            RETURN {{
                name: m.name,
                UK_classes: m.UK_classes,
                longitude: m.longitude,
                latitude: m.latitude,
            }}
    )

    FOR p IN {source}
        FILTER {f"p.region == '{region}'" if source=="Prospects" 
            else f"'{region}' IN p.regions"}
        {"FILTER p.membership_level=='Freemium'" if source != "Prospects" else ""}
        FILTER p.directors != NULL
        FILTER LENGTH(p.directors) > 0
        FILTER p.coordinates != NULL

        {"FILTER p.source == 'base'" if source=="Prospects" else ""}

        FILTER "MANUFACTURING" IN p.UK_sectors

        LET currentYear = DATE_YEAR(DATE_NOW())
        LET directors_at_retirement_age =  (
            FOR d in p.directors
                LET age = currentYear - TO_NUMBER(SPLIT(d.director_date_of_birth, ' ')[-1])
                FILTER age != 2021
                //FILTER age >= {age}
                FILTER age >= 60 AND age <65
                /*
                RETURN {{
                    name: d.director_name,
                    date_of_birth: d.director_date_of_birth,
                    occupation: d.director_occupation,
                    age: age
                }}
                */
                RETURN d.director_name
        )
        
        FILTER LENGTH(directors_at_retirement_age) == LENGTH(p.directors)

        LET members_in_same_region_with_same_SIC_codes = (
            FOR m IN region_members
                FILTER m.UK_classes ANY IN p.UK_classes
                RETURN m.name
        )
        LET number_members_in_same_region_with_same_SIC_codes = LENGTH(members_in_same_region_with_same_SIC_codes)
        FILTER number_members_in_same_region_with_same_SIC_codes > 0
        
        SORT number_members_in_same_region_with_same_SIC_codes DESC

        {f"LIMIT {limit}" if limit is not None else ""}

        LET number_members_in_same_region_same_sectors = LENGTH(region_members[* FILTER CURRENT.UK_sectors ANY IN p.UK_sectors])

        LET members_in_region_within_10km = (
            FOR m IN region_members
                FILTER m.latitude != NULL
                FILTER m.longitude != NULL
                LET d = DISTANCE(p.latitude, p.longitude,
                    m.latitude, m.longitude) / 1000
                FILTER d <= 10 
                /*
                RETURN {{
                    member_name: m.name,
                    postcode: m.postcode,
                    "distance(km)": d,
                }}
                */
                RETURN m.name
        )
        LET number_members_in_region_within_10km = LENGTH(members_in_region_within_10km)
        
        RETURN {{
            prospect_name: p.name,
            directors_at_retirement_age,
            //website: p.website,
            //postcode: p.postcode,
            //{"region: p.region" if source=="Prospects" else "regions: p.regions"},
            //latitude: p.latitude,
            //longitude: p.longitude,
            //prospect_SIC_codes: p.UK_classes,
            members_in_same_region_with_same_SIC_codes,
            number_members_in_same_region_with_same_SIC_codes,
            prospect_sectors: p.UK_sectors,
            number_members_in_same_region_same_sectors,
            members_in_region_within_10km,
            number_members_in_region_within_10km,
        }}
    '''

    return aql_query(db, query, verbose=True)

def match_members_by_articles(member_name, db=None):

    if db is None:
        db = connect_to_mim_database()

    query = f'''
    FOR q IN Members
        
        FILTER q.articles != NULL
        FILTER q.name == '{member_name}'
        
        
        LET keywords = TOKENS(q.all_articles,  "text_en")
        
        FOR m IN memberView

            
            SEARCH ANALYZER(m.all_articles IN keywords 
                //OR BOOST(m.all_articles IN TOKENS("manufacturing", "text_en"), 5)
                , "text_en")
            //SEARCH ANALYZER (m.all_articles IN TOKENS("manufacturing backing britain", "text_en"), "text_en")
        

            FILTER m != q
            
            LET score = BM25(m) 

            SORT score DESC
            LIMIT 10
            
            RETURN {{
                query: q.name,
                query_SIC: q.UK_classes,
                query_num_articles: LENGTH(q.articles),
                //keywords,
                member: m.name,
                member_SIC: m.UK_classes,
                member_num_articles: LENGTH(m.articles),
                //articles: m.all_articles,
                score,
            }}
    '''

    return aql_query(db, query)

def match_members_by_number_of_relevant_articles(
    member_name, 
    top=100, 
    limit=10, 
    ranking="BM25", 
    db=None):

    assert ranking in {"BM25", "TFIDF"}
    if db is None:
        db = connect_to_mim_database()
   
    query = f'''
        FOR q IN Members
            
            FILTER q.articles != NULL
            FILTER q.name == '{member_name}'
            
            LET keywords = TOKENS(q.all_articles,  "text_en")
            
            FOR a IN articleView

                SEARCH ANALYZER(a.content IN keywords, "text_en")

                FILTER a.member_name != q.member_name
                LET m = FIRST(Members[* FILTER CURRENT.member_name==a.member_name ])
                FILTER m.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
                FILTER m.regions ANY IN q.regions
                
                LET score = {ranking}(a) 
                
                SORT score DESC
                LIMIT {top}
                
                COLLECT poster=a.member_name INTO elements={{
                    title: a.title, 
                    score: score,
                }}
                
                LET mean_score = AVERAGE(elements[*].score)
                LET max_score = MAX(elements[*].score)
                LET min_score = MIN(elements[*].score)
                LET num_articles = LENGTH(elements)
                
                SORT num_articles DESC, mean_score DESC
                LIMIT {limit}
                
                RETURN {{
                    article_poster: poster, 
                    mean_score,
                    max_score,
                    min_score,
                    num_articles,
                    article_titles: elements[*].title
                }}
    '''    

    return aql_query(db, query)

def find_prospect_users_by_role(region, role="sales marketing", max_km=10, source="Members", limit=None, db=None):
    if db is None:
        db = connect_to_mim_database()

    query = f'''
    LET region_members = (
        FOR m IN Members
            FILTER '{region}' IN m.regions
                AND m.membership_level IN ['Patron', 'Platinum', 'Gold', 'Silver', 'Bronze']
            RETURN {{
                name: m.name,
                UK_classes: m.UK_classes,
                latitude: m.latitude,
                longitude: m.longitude,
            }}
    )

    LET desired_roles = TOKENS("{role}", "text_en")

    FOR p IN {source}
        FILTER {"p.membership_level == 'Freemium'" if source=="Members"
            else "p.source == 'base'"}
        FILTER {f"'{region}' IN p.regions" if source=="Members"
            else f"p.region == '{region}'"}
        FILTER p.UK_classes != NULL
        
        LET relevant_employees = (
        FOR u IN 1 INBOUND p {"UserWorksAt" if source=="Members" else "BaseContactWorksAt"}
            LET positions = TOKENS(u.{"company_position" if source=="Members" else "title"}, "text_en") 
            FILTER positions ANY IN desired_roles
        
            RETURN {{
                name: u.name,
                email: u.email,
                company_position: u.{"company_position" if source=="Members" else "title"}
            }}
        )
        
        FILTER LENGTH(relevant_employees) > 0

        LET prospect_classes = p.UK_classes
       
        LET supply_chain_classes = (
        FOR code in prospect_classes
            LET code_node = FIRST(UKClasses[* FILTER CURRENT.identifier==code AND CURRENT.type=="class"])
            FOR c, edge, path IN 3 ANY code_node
                GRAPH "SIC_business_connections"
                FILTER IS_SAME_COLLECTION(c, UKClasses)
                FILTER c != code_node
                COLLECT relevant_code=c.identifier
                RETURN relevant_code
        )
        
        
        LET members_in_region_relevant_for_supply_chain = (
        FOR m IN region_members
            FILTER m.UK_classes ANY IN supply_chain_classes
            RETURN m.name
        )
        LET number_members_in_region_relevant_for_supply_chain = LENGTH(members_in_region_relevant_for_supply_chain)

        SORT number_members_in_region_relevant_for_supply_chain DESC
        
        LET members_in_same_region_with_same_class = (
            FOR m IN region_members
                FILTER m.UK_classes ANY IN p.UK_classes
                RETURN m.name
        )
        LET number_members_in_same_region_with_same_class = LENGTH(members_in_same_region_with_same_class)
        
        /*
        LET members_in_same_region_with_same_group = (
            FOR m IN region_members
                FILTER m.UK_groups ANY IN p.UK_groups
                RETURN m.name
        )
        LET number_members_in_same_region_with_same_group = LENGTH(members_in_same_region_with_same_group)
        */
        
        //LET number_members_in_same_region_same_sectors = LENGTH(region_members[* FILTER CURRENT.UK_sectors ANY IN p.UK_sectors ])

        LET members_in_region_within_{max_km}km = (
            FOR m IN region_members
                FILTER m.latitude != NULL
                FILTER m.longitude != NULL
                LET d = DISTANCE(p.latitude, p.longitude,
                    m.latitude, m.longitude) / 1000
                FILTER d <= {max_km}
                
                RETURN m.name
        )
        LET number_members_in_region_within_{max_km}km = LENGTH(members_in_region_within_{max_km}km)

        {f"LIMIT {limit}" if limit is not None else ""}
        
        RETURN {{
            prospect_name: p.name,
            relevant_employees,
            members_in_region_relevant_for_supply_chain,
            number_members_in_region_relevant_for_supply_chain,
            members_in_same_region_with_same_class,
            number_members_in_same_region_with_same_class,
            //members_in_same_region_with_same_group,
            //number_members_in_same_region_with_same_group,
            //number_members_in_same_region_same_sectors,
            members_in_region_within_{max_km}km,
            number_members_in_region_within_{max_km}km,
        }}
        
    '''
    return aql_query(db, query, verbose=True)

def get_user_contact(db=None):

    if db is None:
        db = connect_to_mim_database()

    query = '''


    FOR u IN Users

        FILTER HAS(u, 'email')
        
        LET m = FIRST(
            FOR m IN 1 OUTBOUND u UserWorksAt
                RETURN m
        )
        
        FILTER m.membership_level IN ['Digital', 'Bronze', 'Silver', 'Gold', 'Platinum', 'Patron']
        
        LET postcodes = (
            FOR a IN m.addresses
                RETURN DISTINCT a.postcode
        )
        
        LET counties = (
            FOR a IN m.addresses
                RETURN DISTINCT a.county_name
        )
        

        RETURN {
            full_name: u.full_name,
            email: u.email,
            company_position: u.company_position,
            company_role: u.company_role,
            company_name: u.company_name,
            membership_level: m.membership_level,
            tenancies: CONCAT_SEPARATOR(", ", m.tenancies),
            sic_codes: CONCAT_SEPARATOR(", ", m.sic_codes),
            "region(s)": CONCAT_SEPARATOR(", ", m.regions),
            //place_name: m.place_name,
            "postcode(s)": CONCAT_SEPARATOR(", ", postcodes),
            "county_name(s)": CONCAT_SEPARATOR(", ", counties),
            "divisions(s)": CONCAT_SEPARATOR(", ", m.UK_divisions),
            "sector(s)": CONCAT_SEPARATOR(", ", m.UK_sectors),
        }
    

    '''

    return aql_query(db, query)

def get_members_without_UK_sector(db=None):

    if db is None:
        db = connect_to_mim_database()

    query = '''


    FOR m IN Members
        
        FILTER m.membership_level IN ['Digital', 'Bronze', 'Silver', 'Gold', 'Platinum', 'Patron']
        FILTER m.UK_sectors == NULL
        

        RETURN {
            member_name: m.name,
            membership_level: m.membership_level,
            tenancies: CONCAT_SEPARATOR(", ", m.tenancies),
            sic_codes: CONCAT_SEPARATOR(", ", m.sic_codes),
        }
    

    '''

    return aql_query(db, query)

def main():

    db = connect_to_mim_database()

    member_name = "Hana Tech ltd"
    max_distance = 5
    min_sectors = 2

    event_name = "Backing Britain Virtual Breakfast Morning with Hayley Group and Portakabin"
    max_km = 10

    region = "midlands"

    limit = 25

    # results = same_sector(member_name, max_distance, min_sectors, db=db)
    # results = commerce_match(member_name, max_distance, min_sectors, db=db)
    # results = most_followed_by_staff(member_name, db=db)
    # results = followed_by_followed(member_name, db=db)
    # results = followed_by_followers(member_name, db=db)
    # results = most_followed_of_followers(member_name, db=db)
    # results = future_events(db=db)
    # results = future_events_for_same_sector(member_name, db=db)
    # results = future_events_same_attendees_as_past_events(member_name, db=db)
    # results = explore_local_area(member_name, max_steps=3, max_results=10, db=db)
    # results = build_commerce_chains(member_name, limit=10, db=db)
    # results = uk_class_match(member_name, class_type="groups")
    # results = closest_prospects_to_event_hosts(event_name, max_km=max_km, db=db)
    # results = prospects_in_same_class_as_event_hosts(event_name, db=db)
    # results = prospects_with_commerce_connected_SIC_codes_to_event_hosts(event_name, db=db)
    # results = group_prospects(region, db=db)

    # results = recommend_prospects_to_events(event_name, db=db)

    collection = "Prospects"
    
    # results = recommend_members_based_on_SIC_commerce(limit=10, region=region, collection=collection)
    # results = recommend_members_based_on_SIC_commerce(
    #     region=region,
    #     # name=member_name, 
    #     collection=collection, 
    #     limit=10,
    #     db=db)
        
    # results = recommend_members_based_on_SIC_similarity(
    #     region=region,
    #     # name=member_name, 
    #     collection=collection, 
    #     limit=10,
    #     db=db)

    # results = recommend_prospects(region, 
        # member_name=None, max_km=max_km, limit=limit, source="Members", db=db)

    # results = get_prospects_with_directors_over_age(region, age=65, source="Prospects", limit=None, db=db)

    # results = match_members_by_number_of_relevant_articles(member_name, db=db )

    # results = find_prospect_users_by_role(region, source="Members", limit=None, db=db)

    results = get_members_without_UK_sector()

    for result in results[:5]:
        print (result)

    print ("number of results:", len(results))

    if not isinstance(results, pd.DataFrame):
        results = pd.DataFrame(results)

    results.to_csv("members_without_UK_sector.csv")

if __name__ == "__main__":
    main()