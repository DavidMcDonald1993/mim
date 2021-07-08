import pandas as pd

from pyArango.connection import Connection

import re 

import networkx as nx

import dotenv
dotenv.load_dotenv()

import os 

def sanitise_key(s):
    return re.sub(r"[^a-zA-Z0-9]", "_", s.lower())

def connect_to_mim_database( 
    ):
    host = os.getenv("ARANGOHOST")
    port = os.getenv("ARANGOPORT")
    username = os.getenv("ARANGOUSER")
    password = os.getenv("ARANGOPASSWORD")
    db = os.getenv("ARANGODB")
    protocol = os.getenv("ARANGOPROTOCOL")
    arango_url = f"{protocol}://{host}:{port}"
    conn = Connection(username=username, password=password, arangoURL=arango_url,)
    print ("CONNECTED TO DATABASE:", db, "at URL:", arango_url)
    try:
        return conn[db]
    except:
        print ("creating database", db)
        return conn.createDatabase(name=db)

def connect_to_collection(collection_name, db, className='Collection'):
    try:
        print ("connected to collection", collection_name)
        return db[collection_name]
    except:
        print ("creating collection", collection_name)
        return db.createCollection(className=className, name=collection_name)

def insert_document(db, collection, document, upsert_key="_key", verbose=False):
    assert isinstance(document, dict)

    # remove None values
    document = {k: v for k, v in document.items()
        if v is not None and (isinstance(v, list) or not pd.isnull(v))}

    if verbose:
        print ("inserting document:", document)

    upsert_query = f"""
    UPSERT {{ {upsert_key}: '{document[upsert_key]}' }}
        INSERT {document}
        UPDATE {document}
    IN {collection.name}
    """


    return db.AQLQuery(upsert_query)

def aql_query(db, query, verbose=False):
    if verbose:
        print ("executing query:", query)
    return [record 
        for record in db.AQLQuery(query, rawResults=True, )] # raw results returns dictionaries

def name_to_id(db, collection, names):

    if not isinstance(names, list):
        names = [names]

    aql = f'''
    FOR doc IN {collection}
        {" ".join((f"FILTER doc.{name} != NULL" for name in names))}
        RETURN {{
            _id: doc._id,
            {",".join((f"{name}: doc.{name}" for name in names))}
        }}
    '''
    records = aql_query(db, aql)
    
    records_dict =  {"_".join((record[name] for name in names)): record["_id"]
        for record in records}
    # assert len(records) == len(records_dict), (len(records), len(records_dict))
    if len(records) != len(records_dict):
        print (len(records), len(records_dict))
        seen = set()
        dupes = []
        for key in [
            "_".join((record[name] for name in names))
                for record in records
        ]:
            if key in seen:
                dupes.append(key)
            seen.add(key)
        print (dupes)
        raise Exception
    return records_dict

def create_networkx_graph(
    graph_name, 
    graph_attributes, 
    create_using=nx.MultiDiGraph,
    db=None):

    if db is None:
        db = connect_to_mim_database()

    g = create_using()
    for k, v in graph_attributes['vertexCollections'].items():
        query = f"FOR doc in {k} "
        if len(v) == 0: # return all
            csps = "doc"

        else:
            cspl = [s + ':' + 'doc.' + s for s in v]
            cspl.append('_id: doc._id')
            csps = "{" + ','.join(cspl) + "}"
            
        query = query + "RETURN " + csps

        cursor = aql_query(db, query)
        for doc in cursor:
            g.add_node(doc['_id'], 
            # attr_dict=doc
             **{k: v for k, v in doc.items() 
                if k not in {"_id", "_to", "_from"}})

    for k, v in graph_attributes['edgeCollections'].items():
        query = f"FOR doc in {k} "
        if len(v) == 0: # return all
            csps = "doc"

        else:

            cspl = [s + ':' + 'doc.' + s for s in v]
            cspl.append('_id: doc._id')
            csps = "{" + ','.join(cspl) + "}"
       
        query = query + "RETURN " + csps

        cursor = aql_query(db, query)
        for doc in cursor:
            g.add_edge(doc['_from'], doc['_to'], 
            # attr_dict=doc
            **{k: v for k, v in doc.items() 
                if k not in {"_id", "_to", "_from"}}
        )

    return g


if __name__ == "__main__":

    db = connect_to_mim_database()

    # vertex_collections = {
    #     "Members",
    #     # "Users",
    #     # "Events",
    #     # "Commerces",
    #     # "Sectors",
    # }

    # edge_collections = {
    #     # "MemberMemberBusiness",
    #     # "MemberMemberSector",
    #     # "MemberMemberCommerce",
    #     # "MemberMemberFollows",
    #     "MemberMemberEvents",
    #     # "UserWorksAt",
    #     # "UserFollows",
    #     # "Messages",
    #     # "InSector",
    #     # "PerformsCommerce",
    # }
 
    # attributes = { 
    #     'vertexCollections':
    #     {
    #         vertex_collection: {"name", "about_company"} 
    #             for vertex_collection in vertex_collections 
    #     },\
    # 'edgeCollections' :
    #     {
    #         edge_collection:  {'_from', '_to', "name"} 
    #             for edge_collection in edge_collections
    #     }
    # }


    # g = create_networkx_graph("test-graph", attributes)

    # print(len(g), len(g.edges))
    # # for n in g:
    #     # print (n)
    # # print (list(g.nodes(data=True))[:5])