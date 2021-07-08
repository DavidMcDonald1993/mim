import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir,)))

from utils.arango_utils import aql_query, connect_to_mim_database

from graphviz import Digraph

def main():

    db = connect_to_mim_database()

    # query = f'''

    # FOR m IN Members
    #     FILTER m.name=='ABB Robotics UK'

    #     FOR u, e, p IN 2 ANY m MembersToClass OPTIONS {{uniqueVertices: true}}
    #         FILTER IS_SAME_COLLECTION(u, 'Members')
    #         LIMIT  5

    #         RETURN p

    # '''

    # query = f'''
    # FOR m IN Members
    #     FILTER m.name=='ABB Robotics UK'

    #     FOR u, e, p IN 5 ANY m MembersToClass, ANY MemberMemberCommerce OPTIONS {{uniqueVertices: true}}
    #         FILTER IS_SAME_COLLECTION(u, 'Members')

    #         LIMIT 1

    #         RETURN p
    # '''

    # query = f'''
    # FOR c IN UKClasses
    #     FILTER c.type=='sector'
    #     FILTER c.identifier == 'MANUFACTURING'
    #     LIMIT 1
    #     FOR u, e, p IN 3 INBOUND c UKClassHierarchy
    #         LIMIT 5
    #         RETURN p

    # '''


    query = f'''
    FOR c IN UKClasses
        FILTER c.type=='sector'
        FILTER c.identifier == 'MANUFACTURING'
        LIMIT 1
        FOR u, e, p IN 4 INBOUND c UKClassHierarchy, INBOUND MembersToClass
            LIMIT 4
            RETURN p

    '''

    arango_graph = aql_query(db, query)

    graph_name = 'SIC_hierarchy_members'

    g = Digraph(graph_name, filename=graph_name, format='png', engine='dot')
    g.attr(scale='3', label='', fontsize='9', #size="10,10!"
    rankdir="LR",
    # rankdir="BT",
    )
    g.attr('node', width='2')

    for item in arango_graph:
        for vertex in item['vertices']:
            if "identifier" in vertex:
                name = vertex["identifier"]
                if name == "MANUFACTURING":
                    shape = "box"
                else:
                    shape="circle"
            else:
                name = vertex["name"]
                shape="box"
            g.node(vertex['_id'], label="\n".join(name.split()), shape=shape)
        for edge in item['edges']:
            g.edge(edge['_from'], edge['_to'], )
        # for i, edge in enumerate(item['edges']):
        #     if i == 0 or i == 3:
        #         g.edge(edge['_from'], edge['_to'], )
        #     elif i == 1 or i == 4:
        #         g.edge(edge['_to'], edge['_from'], )
        #     elif i == 2:
        #         g.edge(edge['_from'], edge['_to'], )
        #         g.edge(edge['_to'], edge['_from'], )


    # Render to file into some directory
    g.render(directory='.', filename=graph_name)
    # Or just show rendered file using system default program
    # g.view()

if __name__ == "__main__":
    main()