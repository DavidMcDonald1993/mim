import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))


from utils.arango_utils import connect_to_mim_database, aql_query 


def main():

    db = connect_to_mim_database()

    get_edges_query = f'''
    FOR edge IN MemberMemberBusiness
        RETURN edge
    '''

    edges = aql_query(db, get_edges_query)

    print (len(edges))

if __name__ == "__main__":
    main()