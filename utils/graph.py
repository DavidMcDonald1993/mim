import networkx as nx
import ast

def construct_all_commerces_dict(member_commerce_relations):
    assert isinstance(member_commerce_relations, dict)

    all_commerces = dict()

    for member_commerce_relation in member_commerce_relations:
        member_name = member_commerce_relation["name"]
    
        if "sic_codes" in member_commerce_relation:
            sic_codes = member_commerce_relation["sic_codes"]
        else:
            sic_codes = None
        
        sells = member_commerce_relation["sells"]
        if sells is not None:
            sells = ast.literal_eval(sells)
            for commerce in sells:
                commerce_name = commerce["commerce_name"]
                if commerce_name not in all_commerces:
                    all_commerces[commerce_name] = {
                        "commerce_category": commerce["commerce_category"], 
                        "member_buys": set(),
                        "SIC_buys": set(),
                        "member_sells": set(),                   
                        "SIC_sells": set(),
                    }
                all_commerces[commerce_name]["member_sells"].add(member_name)
                if sic_codes is not None:
                    all_commerces[commerce_name]["SIC_sells"] = \
                        all_commerces[commerce_name]["SIC_sells"].union(sic_codes)
                    
        buys = member_commerce_relation["buys"]
        if buys is not None:
            buys = ast.literal_eval(buys)
            for commerce in buys:
                commerce_name = commerce["commerce_name"]
                if commerce_name not in all_commerces:
                    all_commerces[commerce_name] = {
                        "commerce_category": commerce["commerce_category"], 
                        "member_buys": set(),
                        "SIC_buys": set(),
                        "member_sells": set(),                   
                        "SIC_sells": set(),
                    }
                all_commerces[commerce_name]["member_buys"].add(member_name)
                if sic_codes is not None:
                    all_commerces[commerce_name]["SIC_buys"] = \
                        all_commerces[commerce_name]["SIC_buys"].union(sic_codes)

    all_commerces = {
        commerce : {
            k: (list(v) if isinstance(v, set) else v)
            for k, v in data.items()
        }
        for commerce, data in all_commerces.items()
    }

def construct_member_member_graph():
    sells_relations = get_sells_relations()
    buys_relations = get_buys_relations()



    for member, commerces, categories in sells_relations:
        for commerce in commerces.split(":_:"):
            if commerce not in all_commerces:
                all_commerces[commerce] = {"sells": set(), "buys": set()}
            all_commerces[commerce]["sells"].add(member)


    for member, commerces, categories in buys_relations:
        for commerce in commerces.split(":_:"):
            if commerce not in all_commerces:
                all_commerces[commerce] = {"sells": set(), "buys": set()}
            all_commerces[commerce]["buys"].add(member)

    g = nx.DiGraph()

    for commerce in all_commerces:

        for seller, buyer in product(all_commerces[commerce]["sells"],
            all_commerces[commerce]["buys"]):
            g.add_edge(seller, buyer, commerce=commerce)

    return g

def construct_member_commerce_bipartite_graph():
    sells_relations = get_sells_relations()
    buys_relations = get_buys_relations()

    g = nx.DiGraph()

    for member, commerces, categories in sells_relations:
        if member not in g:
            g.add_node(member, type="member")
        for commerce in commerces.split(":_:"):
            if commerce not in g:
                g.add_node(commerce, type="commerce", shape="box")
            g.add_edge(member, commerce)

    for member, commerces, categories in buys_relations:
        if member not in g:
            g.add_node(member, type="member")
        for commerce in commerces.split(":_:"):
            if commerce not in g:
                g.add_node(commerce, type="commerce", shape="box")
            g.add_edge(commerce, member)

    return g