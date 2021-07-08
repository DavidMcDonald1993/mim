from fuzzywuzzy import fuzz

def find_best_matching_member(query, existing_members, matching_function=fuzz.partial_ratio):
    match_scores = {existing_member: 
        matching_function(query.lower(), existing_member.lower()) 
        for existing_member in existing_members}
    return  max(match_scores, key=match_scores.get)