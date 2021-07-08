west_midlands_postcodes = {
    "B", "CV", "DY", "HR", 
    # "LD", 
    # "NP", 
    "ST", 
    # "SY", 
    "TF", "WR", "WS", "WV"
}

east_midlands_postcodes = {
    # "CV", 
    "DE", "DN", "LE", "LN", 
    # "MK", 
    "NG", 
    # "NN", "OX", 
    "S", #"SK"
    "PE", "IP", "NR", "CB", "CO",
}

yorkshire_postcodes = {
    # "BD", "DL", 
    "DN", "HD", "HG", "HU", "HX", 
    # "LA", 
    "LS", "S", "WF", "YO",
}

all_region_postcodes = {
    "midlands": west_midlands_postcodes.union(east_midlands_postcodes),
    "yorkshire": yorkshire_postcodes,
}