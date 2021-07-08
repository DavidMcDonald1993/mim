import numpy as np
import pandas as pd 



def infer_role(row):

    '''
    ROLES

    Board director    
    Sales             
    Senior Management 
    Marketing         
    Purchasing        
    HR                
    Other 
    '''

    if not pd.isnull(row["company_role"]):
        return row["company_role"]

    position = row["company_position"]
    if pd.isnull(position):
        return position

    position = position.lower()

    if "market" in position:
        return "Marketing"

    if "sale" in position:
        return "Sales"

    if "human" in position or "hr" in position:
        return "HR"

    if "director" in position or "md" in position or "m.d" in position or "ceo" in position or "c.e.o" in position:
        return "Board director"

    if "senior" in position or "manage" in position:
        return "Senior Management"

    return np.NaN

def main():

    filename = "paid_users_without_company_role.csv"

    paid_users = pd.read_csv(filename, index_col=0)
    paid_users["company_role"] = np.NaN

    print (pd.isnull(paid_users["company_role"]).sum())

    paid_users["company_role"] = paid_users.apply(infer_role, axis=1)

    print (pd.isnull(paid_users["company_role"]).sum())

    paid_users.to_csv("paid_users_without_company_role_partial.csv")


if __name__ == "__main__":
    main()