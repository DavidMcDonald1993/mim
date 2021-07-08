import sys
import os.path
sys.path.insert(1, 
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import os

import dotenv
dotenv.load_dotenv()

import mysql.connector
from mysql.connector.errors import ProgrammingError

import re

import json 

separator = ";"

def sanitise_args(args):
    if args is None:
        return None 

    if isinstance(args, str):
        return args

    if isinstance(args, set) or isinstance(args, list):
        args = tuple(args)
    assert isinstance(args, tuple)
    if len(args) == 1:
        args = args[0]
    return args

def load_json(filename):
    with open(filename, "r") as f:
        return json.load(f)

# def load_mysql_credentials(credential_filename="mysql_credentials.json"):
#     return load_json(credential_filename)

def connect_to_mysqldb(
    host=None, 
    port=None, 
    user=None, 
    password=None, 
    database=None,
    ):
    if host is None:
        host = os.getenv("DBHOST")
    if port is None:
        port = os.getenv("DBPORT")
    if user is None:
        user = os.getenv("DBUSER")
    if password is None:
        password=os.getenv("DBPASSWORD")
    if database is None:
        database = os.getenv("DB")
    print ("connecting to MySQL database", 
        database, "using host",
        host, "port", port, "and user", user)
    db = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
    )
    return db

def mysql_query(query, return_cols=False, to_json=True, existing_conn=None):

    if existing_conn is None:
        db = connect_to_mysqldb()
    else:
        db = existing_conn

    print ("executing MySQL query:", query)

    cursor = db.cursor(dictionary=to_json, )

    cursor.execute("SET sort_buffer_size = 256000000")
    cursor.execute("SET group_concat_max_len = 1000000")
    cursor.execute(query)

    records = cursor.fetchall()
    print ("NUMBER OF HITS:", len(records))

    if return_cols:
        records = records, cursor.column_names

    if existing_conn is None:
        cursor.close()
        db.close()

    print ()

    return records

def mysql_create_table(create_table, existing_conn=None):
    if existing_conn is None:
        db = connect_to_mysqldb()
    else:
        db = existing_conn

    cursor = db.cursor()
    try:
        cursor.execute(create_table)
        print ("executed command", create_table)

    except ProgrammingError as e: # table already exists
        print (e)
        pass

    if existing_conn is None:
        db.close()

    return 0

def mysql_insert_many(sql, rows, existing_conn=None, chunksize=1000000):

    def to_chunks(rows):
        chunk = []

        for row in rows:
            chunk.append(row)
            if len(chunk) == chunksize:
                yield chunk
                chunk = []
        if len(chunk) > 0:
            yield chunk

    if existing_conn is None:
        db = connect_to_mysqldb()
    else:
        db = existing_conn

    cursor = db.cursor()

    if isinstance(rows, list):
        print ("inserting", len(rows), "rows")
        cursor.executemany(sql, rows)
    else:

        for chunk in to_chunks(rows):
            print ("inserting", len(chunk), "rows")
            cursor.executemany(sql, chunk)

    db.commit()

    if existing_conn is None:
        db.close()

    return 0

def sanitise_names(names):
    return  [
        re.sub(r"( |\+|-|\*|/|=|<|>|\(|\)|,|\.|'|\[|\]|:|;|{|})", "_", name)
        for name in names]

# def to_json(records, cols):
#     return [
#         {c:r for c, r in zip(cols, record)}
#         for record in records
#     ]

if __name__ == "__main__":
    
    # query = '''
    # SELECT m.name, GROUP_CONCAT(c.name SEPARATOR ':_:'), GROUP_CONCAT(mcc.name SEPARATOR ':_:')
    # FROM members AS `m`
    # INNER JOIN member_buys_commerce AS mbc
    #     ON (m.id=mbc.member_id)
    # INNER JOIN member_commerces AS `c` 
    #     ON (mbc.member_commerce_id=c.id)
    # INNER JOIN member_commerce_categories AS `mcc`
    #     ON (c.category_id=mcc.id)
    # GROUP BY m.name
    # ''' 

    # records = mysql_query(query)

    # print (records[0])

    # query = '''
    # SELECT name
    # FROM members
    # WHERE id IN 
    #     (
    #         SELECT DISTINCT `mbc`.member_id
    #         FROM member_buys_commerce AS `mbc`
    #         INNER JOIN member_sells_commerce AS `msc`
    #             ON (mbc.member_id=msc.member_id)
    #     )
    # '''

    # records = mysql_query(query)

    db = connect_to_mysqldb()

    print (db)