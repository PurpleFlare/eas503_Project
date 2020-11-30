# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 09:54:59 2020

@author: Sam
"""

import pandas as pd
import numpy as np

#change your file path
homeless = pd.read_csv('C:/Users/Sam/Documents/2020Fall/EAS503/Code/Project/homeless_impact.csv')
print(homeless.head())
print(homeless['county'].nunique())
print(homeless['county'].value_counts())

#change your file path
hospital = pd.read_csv('C:/Users/Sam/Documents/2020Fall/EAS503/Code/Project/hospitals_by_county.csv')
print(hospital.head())

db_list = []
key = 0
for row_name, item in homeless.iterrows():
    temp = []
    temp.append(str(key))
    for x in item:
        temp.append(x)
    db_list.append(temp)
    key = key+1
#print(db_list)


def create_connection(db_file, delete_db=False):
    import os
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    
    return rows
  
  def insert_homeless(conn, values):
    sql = """ INSERT INTO HOMELESS VALUES(?,?,?,?,?,?,?,?)"""
    cur = conn.cursor()
    #print(values)
    cur.execute(sql, values)
    return cur.lastrowid

create_homeless_table_sql = """CREATE TABLE [HOMELESS] (
    [PK] TEXT NOT NULL PRIMARY KEY,
    [COUNTY] TEXT NOT NULL,
    [DATE] TEXT NOT NULL,
    [ROOMS] FLOAT,
    [ROOM_OCCUPIED] FLOAT,
    [TRAILERS_REQUESTED] FLOAT,
    [TRAILERS_DELIVERED] FLOAT,
    [DONATED_TRAILERS_DELIVERED] FLOAT
    );"""

db_file = 'homeless5.db'
conn = create_connection(db_file, True)
with conn:
    create_table(conn, create_homeless_table_sql)
    for ele in db_list:
        insert_homeless(conn, ele)
sql_statement = "select * FROM HOMELESS"
df = pd.read_sql_query(sql_statement, conn)
display(df)
