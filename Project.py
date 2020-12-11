# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 09:54:59 2020
@author: Sam
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from sqlite3 import Error
from IPython.display import display 

#change your file path
homeless = pd.read_csv('C:/Users/Sam/Documents/2020Fall/EAS503/Code/Project/homeless_impact.csv')
#print(homeless.head())
#print(homeless['county'].nunique())
#print(homeless['county'].value_counts())

#change your file path
hospital = pd.read_csv('C:/Users/Sam/Documents/2020Fall/EAS503/Code/Project/hospitals_by_county.csv')
#print(hospital.head())

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

db_list2 = []
key = 0
for row_name, item in hospital.iterrows():
    temp = []
    temp.append(str(key))
    for x in item:
        temp.append(x)
    db_list2.append(temp)
    key += 1
#print(db_list2[0])


def create_connection(db_file, delete_db=False):
    import os
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
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

def insert_hospital(conn, values):
    sql = """ INSERT INTO HOSPITAL VALUES(?,?,?,?,?,?,?,?,?,?)"""
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

create_hospital_table_sql = """CREATE TABLE [HOSPITAL] (
    [PK] TEXT NOT NULL PRIMARY KEY,
    [COUNTY] TEXT NOT NULL,
    [DATE] TEXT NOT NULL,
    [HOSPITALIZED_COVID_CONFIRMED_PATIENTS] INTEGER,
    [HOSPITALIZED_SUSPECTED_COVID_PATIENTS] INTEGER,
    [HOSPITALIZED_COVID_PATIENTS] INTEGER,
    [ALL_HOSPITAL_BEDS] INTEGER,
    [ICU_COVID_CONFIRMED_PATIENTS] INTEGER,
    [ICU_SUSPECTED_COVID_PATIENTS] INTEGER,
    [ICU_AVAILABLE_BEDS] INTEGER
    );"""    
    
db_file = 'homeless5.db'
conn = create_connection(db_file, True)

with conn:
    create_table(conn, create_homeless_table_sql, 'HOMELESS')
    for ele in db_list:
        insert_homeless(conn, ele)
        
    create_table(conn, create_hospital_table_sql, 'HOSPITAL')
    for ele in db_list2:
        insert_hospital(conn, ele)

sql_statement = "select * FROM HOMELESS"
df = pd.read_sql_query(sql_statement, conn)
display(df)

sql_statement = "select * FROM HOSPITAL"
df = pd.read_sql_query(sql_statement, conn)
display(df)


sql_statement = "select DISTINCT County FROM HOMELESS" #Contains 172 'Counties' Looks to include Native American Tribes (unfortunate in reality)
df = pd.read_sql_query(sql_statement, conn)
display(df)

sql_statement = "select DISTINCT County FROM HOSPITAL" #There are only 55 unique counties, there are 58 counties in Cal
df = pd.read_sql_query(sql_statement, conn)
display(df)

sql_statement = "select County, avg(HOSPITALIZED_COVID_PATIENTS) as Average_Hospitalized_Covid_Patients  FROM HOSPITAL GROUP BY County order by Average_Hospitalized_Covid_Patients desc" #On average LA has the highest weekly average hospitilized
df = pd.read_sql_query(sql_statement, conn)
display(df)

plt.figure();
df.plot(kind = 'bar')


sql_statement = "select sum(HOSPITALIZED_COVD_CONFIRMED_PATIENTS + ICU_COVID_CONFIRMED_PATIENTS + ROOM_OCCUPIED) from Hospital left join Homeless on Hospital.COUNTY=Homeless.COUNTY"
