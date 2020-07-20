# -*- coding: utf-8 -*-
#
#   Lambda Function - With Lambda handler
#   Created:      11 jun 2020
#   Last up:      20 jul 2020
#
#   Dependencies:   sqlalchemy
#                   numpy
#                   pandas
#                   psycopg2-binary
#                   plotly==4.8.1
#                   boto3
#                   psutil
#                   requests
#
#   Examples:   python main.py local get
#               python main.py local get bar s3
#               python main.py aws get bar s3

import os
import sqlalchemy
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


# Get data
def get():
    print("\nGet data")

    # Init
    dbUrl = POSTGRES_URI
    print(dbUrl)
    engine = sqlalchemy.create_engine(dbUrl)    # Create connection to Postgres Database

    # Get data from database
    print('Get data')
    #df = read_to_dataframe(""" SELECT * FROM "STAGE_TRIAJE" LIMIT 13 """, engine)
    #df = read_to_dataframe(""" SELECT * FROM "STAGE_TRIAJE" WHERE sex IS NOT NULL """, engine)
    #df = read_to_dataframe(""" SELECT * FROM "STAGE_TRIAJE" LIMIT 100000 """, engine)
    df = read_to_dataframe(""" SELECT * FROM "STAGE_TRIAJE" LIMIT 1000 """, engine)
    #df = read_to_dataframe(""" SELECT * FROM "STAGE_TRIAJE" """, engine)
    print(df)
    print()

    # Write to csv
    print('Write csv')
    #df.to_csv('triage.csv')
    df.to_csv('out/triage.csv')


# Create figure
def bar():
    print("\nPlot Bar")

    # Read csv
    print('Read csv')
    #df = pd.read_csv('triage.csv')
    df = pd.read_csv('out/triage.csv')
    print(df)

    # Count frequency
    print('Count frequency')
    #print(df['created_at_fecha'])
    counts = df['created_at_fecha'].value_counts()
    print(counts)
    print()

    # Stats
    length = str(len(df))
    #length = str(len(counts))
    print(length)

    title = 'Triajes en gob.pe'
    text = 'created_at_fecha'
    labels = {'created_at_fecha':'Fecha de creacion'}

    # Create figure - bar
    print('Create figure')
    #fig = px.bar(dic, x='created_at_fecha', y='count')
    #fig = px.bar(counts, title=title, labels=labels)
    fig = px.bar(counts)
    fig.update_traces(marker_color='rgb(158,202,225)', marker_line_color='rgb(8,48,107)', marker_line_width=1.5, opacity=0.6)
    fig.update_layout(title_text='Triajes = ' + length)

    #  Show
    fig.show()

    #  Write to file
    print('Write to html')
    #fig.write_html('./triage_bar.html', auto_open=False)
    fig.write_html('out/triage_bar.html', auto_open=False)

    #print('Write to figure')
    #fig.write_image('./triage_bar.png')
    #fig.write_image('./triage_bar.svg')
    #fig.write_image('./triage_bar.jpg')
    #fig.write_image('./triage_bar.pdf')


# Write on s3 bucket
def s3():
    import boto3

    print("\nWrite to S3 bucket")

    ACCESS_ID = os.environ['ACCESS_ID']
    ACCESS_KEY = os.environ['ACCESS_KEY']

    print('Get resource')
    #s3 = boto3.resource("s3")
    s3 = boto3.resource('s3', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)
    print(s3)

    # init
    bucket_name = "gobpeanalytics"

    string = "dfghj"
    encoded_string = string.encode("utf-8")
    file_name = "hello_from_mac.txt"

    print('Put into bucket')
    s3.Bucket(bucket_name).put_object(Key=file_name, Body=encoded_string)
    s3.Object(bucket_name, 'triage_bar.html').put(Body=open('./triage_bar.html', 'rb'), ContentType='html')


# Aux func
def read_to_dataframe(sql_query, engine):
    print('Read to dataframe')
    table = pd.read_sql(sql_query, engine)
    return table



# Main
import argparse
import datetime
import time
import os

# Parse arguments
parser = argparse.ArgumentParser(description='Gobpe analytics.')
parser.add_argument('tasks', metavar='T', type=str, nargs='+', help='a task for the accumulator')
args = parser.parse_args()
#print(args)
#print(args.tasks)


print("\nGobpe Analytics\n")

# Timer - begin
beg = datetime.datetime.now().time()
print(beg)
start = time.time()
#print(start)
print()


# Database
if 'local' in args.tasks:
    POSTGRES_URI = os.environ['POSTGRES_LOCAL']
if 'aws' in args.tasks:
    POSTGRES_URI = os.environ['POSTGRES_AWS']
print(POSTGRES_URI)


# Tasks
if 'get' in args.tasks:
    get()
if 'bar' in args.tasks:
    bar()
if 's3' in args.tasks:
    s3()


# Timer - end
end = datetime.datetime.now().time()
print(end)
ending = time.time()
#print(ending)
delta = ending - start
print()
print(delta)
print()
