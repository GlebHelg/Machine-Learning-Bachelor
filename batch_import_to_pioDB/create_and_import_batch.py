#This file can be executed by a valid python interpreter
#Execution syntax: 
# python scriptName.py -s some -a args

#    datetime necessary for manipulating dates and times
#predictionio necessary for creation of events in correct JSON-event format
#        pytz necessary for accurate, cross platform timezone calculations (python time zone)
#          os necessary for importing environment variables
#    psycopg2 necessary for communicating with PostgreSQL database
#    argparse necessary for parsing of arguments to this script
#         sys necessary for call to system
from subprocess  import call
from collections import defaultdict
import predictionio
import pytz
import os
import psycopg2
import argparse
import sys
import datetime
import time

#Following SQL senteces only show complexity.

#SQL-Statement 1: First time an item was added. For: $set item event in pio.
sqlArticles = """
SELECT 
    confidential.conf AS item, 
    confidential.conf AS category, 
    confidential.conf AS timestamp 
FROM 
    confidential"""

#SQL-Statement 2: First time a customer bought a product. For: $set user event in pio.
sqlUsers = """
SELECT 
    confidential.conf      AS user, 
    MIN(confidential.conf) AS timestamp 
FROM 
    confidential, confidential, confidential 
WHERE 
    confidential.conf = confidential.conf 
AND 
    confidential.conf = confidential.conf
AND 
    confidential.conf IS NOT NULL 
AND 
    confidential.conf IS NOT NULL 
GROUP BY 
    confidential.conf
ORDER BY 
    timestamp ASC """

#SQL-Statement 3: user-view-events that are also user-buy-events, when timestamp modified.
sqlUserShop = """
SELECT 
    confidential.conf AS user, 
    confidential.conf AS item, 
    confidential.conf AS timestamp 
FROM 
    confidential, confidential, confidential 
WHERE 
    confidential.conf = confidential.conf 
AND 
    confidential.conf = confidential.conf 
AND 
    confidential.conf IS NOT NULL 
ORDER BY 
    confidential.conf"""

#Command Line Execution permits change of default arguments for database connection
parser = argparse.ArgumentParser(description = "Specify IP, DB, PORT and USER")
parser.add_argument(  '--ip', default='localhost')
parser.add_argument('--port', default='5432'     )
parser.add_argument(  '--db', default='rk1'      )
parser.add_argument(   '--u', default='postgres' )
args = parser.parse_args()

def dbConnector():
    #V for Value. Secret environment variables are set using direnv, and extracted from environment by os.environ[].
    hostV = args.ip
    portV = args.port
    dbV   = args.db
    userV = args.u
    passV = os.environ['PASSWORD']

    print """
    Connecting to PostgreSQL service using:
    ip      : %s
    port    : %s
    db      : %s
    username: %s
    """ % (args.ip, args.port, args.db, args.u)

    #Connecting to PostgreSQL Database
    connection = psycopg2.connect(host=hostV, port=portV, database=dbV, user=userV, password=passV)

    return connection.cursor()

def jsonSetItem(cursor, exporter):
    # 0:item, 1:properties, 2:timestamp
    cursor.execute(sqlArticles+limit)
    rowCount = cursor.rowcount

    i = 0
    while i < rowCount:
        row = cursor.fetchone()

        event_properties = {
            "categories":[row[1]]
        }

        event_response = exporter.create_event(
        event              = "$set"                          ,
        entity_type        = "item"                          ,
        entity_id          = row[0]                          ,
        #target_entity_type = ""                              ,
        #target_entity_id   = ""                              ,
        properties         = event_properties                ,
        event_time         = row[2].replace(tzinfo = pytz.utc))

        i += 1

def jsonSetUser(cursor, exporter):
    # 0:user, 1:timestamp
    cursor.execute(sqlUsers+limit)
    rowCount = cursor.rowcount

    i = 0
    while i < rowCount:
        row = cursor.fetchone()

        event_properties = {}

        event_response = exporter.create_event(
        event              = "$set"                          ,
        entity_type        = "user"                          ,
        entity_id          = row[0]                          ,
        #target_entity_type = ""                              ,
        #target_entity_id   = ""                              ,
        properties         = event_properties                ,
        event_time         = row[1].replace(tzinfo = pytz.utc))       

        i += 1

def jsonUserViewItem(cursor, exporter):
    # 0:user, 1:item, 2:timestamp
    cursor.execute(sqlUserShop+limit)
    rowCount = cursor.rowcount

    i = 0
    while i < rowCount:
        row = cursor.fetchone()

        event_properties = {}

        adjuTime = row[2] - datetime.timedelta(0,0,0,0,2)

        event_response = exporter.create_event(
        event              = "view"                             ,
        entity_type        = "user"                             ,
        entity_id          = row[0]                             ,
        target_entity_type = "item"                             ,
        target_entity_id   = row[1]                             ,
        properties         = event_properties                   ,
        event_time         = adjuTime.replace(tzinfo = pytz.utc))

        i += 1

def jsonUserBuyItem(cursor, exporter):
    # 0:user, 1:article, 2:timestamp
    cursor.execute(sqlUserShop+limit)
    rowCount = cursor.rowcount
    i = 0
    while i < rowCount:
        row = cursor.fetchone()

        event_properties = {}

        event_response = exporter.create_event(
        event              = "buy"                            ,
        entity_type        = "user"                           ,
        entity_id          = row[0]                           ,
        target_entity_type = "item"                           ,
        target_entity_id   = row[1]                           ,
        properties         = event_properties                 ,
        event_time         = row[2].replace(tzinfo = pytz.utc))

        i += 1
#
#end jsonUserBuyItem()

# mainController executes SQL's, and send them apropriately to be filled in JSON format
def mainController(cursor):
    # FileExporter is preparing a file for write
    exporter = predictionio.FileExporter(file_name="my_events.json")
    
    jsonSetItem(cursor, exporter)

    jsonSetUser(cursor, exporter)

    jsonUserViewItem(cursor, exporter)

    jsonUserBuyItem(cursor, exporter)
    
    # close the FileExporter when finish writing all events
    exporter.close()
#
#end mainController()

# Creating cursor so as to permit execution of PostgreSQL commands
cursor = dbConnector()
# limit must be set to empty string or semicolon when not testing
limit = ' LIMIT 2'

start_time = time.time()
mainController(cursor)
call(["pio", "import", "--appid", "1", "--input", "my_events.json", "--" "--driver-class-path", "/usr/share/java/postgresql-jdbc4.jar"])
printf "--- %s seconds ---", time.time() - start_time
