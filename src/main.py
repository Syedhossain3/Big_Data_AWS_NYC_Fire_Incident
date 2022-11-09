from sodapy import Socrata
from datetime import datetime, date
# from dateutil import parser
import requests
from requests.auth import HTTPBasicAuth
import json
import argparse
import sys
import os

# Creates a parser. Parser is the thing where you add your arguments. 
parser = argparse.ArgumentParser(description='Fire incident dispatchment data')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
print(args)

DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]

# #This comes from the documentation:
# #https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9
# DATASET_ID="8m42-w767"
# APP_TOKEN="H0rS3DX1i01iABJfq0ajtrOQX"
# ES_HOST="https://search-bigdata9760-syed-hossain-3gwtsvmh52ylsxzk7udrkezidu.us-east-1.es.amazonaws.com"
# ES_USERNAME=""
# ES_PASSWORD="!"
# INDEX_NAME="requests"


if __name__ == '__main__':
    try:
        #Using requests.put(), we are creating an index (db) first.
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                json={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    },
                    "mappings": {
                        
                        "properties": {
                            "starfire_incident_id": {"type": "keyword"},
                            "incident_datetime": {"type": "date"},
                            "alarm_box_location": {"type": "keyword"},
                            "communitydistrict": {"type": "number"},
                            "communityschooldistrict": {"type": "number"},
                            "incident_borough": {"type": "keyword"},
                            "dispatch_response_seconds_qy": {"type": "number"},
                            "ladders_assigned_quantity": {"type": "number"},
                            "incident_response_seconds_qy":{"type": "number"},
                            "engines_assigned_quantity": {"type": "number"},
                            "incident_classification": {"type": "keyword"},
                            "alarm_box_number": {"type": "number"},
                            "policeprecinct": {"type": "number"},
                            "citycouncildistrict": {"type": "number"},
                            "congressionaldistrict": {"type": "number"},
                            "first_assignment_datetime": {"type": "date"},
                            "incident_close_datetime": {"type": "date"},
                            "first_activation_datetime": {"type": "date"},
                            "incident_travel_tm_seconds_qy": {"type": "number"},
                            "other_units_assigned_quantity": {"type": "number"},
                            "zipcode": {"type": "number"} 
                            
                        }
                    },
                }
            )
        resp.raise_for_status()
        # print(resp.json())
    except Exception as e:
        print("Index already exists! Skipping")    
    
    def fields(filed, type, current_row, esrow):
        for key,value in current_row.items():
            if filed in key:
                if type == 'number':
                    # print(key + " " + str(int(value)))
                    esrow[key] = int(value)
                elif type == 'date':
                    esrow[key] = value
                    # esrow[key] = datetime.strptime(esrow[key] , "%Y-%m-%dT%H:%M:%S.%f").date()
                    # print(key + " " + str(datetime.datetime.strptime(value , "%Y-%m-%dT%H:%M:%S.%f"))
                elif type == 'float':
                    # print(key + " " + str(float(value)))
                    esrow[key] = float(value)
                elif type == 'text':
                    # print(key + " " + str(float(value)))
                    esrow[key] = str(value)
        
    # Remove the comments
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
    rows = int(client.get(DATASET_ID, select='COUNT(*)')[0]['COUNT'])
    
    if args.num_pages is None:
        # print(args.num_pages)
        args.num_pages = row // args.page._size + 1
    
    # 	es_rows=[]
    for x in range(0, args.num_pages):
        rows = client.get(DATASET_ID, limit=args.page_size , offset=x*(args.page_size))
        print("-->" + str(x*args.page_size))
        es_rows=[]

        # Print out a few specific columns instead of the entire output
        for row in rows:
            try:
                es_row = {}
                fields('starfire_incident_id', 'text', row, es_row) # int
                fields('incident_datetime', 'date', row, es_row) #tesy
                fields('alarm_box_location', 'text', row, es_row)
                fields('communitydistrict', 'number', row, es_row)
                fields('communityschooldistrict', 'number', row, es_row)
                fields('incident_borough', 'text', row, es_row)
                fields('dispatch_response_seconds_qy', 'number', row, es_row)
                fields('ladders_assigned_quantity', 'number', row, es_row)
                fields('engines_assigned_quantity', 'number', row, es_row)
                fields('incident_classification', 'text', row, es_row) # int
                fields('alarm_box_number', 'number', row, es_row)
                fields('policeprecinct', 'number', row, es_row)
                fields('citycouncildistrict', 'number', row, es_row)
                fields('congressionaldistrict', 'number', row, es_row)
                fields('first_assignment_datetime', 'date', row, es_row)
                fields('incident_close_datetime', 'date', row, es_row)
                fields('first_activation_datetime', 'date', row, es_row)
                fields('incident_response_seconds_qy', 'number', row, es_row)
                fields('incident_travel_tm_seconds_qy', 'number', row, es_row)
                fields('other_units_assigned_quantity', 'number', row, es_row)
                fields('zipcode', 'number', row, es_row)
                # print(es_row)
            except Exception as e:
                print (f"Error!: {e}, skipping row: {es_row}") #//comment temporary
                continue
            es_rows.append(es_row)
            # print(es_rows) #//see all rows
                
               
                
                # print(resp.json())
        bulk_upload_data = ""
        for line in es_rows:
            # print(f'Handling row {line["starfire_incident_id"]}') //comment temporary
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
        # print (bulk_upload_data)
        
        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            print ('Done')
                
            # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}")



    

        
        