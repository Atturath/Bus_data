# The main purpose is to get API every one hour and store it in the cloud database (MongoDB)
# There are four main parts: 1.set up API, 2.set up database access, 3.main function to load str data into json and store it in cloud, and 4.schedule
# Next plan: using VPS to automate the task

import json
import requests
from pymongo import MongoClient
import schedule
import time

#-------------API-----------#
key = '********************'

headers = {
    'Ocp-Apim-Subscription-Key': key
}

url = 'https://api.at.govt.nz/realtime/legacy'

#-------------MongoDB-----------#
client = MongoClient("mongodb+srv://<username>:<password>@cluster0.7gfdq86.mongodb.net/test")
db = client.get_database('AT_Bus')
records = db.AT_Bus

#-------------Data Structure-----------#


#-------------Main Function-----------#
def Bus_data():
    # Make the API request
    response = requests.get(url,headers=headers)

    print(response.status_code)

    # Check the response status code
    if response.status_code == 200:
        # Request was successful
    
        json_data = json.loads(response.text)["response"]

    else:
        # Request encountered an error
        print(f"API request failed with status code {response.status_code}")
    
    records.insert_one(json_data)

#-------------Timed task-----------#    
schedule.every(1).hour.do(Bus_data)

count = 0
while count<24*5:
    schedule.run_pending()
    time.sleep(3600)
    print(count)
    count+=1
    





