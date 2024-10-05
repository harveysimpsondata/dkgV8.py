import os
import requests
import json
from dotenv import load_dotenv
#from marta.api import get_buses, get_trains


load_dotenv()

marta_api = os.getenv('MARTA')
url = "https://developerservices.itsmarta.com:18096/itsmarta/railrealtimearrivals/traindata"

params = {
    'apiKey': marta_api,
}

r = requests.get(url, params=params)

if r.status_code == 200:
    data = r.json()

    # Create JSON-LD structure
    json_ld_data = {
        "@context": {
            "@vocab": "http://schema.org/",
            "DESTINATION": "http://schema.org/destination",
            "DIRECTION": "http://schema.org/direction",
            "EVENT_TIME": "http://schema.org/eventTime",
            "IS_REALTIME": "http://schema.org/isRealtime",
            "LINE": "http://schema.org/line",
            "NEXT_ARR": "http://schema.org/nextArrival",
            "STATION": "http://schema.org/trainStation",
            "TRAIN_ID": "http://schema.org/trainID"
        },
        "@graph": []
    }

    # Iterate over the response data to format each train event
    for train in data:
        train_event = {
            "@type": "TrainEvent",
            "DESTINATION": train.get('DESTINATION'),
            "DIRECTION": train.get('DIRECTION'),
            "EVENT_TIME": train.get('EVENT_TIME'),
            "IS_REALTIME": train.get('IS_REALTIME'),
            "LINE": train.get('LINE'),
            "NEXT_ARR": train.get('NEXT_ARR'),
            "STATION": train.get('STATION'),
            "TRAIN_ID": train.get('TRAIN_ID')
        }
        json_ld_data["@graph"].append(train_event)

    # Convert to JSON-LD format and print (or you can write it to a file)
    json_ld_str = json.dumps(json_ld_data, indent=2)
    print(json_ld_str)


else:
    print(f"Failed to get data: {r.status_code}")