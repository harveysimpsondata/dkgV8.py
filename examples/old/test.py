import os
import requests
import json
import schedule
import time
from dotenv import load_dotenv
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider

# Load environment variables
load_dotenv()

# load environments
marta_api = os.getenv('MARTA')
node_hostname = os.getenv('NODE_HOSTNAME')
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')
private_key = os.getenv('PRIVATE_KEY')

def fetch_and_upload_marta_data():

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
        for train in data[:1]:
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


        # Step 1: Print the JSON-LD format of the data
        json_ld_str = json.dumps(json_ld_data, indent=2)
        print(json_ld_str)

        # DKG Integration (up to Step 4)

        node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
        blockchain_provider = BlockchainProvider(
            "testnet",
            "base",
            rpc_uri=rpc_uri,
            private_key=private_key,
        )

        dkg = DKG(node_provider, blockchain_provider)


        # Step 2: Format the asset (assertion) for the DKG
        formatted_assertions = dkg.assertion.format_graph({"public": json_ld_data})
        print("======================== ASSET FORMATTED")
        print(json.dumps(formatted_assertions, indent=4))


        # Step 3: Calculate the Merkle root (public assertion ID)
        public_assertion_id = dkg.assertion.get_public_assertion_id({"public": json_ld_data})
        print("======================== PUBLIC ASSERTION ID (MERKLE ROOT) CALCULATED")
        print(public_assertion_id)


        # Step 4: Get the bid suggestion for the asset
        public_assertion_size = dkg.assertion.get_size({"public": json_ld_data})
        bid_suggestion = dkg.network.get_bid_suggestion(
            public_assertion_id,
            public_assertion_size,
            2  # Replication factor
        )
        print("======================== BID SUGGESTION CALCULATED")
        print(json.dumps(bid_suggestion, indent=4))



        # Step 5: Check current allowance
        try:
            current_allowance = dkg.asset.get_current_allowance()
            print("======================== CURRENT ALLOWANCE")
            print(current_allowance)


            # Step 6: Check if current allowance is less than bid suggestion
            if current_allowance < bid_suggestion:
                print(
                    f"Current allowance {current_allowance} is less than bid suggestion {bid_suggestion}. Increasing allowance...")
                allowance_increase = dkg.asset.increase_allowance(bid_suggestion)
                print("======================== ALLOWANCE INCREASED")
                print(allowance_increase)
            else:
                print(f"Current allowance {current_allowance} is sufficient for the bid suggestion {bid_suggestion}.")
        except Exception as e:
            print(f"Error fetching current allowance: {e}")


        # # Step 7: Create the asset on the OriginTrail network
        # try:
        #     create_asset_result = dkg.asset.create({"public": json_ld_data}, 2)  # Replication factor of 2
        #     print("======================== ASSET CREATED")
        #     print(json.dumps(create_asset_result, indent=4))
        # except Exception as e:
        #     print(f"Error creating asset: {e}")
        #
        #
        #
        #
        #
        # # Step 8: Query the asset from the network to verify creation
        # if create_asset_result and create_asset_result.get("UAL"):
        #
        #     validate_ual = dkg.asset.is_valid_ual(create_asset_result["UAL"])
        #     print(f"Is {create_asset_result['UAL']} a valid UAL: {validate_ual}")
        #
        #     try:
        #         get_asset_result = dkg.asset.get(create_asset_result["UAL"], state="latest_finalized")
        #         print("======================== ASSET RESOLVED")
        #         print(json.dumps(get_asset_result, indent=4))
        #     except Exception as e:
        #         print(f"Error retrieving the asset: {e}")

            # Step 9: Query private data
            # try:
            #     get_private_asset_result = dkg.asset.get(create_asset_result["UAL"], content_visibility="private", state="latest_finalized")
            #     print("======================== PRIVATE ASSET RESOLVED")
            #     print(json.dumps(get_private_asset_result, indent=4))
            # except Exception as e:
            #     print(f"Error retrieving the private asset: {e}")

    else:
        print(f"Failed to get data: {r.status_code}")


fetch_and_upload_marta_data()

# # Schedule the function to run every 8 minutes
# schedule.every(10).minutes.do(fetch_and_upload_marta_data)
#
# # Keep the script running
# try:
#     while True:
#         schedule.run_pending()
#         time.sleep(2)
# except KeyboardInterrupt:
#     print("Process interrupted. Exiting gracefully.")
