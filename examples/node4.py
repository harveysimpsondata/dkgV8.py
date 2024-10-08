import os
import requests
import json
from dotenv import load_dotenv
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
import time
import uuid

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
    params = {'apiKey': marta_api}

    try:
        r = requests.get(url, params=params, timeout=10)  # Add a timeout to the request
        r.raise_for_status()  # Raise an exception if the status code isn't 200
    except requests.RequestException as e:
        print(f"Error fetching MARTA data: {e}")
        return

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

    print(json.dumps(json_ld_data, indent=2))

    node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
    blockchain_provider = BlockchainProvider("testnet", "base", rpc_uri=rpc_uri, private_key=private_key)

    dkg = DKG(node_provider, blockchain_provider)

    # Step 2: Format the asset (assertion)
    formatted_assertions = dkg.assertion.format_graph({"public": json_ld_data})
    print("======================== ASSET FORMATTED")
    print(json.dumps(formatted_assertions, indent=4))

    # Step 3: Calculate the Merkle root (public assertion ID)
    public_assertion_id = dkg.assertion.get_public_assertion_id({"public": json_ld_data})
    print("======================== PUBLIC ASSERTION ID (MERKLE ROOT) CALCULATED")
    print(public_assertion_id)

    # Step 4: Get the bid suggestion for the asset
    public_assertion_size = dkg.assertion.get_size({"public": json_ld_data})
    bid_suggestion = dkg.network.get_bid_suggestion(public_assertion_id, public_assertion_size, 1)

    if not bid_suggestion or bid_suggestion <= 0:
        print("Invalid bid suggestion. Skipping asset creation.")
        return

    print("======================== BID SUGGESTION CALCULATED")
    print(json.dumps(bid_suggestion, indent=4))

    try:
        # Increase allowance without checking the current allowance
        print(f"Increasing allowance by {bid_suggestion}...")
        allowance_increase = dkg.asset.increase_allowance(bid_suggestion)
        print("======================== ALLOWANCE INCREASED")
        print(allowance_increase)

        # Create the asset
        create_asset_result = dkg.asset.create({"public": json_ld_data}, 2)
        print("======================== ASSET CREATED")
        print(json.dumps(create_asset_result, indent=4))

    except Exception as e:
        print(f"Error creating asset: {e}")
        return

    if create_asset_result and create_asset_result.get("UAL"):
        validate_ual = dkg.asset.is_valid_ual(create_asset_result["UAL"])
        print(f"Is {create_asset_result['UAL']} a valid UAL: {validate_ual}")


# Run the function once
try:
    while True:
        fetch_and_upload_marta_data()
        print("Waiting for 30 secs before next run...")
        time.sleep(30)  # 2 minutes
except KeyboardInterrupt:
    print("Process interrupted. Exiting.")
