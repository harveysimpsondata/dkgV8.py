import json
import math
import random
import time
from dotenv import load_dotenv
import os
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider

# Load environment variables
load_dotenv()

node_hostname = os.getenv('NODE_HOSTNAME')
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')
private_key = os.getenv('PRIVATE_KEY')

# Create node and blockchain providers
node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
blockchain_provider = BlockchainProvider(
    "testnet",
    "base",
    rpc_uri=rpc_uri,
    private_key=private_key,
)

dkg = DKG(node_provider, blockchain_provider)


# Helper function to print dividers
def divider():
    print("==================================================")
    print("==================================================")
    print("==================================================")


# Helper function to pretty-print JSON data
def print_json(json_dict: dict):
    print(json.dumps(json_dict, indent=4))


# Step 1: Check node info to ensure connection
info_result = dkg.node.info
print("======================== NODE INFO RECEIVED")
print_json(info_result)
divider()

# Step 2: Define the asset (content) you want to publish
content = {
    "public": {
        "@context": ["http://schema.org"],
        "@id": "uuid:1",
        "company": "BAH",
        "user": {"@id": "uuid:user:1"},
        "city": {"@id": "uuid:atlanta"},
    },
    "private": {
        "@context": ["http://schema.org"],
        "@graph": [
            {"@id": "uuid:user:1", "name": "lee", "lastname": "simpson"},
            {"@id": "uuid:atlanta", "title": "atlanta", "postCode": "30390"},
        ],
    },
}

# Step 3: Format the asset (assertion) for the network
formatted_assertions = dkg.assertion.format_graph(content)
print("======================== ASSET FORMATTED")
print_json(formatted_assertions)
divider()

# Step 4: Calculate the Merkle root (public assertion ID)
public_assertion_id = dkg.assertion.get_public_assertion_id(content)
print("======================== PUBLIC ASSERTION ID (MERKLE ROOT) CALCULATED")
print(public_assertion_id)
divider()

# Step 5: Get the bid suggestion for the asset
public_assertion_size = dkg.assertion.get_size(content)
bid_suggestion = dkg.network.get_bid_suggestion(
    public_assertion_id,
    public_assertion_size,
    2,  # Replication factor
)
print("======================== BID SUGGESTION CALCULATED")
print(bid_suggestion)
divider()

# Step 6: Increase allowance for the asset creation
try:
    allowance_increase = dkg.asset.increase_allowance(bid_suggestion)
    print("======================== INCREASE ALLOWANCE")
    print(allowance_increase)
except Exception as e:
    print(f"Error increasing allowance: {e}")
divider()

# Step 7: Create the asset on the OriginTrail network
try:
    create_asset_result = dkg.asset.create(content, 2)  # Replication factor of 2
    print("======================== ASSET CREATED")
    print_json(create_asset_result)
except Exception as e:
    print(f"Error creating asset: {e}")
divider()

# Step 8: Validate the UAL of the created asset
if create_asset_result and create_asset_result.get("UAL"):
    validate_ual = dkg.asset.is_valid_ual(create_asset_result["UAL"])
    print(f"Is {create_asset_result['UAL']} a valid UAL: {validate_ual}")
    divider()

    # Step 9: Query the asset from the network to verify creation
    try:
        get_asset_result = dkg.asset.get(create_asset_result["UAL"])
        print("======================== ASSET RESOLVED")
        print_json(get_asset_result)
    except Exception as e:
        print(f"Error retrieving the asset: {e}")

    # Step 10: Query private data
    try:
        get_private_asset_result = dkg.asset.get(create_asset_result["UAL"], content_visibility="private")
        print("======================== PRIVATE ASSET RESOLVED")
        print_json(get_private_asset_result)
    except Exception as e:
        print(f"Error retrieving the private asset: {e}")

    # Step 11: Decrease allowance (Optional)
    try:
        dkg.asset.decrease_allowance(bid_suggestion)
        print("Allowance successfully decreased.")
    except Exception as e:
        print(f"Error decreasing allowance: {e}")
else:
    print("Asset creation failed or UAL missing.")
