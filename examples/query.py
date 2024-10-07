from dotenv import load_dotenv
import os
import json
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from rdflib import Graph, plugin
from rdflib.serializer import Serializer

# Load environment variables
load_dotenv()

node_hostname = os.getenv('NODE_HOSTNAME')
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')
private_key = os.getenv('PRIVATE_KEY')


node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
blockchain_provider = BlockchainProvider(
    "testnet",
    "base",
    rpc_uri=rpc_uri,
    private_key=private_key,
)

dkg = DKG(node_provider, blockchain_provider)

# Helper function to pretty-print JSON data
def print_json(json_dict: dict):
    print(json.dumps(json_dict, indent=4))

# Step 1: Check node info to ensure connection
info_result = dkg.node.info
print("======================== NODE INFO RECEIVED")
print_json(info_result)

# Asset UAL
#ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/54085"
#ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/245266"
# ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/261920"
#ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/262536"
#ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/263670"


# other ual
#ual="did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/249055"
# Query latest finalized state
try:
    get_asset_result = dkg.asset.get(ual, state="latest_finalized", content_visibility="public")
    print("======================== ASSET DATA RECEIVED")
    print_json(get_asset_result)
except Exception as e:
    print(f"Error retrieving the asset: {e}")

# # Extract public assertion data
# public_assertion_data = get_asset_result.get('public', {}).get('assertion')
#
# if public_assertion_data:
#     # Convert Turtle (n-quads) to JSON-LD using rdflib
#     def convert_turtle_to_jsonld(turtle_data):
#         g = Graph()
#         g.parse(data=turtle_data, format='nquads')
#
#         # Serialize the graph to JSON-LD
#         jsonld_data = g.serialize(format='json-ld', indent=4)
#         return jsonld_data
#
#     jsonld_output = convert_turtle_to_jsonld(public_assertion_data)
#     print("======================== ASSET IN JSON-LD FORMAT")
#     print(jsonld_output)
# else:
#     print("No public assertion data available.")
