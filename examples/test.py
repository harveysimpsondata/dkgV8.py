import os
import pandas as pd
import random
import json
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dotenv import load_dotenv
import uuid
import time
import concurrent.futures
from web3 import Web3



# Load environment variables (assuming you have .env with blockchain details)
load_dotenv()

# Load environment variables
node_hostname = "192.168.1.36"
node_port = os.getenv('NODE_PORT')
rpc_uri = "https://84532.rpc.thirdweb.com/7121fce4e60c849496e43d5c737c0b35"
private_key = os.getenv('PRIVATE_KEY_12')

node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
blockchain_provider = BlockchainProvider(
    "testnet",
    "base",
    rpc_uri=rpc_uri,
    private_key=private_key,
)
dkg = DKG(node_provider, blockchain_provider)




def divider():
    print("==================================================")
    print("==================================================")
    print("==================================================")


def print_json(json_dict: dict):
    print(json.dumps(json_dict, indent=4))
info_result = dkg.node.info

print("======================== NODE INFO RECEIVED")
print_json(info_result)

paranet_data = {
    "public": {
        "@context": ["http://schema.org"],
        "@id": "uuid:112345",
        "company": "OT",
        "city": {"@id": "uuid:belgrade"},
    }
}

create_paranet_knowledge_asset_result = dkg.asset.create(paranet_data, 1)

print("======================== PARANET KNOWLEDGE ASSET CREATED")
print_json(create_paranet_knowledge_asset_result)

divider()

paranet_ual = create_paranet_knowledge_asset_result["UAL"]
create_paranet_result = dkg.paranet.create(
    paranet_ual,
    "TestParanet",
    "TestParanetDescription",
)

print("======================== PARANET CREATED")
print_json(create_paranet_result)