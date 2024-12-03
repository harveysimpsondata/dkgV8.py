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
node_1_hostname = "192.168.1.37"
node_1_port = os.getenv('NODE_PORT')
rpc_uri = "https://sepolia.base.org"
private_key = os.getenv('PRIVATE_KEY_V01')

node_provider = NodeHTTPProvider(f"http://{node_1_hostname}:{node_1_port}")
blockchain_provider = BlockchainProvider(
    "testnet",
    "base",
    rpc_uri=rpc_uri,
    private_key=private_key,
)

dkg = DKG(node_provider, blockchain_provider)

# "0x6f7c3248022Edd9C81AF5C0fe27e14f548172F2c"
node1_identity_id = dkg.node.get_identity_id(dkg.blockchain_provider.account.address)
public_address = dkg.blockchain_provider.account.address

def divider():
    print("==================================================")
    print("==================================================")
    print("==================================================")


def print_json(json_dict: dict):
    print(json.dumps(json_dict, indent=4))
info_result = dkg.node.info

print("======================== NODE INFO RECEIVED")
print_json(info_result)
print(node1_identity_id)
print(public_address)

