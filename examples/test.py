import json
import math
import random
import time
from dotenv import load_dotenv
import os
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider

load_dotenv()

node_hostname = os.getenv('NODE_HOSTNAME')
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('RPC_URI')
private_key = os.getenv('PRIVATE_KEY')


node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
blockchain_provider = BlockchainProvider(
    "testnet",
    "base",
    rpc_uri=rpc_uri,
    private_key=private_key,
)

dkg = DKG(node_provider, blockchain_provider)


info_result = dkg.node.info


def print_json(json_dict: dict):
    print(json.dumps(json_dict, indent=4))


print("======================== NODE INFO RECEIVED")
print_json(info_result)
