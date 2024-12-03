import os
import pandas as pd
import random
import json
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dkg.dataclasses import ParanetNodesAccessPolicy, ParanetMinersAccessPolicy
from dotenv import load_dotenv
import uuid
import time
import concurrent.futures
from web3 import Web3
from hexbytes import HexBytes

def print_json(json_dict: dict):
    def convert_hexbytes(data):
        if isinstance(data, dict):
            return {k: convert_hexbytes(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [convert_hexbytes(i) for i in data]
        elif isinstance(data, tuple) and hasattr(data, '_asdict'):
            return convert_hexbytes(data._asdict())
        elif isinstance(data, bytes):
            return data.decode("utf-8")
        elif isinstance(data, HexBytes):
            return data.to_0x_hex()
        else:
            return data

    serializable_dict = convert_hexbytes(json_dict)
    print(json.dumps(serializable_dict, indent=4))


####### Load environment variables #######
load_dotenv()
RPC_URI = "https://sepolia.base.org"

# Node 1 NAS
# public ip 75.139.145.238
NODE_1_HOSTNAME = "75.139.145.238"
NODE_1_PORT = os.getenv('NODE_PORT')
PRIVATE_KEY_OP_1 = os.getenv('PRIVATE_KEY_V01')

mangement="13848342483de571dd49234c258267cb7cfd816716071651d5c5b97a2a27636d"
NODE_1_PROVIDER = NodeHTTPProvider(f"http://{NODE_1_HOSTNAME}:{NODE_1_PORT}")
BLOCKCHAIN_1_PROVIDER = BlockchainProvider(
    "testnet",
    "base",
    rpc_uri=RPC_URI,
    private_key=PRIVATE_KEY_OP_1,
)


### Get Node 1 info
dkg = DKG(NODE_1_PROVIDER, BLOCKCHAIN_1_PROVIDER)

info = dkg.node.info
print(info)

# paranet_ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/6737441"
# ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/6970836"
# miner_addresses = [
#
# ]
#
# ##### Miners
# # dkg.paranet.add_curated_miners(paranet_ual, miner_addresses)
# knowledge_miners = dkg.paranet.get_knowledge_miners(paranet_ual)
# print_json(knowledge_miners)


##### Get KA
#dkg_1.asset.local_store
# get_asset_result = dkg.asset.get(ual)
# # dkg.asset
# print_json(get_asset_result)

# Define your SPARQL query
# sparql_query = """
#     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#     SELECT * WHERE {
#       ?sub ?pred ?obj .
#     }
# """
#
#
# query_result = dkg.graph.query(
#     sparql_query,
#     repository=paranet_ual,
# )
#
# print(query_result)















