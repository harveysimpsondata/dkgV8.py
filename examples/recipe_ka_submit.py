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
# NODE_1_HOSTNAME = ""
# NODE_1_PORT = os.getenv('NODE_PORT')
# PRIVATE_KEY_OP_1 = os.getenv('PRIVATE_KEY_V01')

# NODE_1_PROVIDER = NodeHTTPProvider(f"http://{NODE_1_HOSTNAME}:{NODE_1_PORT}")
# BLOCKCHAIN_1_PROVIDER = BlockchainProvider(
#     "testnet",
#     "base", 
#     rpc_uri=RPC_URI,
#     private_key=PRIVATE_KEY_OP_1,
# )

# NODE 2 RASPBERRY PI
NODE_2_HOSTNAME = ""
NODE_2_PORT = os.getenv('NODE_PORT')
PRIVATE_KEY_OP_2 = os.getenv('PRIVATE_KEY_V01')


NODE_2_PROVIDER = NodeHTTPProvider(f"http://{NODE_2_HOSTNAME}:{NODE_2_PORT}")
BLOCKCHAIN_2_PROVIDER = BlockchainProvider(
    "testnet",
    "base",
    rpc_uri=RPC_URI,
    private_key=PRIVATE_KEY_OP_2,
)

### Get Node 2 info
dkg_2 = DKG(NODE_2_PROVIDER, BLOCKCHAIN_2_PROVIDER)
info_result_1 = dkg_2.node.info
print_json(info_result_1)

node_2_identity_id = dkg_2.node.get_identity_id(dkg_2.blockchain_provider.account.address)
public_address_2 = dkg_2.blockchain_provider.account.address
print(f"Node 2 Identity ID: {node_2_identity_id}, Node 2 Public Address: {public_address_2}")

recipe_file_path = "examples/recipes/1-2-3-cherry-poke-cake.json"

# Read the recipe JSON file
with open(recipe_file_path, 'r') as file:
    recipe_content = json.load(file)

# Create the knowledge asset on node 1

recipe_content = {
    "private": recipe_content
}

#print_json(recipe_content)

# def local_store(
#         self,
#         content: dict[Literal["public", "private"], JSONLD],
#         epochs_number: int,
#         token_amount: Wei | None = None,
#         immutable: bool = False,
#         content_type: Literal["JSON-LD", "N-Quads"] = "JSON-LD",
#         paranet_ual: UAL | None = None,
#     ) -> dict[str, UAL | HexStr | dict[str, dict[str, str] | TxReceipt]]:

paranet_ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/6737441"

local_store_first_asset_result = dkg_2.asset.local_store(
    content=recipe_content,
    epochs_number=1,
    paranet_ual=paranet_ual,
)

if local_store_first_asset_result is not None:
    print("Successfully stored the recipe as a knowledge asset")
    print_json(local_store_first_asset_result)
else:
    print("Failed to store the recipe as a knowledge asset")




