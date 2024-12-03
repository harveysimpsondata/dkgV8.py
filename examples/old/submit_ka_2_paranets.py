import os
import random
import json
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dotenv import load_dotenv
import uuid
import time
from web3 import Web3

# Load environment variables
load_dotenv()
node_hostname = "192.168.1.36"
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')
private_key = os.getenv('PRIVATE_KEY_1')
web3 = Web3(Web3.HTTPProvider(rpc_uri))


def generate_unique_id():
    return str(uuid.uuid4())


def create_json_ld():
    return {
        "@context": "http://schema.org/",
        "@type": "Person",
        "identifier": generate_unique_id(),
        "name": f"John Doe {random.randint(1, 1000)}",
        "email": f"john.doe{random.randint(1, 1000)}@example.com"
    }


# Function to create a KA
def create_ka(dkg, json_ld_data):
    try:
        result = dkg.asset.create({"public": json_ld_data}, 1)
        result = result.get("UAL")
        print(f"KA created with UAL: {result}")
        return result
    except Exception as e:
        print(f"Error creating KA: {e}")
        return None


# Function to turn a KA into a paranet
def create_paranet(dkg, ka_ual):
    try:
        result = dkg.paranet.create(
            ka_ual, "ExampleParanet1", "Description"
        )
        #result = result.get("UAL")
        print(f"Paranet created with UAL: {result}")
        return result
    except Exception as e:
        print(f"Error creating paranet: {e}")
        return None


# Function to submit a KA to a paranet
def submit_ka_to_paranet(dkg, ka_ual, paranet_ual):
    try:
        result = dkg.asset.submit_to_paranet(ka_ual, paranet_ual)
        print(f"KA {ka_ual} submitted to paranet {paranet_ual}")
        return result
    except Exception as e:
        print(f"Error submitting KA to paranet: {e}")
        return None


# Main execution
if __name__ == '__main__':
    node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
    blockchain_provider = BlockchainProvider("testnet", "base", rpc_uri=rpc_uri, private_key=private_key)
    dkg = DKG(node_provider, blockchain_provider)

    # Step 1: Create KA_1 and turn it into paranet_1
    # ka_1_data = create_json_ld()
    # ka_1_ual = create_ka(dkg, ka_1_data)
    paranet_1_ual = create_paranet(dkg, "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/6262907")
    #
    # # Step 2: Create KA_2 and turn it into paranet_2
    # ka_2_data = create_json_ld()
    # ka_2_ual = create_ka(dkg, ka_2_data)
    # paranet_2_ual = create_paranet(dkg, ka_2_ual)
    #
    # # Step 3: Create KA_3
    # ka_3_data = create_json_ld()
    # ka_3_ual = create_ka(dkg, ka_3_data)
    #
    # # Step 4: Submit KA_3 to both paranets
    # if ka_3_ual:
    #     submit_ka_to_paranet(dkg, ka_3_ual, paranet_1_ual)
    #     submit_ka_to_paranet(dkg, ka_3_ual, paranet_2_ual)

    print("Process complete.")
