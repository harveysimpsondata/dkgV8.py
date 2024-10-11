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
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from urllib3 import Retry
from hexbytes import HexBytes

# Load environment variables (assuming you have .env with blockchain details)
load_dotenv()

# Load environment variables
node_hostname = os.getenv('NODE_HOSTNAME')
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')

# Load private keys (assign more private keys as needed)
private_keys = [
    os.getenv('PRIVATE_KEY_1'),
    os.getenv('PRIVATE_KEY_2'),
    os.getenv('PRIVATE_KEY_3'),
    os.getenv('PRIVATE_KEY_4'),
    os.getenv('PRIVATE_KEY_5'),
    os.getenv('PRIVATE_KEY_6'),
    os.getenv('PRIVATE_KEY_7'),
    os.getenv('PRIVATE_KEY_8'),
    os.getenv('PRIVATE_KEY_9'),
    os.getenv('PRIVATE_KEY_10'),
    os.getenv('PRIVATE_KEY_11'),
    os.getenv('PRIVATE_KEY_12'),
    os.getenv('PRIVATE_KEY_13'),
    os.getenv('PRIVATE_KEY_14'),
    os.getenv('PRIVATE_KEY_15'),
    os.getenv('PRIVATE_KEY_16'),
    os.getenv('PRIVATE_KEY_17'),
    os.getenv('PRIVATE_KEY_18'),
    os.getenv('PRIVATE_KEY_19'),
    os.getenv('PRIVATE_KEY_20')
]

# Public keys for curated miners
PUBLIC_KEY3 = '0x90F79bf6EB2c4f870365E785982E1f101E93b906'
PUBLIC_KEY4 = '0xe5beaB7853A22f054Ef287EA62aCe7A32528b3eE'
PUBLIC_KEY5 = '0x8A4673B00B04b59CaC44926ABeDa85ed181fA436'

class NODES_ACCESS_POLICY:
    OPEN = 0
    CURATED = 1

class MINERS_ACCESS_POLICY:
    OPEN = 0
    CURATED = 1

# Create a persistent connection to the node
class PersistentNodeHTTPProvider(NodeHTTPProvider):
    def __init__(self, node_url):
        super().__init__(node_url)
        self.session = Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def post(self, endpoint, data):
        url = f"{self.url}/{endpoint}"
        response = self.session.post(url, json=data)
        return response


# Function to print JSON in a formatted way
def print_json(json_dict: dict):
    def convert_hexbytes(data):
        if isinstance(data, dict):
            return {k: convert_hexbytes(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [convert_hexbytes(i) for i in data]
        elif isinstance(data, HexBytes):
            return data.hex()
        else:
            return data

    serializable_dict = convert_hexbytes(json_dict)
    print(json.dumps(serializable_dict, indent=4))


# Function to load CSV files into a single DataFrame
def load_csv_files(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    dataframes = [pd.read_csv(os.path.join(folder_path, csv_file)) for csv_file in csv_files]
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df

# Function to generate a unique ID based on selected values
def generate_unique_id(first_name, last_name, email, gender, ip_address):
    random_string = f"{first_name}_{last_name}_{email}_{gender}_{ip_address}"
    unique_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, random_string))  # Generate UUID5 based on DNS namespace
    return unique_id

# Function to randomly select values from each column
def generate_random_record(df):
    first_name = random.choice(df['first_name'].dropna().tolist())
    last_name = random.choice(df['last_name'].dropna().tolist())
    email = random.choice(df['email'].dropna().tolist())
    gender = random.choice(df['gender'].dropna().tolist())
    ip_address = random.choice(df['ip_address'].dropna().tolist())

    return {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "gender": gender,
        "ip_address": ip_address
    }

# Function to create JSON-LD data for a knowledge asset
def create_json_ld(record):
    json_ld_data = {
        "@context": {
            "@vocab": "http://schema.org/",
            "id": "http://schema.org/identifier",
            "first_name": "http://schema.org/givenName",
            "last_name": "http://schema.org/familyName",
            "email": "http://schema.org/email",
            "gender": "http://schema.org/gender",
            "ip_address": "http://schema.org/IPAddress"
        },
        "@graph": [{
            "@type": "Person",
            "id": str(record['id']),
            "first_name": record['first_name'],
            "last_name": record['last_name'],
            "email": record['email'],
            "gender": record['gender'],
            "ip_address": record['ip_address']
        }]
    }
    return json_ld_data

# Function to set the allowance before creating multiple assets
def set_allowance(dkg, allowance_value):
    try:
        dkg.asset.set_allowance(allowance_value)
        print(f"======================== ALLOWANCE SET TO {allowance_value}")
    except Exception as e:
        print(f"Error setting allowance: {e}")


# Function to create and manage the Paranet
def create_paranet_from_csv(df, private_key, allowance_value):
    try:
        # Create the DKG instance with the provided private key
        node_provider = PersistentNodeHTTPProvider(f"http://{node_hostname}:{node_port}")
        blockchain_provider = BlockchainProvider(
            "testnet", "base", rpc_uri=rpc_uri, private_key=private_key
        )
        dkg = DKG(node_provider, blockchain_provider)

        # Set the allowance
        set_allowance(dkg, allowance_value)

        # Generate random knowledge asset data for Paranet
        random_record = generate_random_record(df)
        random_record['id'] = generate_unique_id(
            random_record['first_name'],
            random_record['last_name'],
            random_record['email'],
            random_record['gender'],
            random_record['ip_address']
        )
        json_ld_data = create_json_ld(random_record)

        # Create the Knowledge Asset for the Paranet
        create_asset_result = dkg.asset.create({"public": json_ld_data}, 1)
        print('************************ KNOWLEDGE ASSET FOR PARANET CREATED')
        ka_ual = create_asset_result.get("UAL")

        # Create the Paranet using the Knowledge Asset's UAL
        if ka_ual:
            create_paranet_result = dkg.paranet.create(
                ka_ual, "ExampleParanet", "Description of the Paranet", NODES_ACCESS_POLICY.CURATED, MINERS_ACCESS_POLICY.CURATED
            )
            print("======================== PARANET CREATED")
            paranet_ual = create_paranet_result["UAL"]

            # Optionally create and link a service to the Paranet
            paranet_service_data = {
                "public": {
                    "@context": ["http://schema.org"],
                    "@id": str(uuid.uuid4()),
                    "service": "Example Service",
                    "model": {"@id": "uuid:model_1"},
                }
            }
            service_asset_result = dkg.asset.create(paranet_service_data, 1)
            paranet_service_ual = service_asset_result.get("UAL")

            if paranet_service_ual:
                create_paranet_service_result = dkg.paranet.create_service(
                    paranet_service_ual,
                    "ExampleParanetService",
                    "Description of the service",
                    ["0x03C094044301E082468876634F0b209E11d98452"]
                )
                print("======================== PARANET SERVICE CREATED")
                dkg.paranet.add_services(paranet_ual, [paranet_service_ual])
                print("======================== SERVICE LINKED TO PARANET")

            return paranet_ual  # Return the Paranet UAL for submitting Knowledge Assets

    except Exception as e:
        print(f"Error creating Paranet: {e}")
        return None


# Function to create Knowledge Assets and submit them to an existing Paranet
def create_and_submit_knowledge_assets_to_paranet(paranet_ual, num_assets, df, private_key, allowance_value):
    try:
        node_provider = PersistentNodeHTTPProvider(f"http://{node_hostname}:{node_port}")
        blockchain_provider = BlockchainProvider(
            "testnet", "base", rpc_uri=rpc_uri, private_key=private_key
        )
        dkg = DKG(node_provider, blockchain_provider)

        # Set the allowance
        set_allowance(dkg, allowance_value)

        for _ in range(num_assets):
            # Generate random knowledge asset data
            random_record = generate_random_record(df)
            random_record['id'] = generate_unique_id(
                random_record['first_name'],
                random_record['last_name'],
                random_record['email'],
                random_record['gender'],
                random_record['ip_address']
            )
            json_ld_data = create_json_ld(random_record)

            # Create the Knowledge Asset
            create_asset_result = dkg.asset.create({"public": json_ld_data}, 1)
            print('************************ KNOWLEDGE ASSET CREATED')
            ka_ual = create_asset_result.get("UAL")

            # Submit the Knowledge Asset to the Paranet
            if ka_ual and paranet_ual:
                submit_to_paranet_result = dkg.asset.submit_to_paranet(ka_ual, paranet_ual)
                print(f"Knowledge Asset {ka_ual} submitted to Paranet {paranet_ual}")
                print_json(submit_to_paranet_result)

    except Exception as e:
        print(f"Error submitting assets to Paranet: {e}")


# Main execution
if __name__ == '__main__':
    # Path to the folder containing the CSV files
    folder_path = '../mock_data'

    # Load all CSV files into a single DataFrame
    df = load_csv_files(folder_path)

    num_ka_threads = 3  # Number of threads for Knowledge Assets
    num_paranet_threads = 1  # One thread for Paranet

    # Set up threading for Paranet and Knowledge Assets submission
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_ka_threads + num_paranet_threads) as executor:
        # Thread for Paranet creation
        paranet_future = executor.submit(create_paranet_from_csv, df, private_keys[0], 1000000000000000000)

        # Once the Paranet is created, submit Knowledge Assets
        paranet_ual = paranet_future.result()
        if paranet_ual:
            # Threads for submitting Knowledge Assets to the Paranet
            futures = []
            for i in range(num_ka_threads):
                futures.append(executor.submit(create_and_submit_knowledge_assets_to_paranet, paranet_ual, 10, df, private_keys[i + 1], 1000000000000000000))

            # Wait for all Knowledge Asset submissions to complete
            for future in concurrent.futures.as_completed(futures):
                future.result()
