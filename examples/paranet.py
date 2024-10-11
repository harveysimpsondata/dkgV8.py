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

# Create a persistent connection to the node
class PersistentNodeHTTPProvider(NodeHTTPProvider):
    def __init__(self, node_url):
        # Call the parent constructor
        super().__init__(node_url)

        # Create a persistent session
        self.session = Session()

        # Configure the session to retry and use keep-alive
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)

        # Mount the adapter for http(s)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def post(self, endpoint, data):
        # Use the session to make a POST request
        url = f"{self.url}/{endpoint}"
        response = self.session.post(url, json=data)
        return response

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

# Function to upload knowledge assets using the DKG with allowance management
def upload_knowledge_asset_with_increase(json_ld_data, private_key, allowance_value):
    try:
        # Create the DKG instance with the provided private key
        node_provider = PersistentNodeHTTPProvider(f"http://{node_hostname}:{node_port}")
        blockchain_provider = BlockchainProvider(
            "testnet",
            "base",
            rpc_uri=rpc_uri,
            private_key=private_key,
        )
        dkg = DKG(node_provider, blockchain_provider)

        # Set the allowance (this will speed up subsequent asset publishing)
        set_allowance(dkg, allowance_value)

        # Format the knowledge asset for the DKG
        formatted_assertions = dkg.assertion.format_graph({"public": json_ld_data})
        print("======================== ASSET FORMATTED")

        # Create the asset after setting allowance
        create_asset_result = dkg.asset.create({"public": json_ld_data}, 1)
        print('************************ ASSET CREATED')

        # Optionally create a Paranet after creating the asset
        paranet_ual = create_asset_result.get("UAL")
        if paranet_ual:
            create_paranet_result = dkg.paranet.create(
                paranet_ual, "ExampleParanet", "Description of the Paranet"
            )
            print("======================== PARANET CREATED")
            print(create_paranet_result)

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
                dkg.paranet.add_services(paranet_ual, [paranet_service_ual])
                print("======================== SERVICE LINKED TO PARANET")

        if create_asset_result and create_asset_result.get("UAL"):
            validate_ual = dkg.asset.is_valid_ual(create_asset_result["UAL"])
            print(f"Is {create_asset_result['UAL']} a valid UAL: {validate_ual}")

    except Exception as e:
        print(f"Error creating asset: {e}")

# Main execution
if __name__ == '__main__':
    # Path to the folder containing the CSV files
    folder_path = '../mock_data'

    # Load all CSV files into a single DataFrame
    df = load_csv_files(folder_path)

    # Set up threading for multithreaded execution
    num_threads = 4  # Specify 4 threads explicitly
    allowance_value = 1000000000000000000  # Set a large allowance to speed up publishing

    # Assign one private key to each thread
    def assign_private_key_to_thread(thread_id, df):
        private_key = private_keys[thread_id % len(private_keys)]
        random_record = generate_random_record(df)
        random_record['id'] = generate_unique_id(
            random_record['first_name'],
            random_record['last_name'],
            random_record['email'],
            random_record['gender'],
            random_record['ip_address']
        )
        json_ld_data = create_json_ld(random_record)
        upload_knowledge_asset_with_increase(json_ld_data, private_key, allowance_value)

    # Thread pool executor for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        while True:
            futures = []
            # Submit a task for each thread, assigning a unique private key
            for thread_id in range(num_threads):
                futures.append(executor.submit(assign_private_key_to_thread, thread_id, df))

            # Ensure all threads complete their tasks
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    print(f"Generated an exception: {exc}")

            # Wait for 0.01 second before running the next batch
            print("Waiting 0.01 second before running the next batch...")
            time.sleep(0.01)
