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
node_hostname = os.getenv("NODE_HOSTNAME")
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
    os.getenv('PRIVATE_KEY_10')
]

# Create a persistent connection to the node
class PersistentNodeHTTPProvider(NodeHTTPProvider):
    def __init__(self, node_url):
        super().__init__(node_url)
        self.session = Session()
        self.setup_http_adapter()

    def setup_http_adapter(self):
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=5, pool_maxsize=5)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def reconnect(self):
        """Reconnect by setting up a new session and HTTP adapter."""
        print("Reconnecting the HTTP session...")
        self.session.close()  # Close the current session
        self.session = Session()  # Reinitialize the session
        self.setup_http_adapter()  # Setup the HTTP adapter again

    def post(self, endpoint, data):
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
    unique_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, random_string))
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


# Global cache for allowances
allowance_cache = {}


# Function to ensure allowance is set
def ensure_allowance(dkg, private_key, required_allowance):
    try:
        if private_key in allowance_cache and allowance_cache[private_key] >= required_allowance:
            print(f"Cached allowance is sufficient: {allowance_cache[private_key]}")
            return

        current_allowance = dkg.asset.get_current_allowance()
        allowance_cache[private_key] = current_allowance

        if current_allowance < required_allowance:
            set_allowance(dkg, private_key, required_allowance)
        else:
            print(f"Allowance is already sufficient: {current_allowance}")
    except Exception as e:
        print(f"Error checking or setting allowance: {e}")


# Function to set the allowance
def set_allowance(dkg, private_key, allowance_value):
    try:
        dkg.asset.set_allowance(allowance_value)
        allowance_cache[private_key] = allowance_value
        print(f"======================== ALLOWANCE SET TO {allowance_value} for {private_key}")
    except Exception as e:
        print(f"Error setting allowance: {e}")


# Function to upload knowledge assets using the DKG with allowance management
def upload_knowledge_asset_with_increase(json_ld_data, private_key, allowance_value, node_provider):
    try:
        blockchain_provider = BlockchainProvider(
            "testnet",
            "base",
            rpc_uri=rpc_uri,
            private_key=private_key,
        )
        dkg = DKG(node_provider, blockchain_provider)

        ensure_allowance(dkg, private_key, allowance_value)

        formatted_assertions = dkg.assertion.format_graph({"public": json_ld_data})
        print("======================== ASSET FORMATTED")

        time.sleep(3)  # Add delay before publishing to avoid timeout issues
        create_asset_result = dkg.asset.create({"public": json_ld_data}, 1)
        print('************************ ASSET CREATED')

        if create_asset_result and create_asset_result.get("UAL"):
            validate_ual = dkg.asset.is_valid_ual(create_asset_result["UAL"])
            print(f"Is {create_asset_result['UAL']} a valid UAL: {validate_ual}")

    except Exception as e:
        print(f"Error creating asset: {e}")
        return False  # Return False if there's an error

    return True  # Return True if asset was created successfully


# Main execution
if __name__ == '__main__':
    folder_path = '/Users/leesimpson/mock_data'
    df = load_csv_files(folder_path)

    num_threads = 7  # Number of workers
    allowance_value = 1000000000000000000
    reconnect_threshold = 3  # Number of consecutive failure iterations before reconnecting
    iterations = 0
    consecutive_failure_iterations = 0  # Track consecutive failure iterations
    node_provider = PersistentNodeHTTPProvider(f"http://{node_hostname}:{node_port}")

    def assign_private_key_to_thread(thread_id, df, node_provider):
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
        success = upload_knowledge_asset_with_increase(json_ld_data, private_key, allowance_value, node_provider)
        return success

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        while True:
            futures = []
            iterations += 1
            iteration_failed = False  # Track whether this iteration failed

            # Submit tasks and check errors
            for thread_id in range(num_threads):
                futures.append(executor.submit(assign_private_key_to_thread, thread_id, df, node_provider))
                time.sleep(5)  # You can adjust this sleep time based on your setup

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if not result:
                        iteration_failed = True  # Mark this iteration as failed if any task fails
                except Exception as exc:
                    print(f"Generated an exception: {exc}")
                    iteration_failed = True  # Mark this iteration as failed

            # If all workers encountered errors in this iteration, track the failure
            if iteration_failed:
                consecutive_failure_iterations += 1
                print(f"Iteration {iterations} failed. Consecutive failures: {consecutive_failure_iterations}")

                # If failures have occurred for reconnect_threshold iterations, reconnect
                if consecutive_failure_iterations >= reconnect_threshold:
                    print(f"Consecutive failure threshold reached. Reconnecting after {iterations} iterations...")
                    node_provider.reconnect()
                    consecutive_failure_iterations = 0  # Reset consecutive failure counter
            else:
                # Reset failure counter if any iteration succeeds
                consecutive_failure_iterations = 0

            print(f"Iteration {iterations} completed. Waiting 2 seconds before running the next batch...")
            time.sleep(2)
