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

# Load environment variables (assuming you have .env with blockchain details)
load_dotenv()

# Load environment variables
node_hostname = os.getenv('NODE_HOSTNAME')
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')

# Load private keys (two private keys for parallel processing)
private_keys = [
    os.getenv('PRIVATE_KEY_1'),
    os.getenv('PRIVATE_KEY_2'),
    os.getenv('PRIVATE_KEY_3'),
    os.getenv('PRIVATE_KEY_4'),
    os.getenv('PRIVATE_KEY_5'),
    os.getenv('PRIVATE_KEY_6')
]

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


# Function to upload knowledge assets using the DKG with allowance management
# Function to upload knowledge assets using the DKG with allowance management
def upload_knowledge_asset_with_increase(json_ld_data, private_key):
    try:
        # Create the DKG instance with the provided private key
        node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
        blockchain_provider = BlockchainProvider(
            "testnet",
            "base",
            rpc_uri=rpc_uri,
            private_key=private_key,
        )
        dkg = DKG(node_provider, blockchain_provider)

        # Format the knowledge asset for the DKG
        formatted_assertions = dkg.assertion.format_graph({"public": json_ld_data})
        print("======================== ASSET FORMATTED")

        # Calculate the bid suggestion for the asset
        public_assertion_id = dkg.assertion.get_public_assertion_id({"public": json_ld_data})
        public_assertion_size = dkg.assertion.get_size({"public": json_ld_data})
        bid_suggestion = dkg.network.get_bid_suggestion(
            public_assertion_id,
            public_assertion_size,
            1  # Replication factor
        )
        print("======================== BID SUGGESTION CALCULATED")
        print(bid_suggestion)

        # Check current allowance
        current_allowance = dkg.asset.get_current_allowance()

        # Increase allowance only if the current allowance is less than the bid suggestion
        if current_allowance < bid_suggestion:
            print(f"Current allowance ({current_allowance}) is less than bid suggestion ({bid_suggestion}). Increasing allowance...")
            allowance_increase = dkg.asset.increase_allowance(bid_suggestion)
            print("======================== ALLOWANCE INCREASED")
            print(allowance_increase)
        else:
            print(f"Current allowance ({current_allowance}) is sufficient. No increase needed.")

        # Create the asset after increasing allowance (if needed)
        create_asset_result = dkg.asset.create({"public": json_ld_data}, 1)
        print("======================== ASSET CREATED")

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
    num_threads = os.cpu_count()  # Adjust based on available system resources

    # Thread pool executor for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        while True:
            futures = []
            # Create and upload 1000 assets in parallel
            for _ in range(100):  # Create 1000 knowledge assets
                # Generate a random record from the DataFrame
                random_record = generate_random_record(df)

                # Generate a unique ID for the selected record
                random_record['id'] = generate_unique_id(
                    random_record['first_name'],
                    random_record['last_name'],
                    random_record['email'],
                    random_record['gender'],
                    random_record['ip_address']
                )

                # Create JSON-LD data
                json_ld_data = create_json_ld(random_record)

                # Use alternating private keys for parallel uploads
                private_key = private_keys[_ % len(private_keys)]

                # Submit the task to the executor for parallel execution
                futures.append(executor.submit(upload_knowledge_asset_with_increase, json_ld_data, private_key))

            # Ensure all threads complete their tasks
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    print(f"Generated an exception: {exc}")

            # Wait for 60 seconds before running the next batch
            print("Waiting 1 seconds before running the next batch...")
            time.sleep(1)