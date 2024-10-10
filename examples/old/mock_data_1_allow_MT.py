import os
import pandas as pd
import random
import json
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dotenv import load_dotenv
import uuid
import concurrent.futures
import os

# Load environment variables (assuming you have .env with blockchain details)
load_dotenv()

# Load environment variables
node_hostname = os.getenv('NODE_HOSTNAME')
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')
private_key = os.getenv('PRIVATE_KEY')

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

# DKG Integration: Uploading knowledge asset without increasing allowance
def upload_knowledge_asset_without_increase(json_ld_data, dkg):
    try:
        # Format the knowledge asset for the DKG
        formatted_assertions = dkg.assertion.format_graph({"public": json_ld_data})
        print("======================== ASSET FORMATTED")
        print(json.dumps(formatted_assertions, indent=4))

        # Create the asset (no allowance increase)
        create_asset_result = dkg.asset.create({"public": json_ld_data}, 1)
        print("======================== ASSET CREATED")
        print(json.dumps(create_asset_result, indent=4))

        if create_asset_result and create_asset_result.get("UAL"):
            validate_ual = dkg.asset.is_valid_ual(create_asset_result["UAL"])
            print(f"Is {create_asset_result['UAL']} a valid UAL: {validate_ual}")

    except Exception as e:
        print(f"Error creating asset: {e}")

# DKG Integration: Setting allowance one time
def set_allowance(dkg, json_ld_data):
    try:
        # Format the knowledge asset for the DKG
        formatted_assertions = dkg.assertion.format_graph({"public": json_ld_data})
        print("======================== ASSET FORMATTED")
        print(json.dumps(formatted_assertions, indent=4))

        # Calculate the Merkle root (public assertion ID)
        public_assertion_id = dkg.assertion.get_public_assertion_id({"public": json_ld_data})
        print("======================== PUBLIC ASSERTION ID (MERKLE ROOT) CALCULATED")
        print(public_assertion_id)

        # Get the bid suggestion for the asset
        public_assertion_size = dkg.assertion.get_size({"public": json_ld_data})
        bid_suggestion = dkg.network.get_bid_suggestion(
            public_assertion_id,
            public_assertion_size,
            1  # Replication factor
        )
        print("======================== BID SUGGESTION CALCULATED")
        print(json.dumps(bid_suggestion, indent=4))

        # Increase allowance one time
        print(f"Increasing allowance by {bid_suggestion}...")
        allowance_increase = dkg.asset.increase_allowance(bid_suggestion)
        print("======================== ALLOWANCE INCREASED")
        print(allowance_increase)

    except Exception as e:
        print(f"Error increasing allowance: {e}")

# Main execution
if __name__ == '__main__':
    # Path to the folder containing the CSV files
    folder_path = '/Users/leesimpson/mock_data'

    # Load all CSV files into a single DataFrame
    df = load_csv_files(folder_path)

    # Create the DKG instance once
    node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
    blockchain_provider = BlockchainProvider(
        "testnet",
        "base",
        rpc_uri=rpc_uri,
        private_key=private_key,
    )
    dkg = DKG(node_provider, blockchain_provider)

    # Set allowance once before uploading knowledge assets
    random_record = generate_random_record(df)
    random_record['id'] = generate_unique_id(
        random_record['first_name'],
        random_record['last_name'],
        random_record['email'],
        random_record['gender'],
        random_record['ip_address']
    )
    json_ld_data = create_json_ld(random_record)
    set_allowance(dkg, json_ld_data)  # Increase allowance once

    # Get the number of CPU cores for setting up threading
    #num_threads = os.cpu_count()
    #num_threads = 2

    # Thread pool executor for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        # Loop to create and upload knowledge assets in parallel
        for _ in range(2):  # Adjust as needed
            # Step 1: Generate a random record from the DataFrame
            random_record = generate_random_record(df)

            # Step 2: Generate a unique ID for the selected record
            random_record['id'] = generate_unique_id(
                random_record['first_name'],
                random_record['last_name'],
                random_record['email'],
                random_record['gender'],
                random_record['ip_address']
            )

            # Step 3: Create JSON-LD data
            json_ld_data = create_json_ld(random_record)

            # Submit the task to the executor for parallel execution
            futures.append(executor.submit(upload_knowledge_asset_without_increase, json_ld_data, dkg))

        # Wait for all threads to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                print(f"Generated an exception: {exc}")
