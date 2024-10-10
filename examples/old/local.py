import os
import pandas as pd
import random
import json
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dotenv import load_dotenv
import uuid
import asyncio
import concurrent.futures

# Load environment variables (assuming you have .env with blockchain details)
load_dotenv()

# Load environment variables (your DKG node should be on localhost or the same server)
node_hostname = 'localhost'
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')

# Load multiple private keys for parallel processing
private_keys = [
    os.getenv('PRIVATE_KEY_4'),
    os.getenv('PRIVATE_KEY_5')
    # Add more private keys as needed for parallel uploads
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

# Function to randomly select values from each column in the DataFrame
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

# Asynchronous function to handle uploading assets to the DKG with retries
async def upload_knowledge_asset_with_retry(json_ld_data, private_key, retries=3, delay=5):
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

        # Calculate the bid suggestion for the asset
        public_assertion_id = dkg.assertion.get_public_assertion_id({"public": json_ld_data})
        public_assertion_size = dkg.assertion.get_size({"public": json_ld_data})
        bid_suggestion = dkg.network.get_bid_suggestion(
            public_assertion_id,
            public_assertion_size,
            1  # Replication factor
        )

        # Check current allowance
        current_allowance = dkg.asset.get_current_allowance()

        # Increase allowance if the current allowance is below the bid suggestion
        if current_allowance < bid_suggestion:
            print(f"Current allowance ({current_allowance}) is less than bid suggestion ({bid_suggestion}). Increasing allowance...")
            await asyncio.to_thread(dkg.asset.increase_allowance, bid_suggestion)
        else:
            print(f"Current allowance ({current_allowance}) is sufficient. No increase needed.")

        # Now, upload the asset regardless of whether allowance was increased or not
        create_asset_result = await asyncio.to_thread(dkg.asset.create, {"public": json_ld_data}, 1)

        # Confirm the asset was created
        if create_asset_result and create_asset_result.get("UAL"):
            validate_ual = dkg.asset.is_valid_ual(create_asset_result["UAL"])
            print(f"Asset successfully created. Is UAL valid: {validate_ual}")
        else:
            print(f"Asset creation failed for public assertion ID: {public_assertion_id}")

    except Exception as e:
        if retries > 0:
            print(f"Error creating asset: {e}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            await upload_knowledge_asset_with_retry(json_ld_data, private_key, retries=retries - 1, delay=delay * 2)
        else:
            print(f"Asset creation failed after retries: {e}")

# Function to format and submit the assets using multithreading and asyncio
async def format_and_upload_assets(df, private_keys, num_assets=100):
    loop = asyncio.get_running_loop()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(private_keys)) as executor:
        for _ in range(num_assets):
            # Generate and format the asset, then upload it immediately
            random_record = await loop.run_in_executor(executor, generate_random_record, df)
            random_record['id'] = generate_unique_id(
                random_record['first_name'],
                random_record['last_name'],
                random_record['email'],
                random_record['gender'],
                random_record['ip_address']
            )
            json_ld_data = create_json_ld(random_record)

            # Alternate between private keys for parallel uploads
            private_key = private_keys[_ % len(private_keys)]

            # Immediately upload the asset as it's ready with retry logic
            asyncio.create_task(upload_knowledge_asset_with_retry(json_ld_data, private_key))

# Main execution loop
async def main():
    folder_path = '../mock_data'  # Ensure you are pointing to the correct CSV data location
    df = load_csv_files(folder_path)

    while True:
        await format_and_upload_assets(df, private_keys, num_assets=50)  # You can adjust the number of assets as needed
        print("Waiting 1 seconds before running the next batch...")
        await asyncio.sleep(1)

# Run the asyncio event loop
if __name__ == '__main__':
    asyncio.run(main())
