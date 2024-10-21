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
from web3 import Web3

# Load environment variables (assuming you have .env with blockchain details)
load_dotenv()

# Load environment variables
node_hostname = os.getenv("NODE_HOSTNAME")
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')

# Web3 setup
web3 = Web3(Web3.HTTPProvider("https://sepolia.base.org"))

# Load private keys (for parallel processing)
private_keys = [
    # os.getenv('PRIVATE_KEY_1'),
    # os.getenv('PRIVATE_KEY_2'),
    # os.getenv('PRIVATE_KEY_3'),
    # os.getenv('PRIVATE_KEY_4'),
    # os.getenv('PRIVATE_KEY_5'),
    # os.getenv('PRIVATE_KEY_6'),
    # os.getenv('PRIVATE_KEY_7'),
    # os.getenv('PRIVATE_KEY_8'),
    # os.getenv('PRIVATE_KEY_9'),
    # os.getenv('PRIVATE_KEY_10'),
    # os.getenv('PRIVATE_KEY_11'),
    # os.getenv('PRIVATE_KEY_12'),
    # os.getenv('PRIVATE_KEY_13'),
    # os.getenv('PRIVATE_KEY_14'),
    # os.getenv('PRIVATE_KEY_15'),
    # os.getenv('PRIVATE_KEY_16'),
    # os.getenv('PRIVATE_KEY_17'),
    # os.getenv('PRIVATE_KEY_18'),
    # os.getenv('PRIVATE_KEY_19'),
    # os.getenv('PRIVATE_KEY_20'),
    # os.getenv('PRIVATE_KEY_21'),
    # os.getenv('PRIVATE_KEY_22'),
    # os.getenv('PRIVATE_KEY_23'),
    # os.getenv('PRIVATE_KEY_24'),
    # os.getenv('PRIVATE_KEY_25'),
    # os.getenv('PRIVATE_KEY_26'),
    os.getenv('PRIVATE_KEY_27'),
    os.getenv('PRIVATE_KEY_28'),
    os.getenv('PRIVATE_KEY_29'),
    os.getenv('PRIVATE_KEY_30'),
    os.getenv('PRIVATE_KEY_31'),
    os.getenv('PRIVATE_KEY_32'),
    os.getenv("PRIVATE_KEY_33"),
    os.getenv("PRIVATE_KEY_34"),
    os.getenv("PRIVATE_KEY_35"),
    os.getenv("PRIVATE_KEY_36"),
    os.getenv("PRIVATE_KEY_37"),
    os.getenv("PRIVATE_KEY_38"),
    os.getenv("PRIVATE_KEY_39"),
    os.getenv("PRIVATE_KEY_40"),
    os.getenv("PRIVATE_KEY_41"),
    os.getenv("PRIVATE_KEY_42"),
    os.getenv("PRIVATE_KEY_43"),
    os.getenv("PRIVATE_KEY_44"),
    os.getenv("PRIVATE_KEY_45"),
    os.getenv("PRIVATE_KEY_46"),
    os.getenv("PRIVATE_KEY_47"),
    os.getenv("PRIVATE_KEY_48"),
    os.getenv("PRIVATE_KEY_49"),
    os.getenv("PRIVATE_KEY_50"),
]

# Generate a unique ID
def generate_unique_id(email, ip_address):
    random_string = f"{email}_{ip_address}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, random_string))

# Randomly select values from each column
def generate_random_record(df):
    email = random.choice(df['email'].dropna().tolist())
    ip_address = random.choice(df['ip_address'].dropna().tolist())
    return {
        "email": email,
        "ip_address": ip_address
    }

# Load CSV files into a single DataFrame
def load_csv_files(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    dataframes = [pd.read_csv(os.path.join(folder_path, csv_file)) for csv_file in csv_files]
    combined_df = pd.concat(dataframes, ignore_index=True)
    return combined_df

def create_json_ld(record):
    return {
        "@context": {
            "@vocab": "http://schema.org/",
            "id": "http://schema.org/identifier",
            "email": "http://schema.org/email",
            "ip_address": "http://schema.org/IPAddress"
        },
        "@graph": [{
            "@type": "Person",
            "id": str(record['id']),
            "email": record['email'],
            "ip_address": record['ip_address']
        }]
    }

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

# Function to handle nonce too low issues and retry the transaction
def send_new_transaction(public_key, private_key):
    try:
        nonce = web3.eth.get_transaction_count(public_key, 'latest')
        gas_price = web3.eth.gas_price
        estimated_gas = web3.eth.estimate_gas({
            'to': '0x1A52d36f4C6E3Fd618702B5C24EdeA581b5f90C4',  # Random address to send a tiny amount
            'from': public_key,
            'value': web3.to_wei(0.0001, 'ether')
        })

        multiplier = 1.2  # Start with 1.2x the current gas price

        while True:
            try:
                # Create the new transaction with the dynamic gas price
                transaction = {
                    'to': '0x1A52d36f4C6E3Fd618702B5C24EdeA581b5f90C4',
                    'value': web3.to_wei(0.0001, 'ether'),
                    'gas': estimated_gas,
                    'gasPrice': int(gas_price * multiplier),
                    'nonce': nonce
                }

                # Sign and send the transaction
                signed_txn = web3.eth.account.sign_transaction(transaction, private_key)
                txn_hash = web3.eth.send_raw_transaction(signed_txn.rawTransaction)
                print(f"Replacement transaction sent with hash: {web3.to_hex(txn_hash)}")
                break  # Exit the loop on success

            except Exception as e:
                if "replacement transaction underpriced" in str(e):
                    print(f"Transaction failed with underpriced error. Increasing gas price by 20%. Error: {str(e)}")
                    multiplier += 0.2  # Increase gas price by 20%
                    time.sleep(3)  # Wait for 2 seconds before retrying
                else:
                    print(f"Failed to send replacement transaction: {str(e)}")
                    break  # Exit loop if it's not a gas price issue

    except Exception as e:
        print(f"Failed to initiate transaction: {str(e)}")

# Function to upload knowledge assets using the DKG with nonce handling and transaction retry
def upload_knowledge_asset_with_increase(json_ld_data, private_key, allowance_value):
    try:
        node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
        blockchain_provider = BlockchainProvider(
            "testnet",
            "base",
            rpc_uri=rpc_uri,
            private_key=private_key,
        )
        dkg = DKG(node_provider, blockchain_provider)

        # Get public key from private key
        public_key = web3.eth.account.from_key(private_key).address

        # Ensure the allowance is set before uploading
        ensure_allowance(dkg, private_key, allowance_value)

        # Retry logic for nonce too low errors
        retry_count = 0
        max_retries = 5

        while retry_count < max_retries:
            try:
                # Try creating the asset
                create_asset_result = dkg.asset.create({"public": json_ld_data}, 1)
                if create_asset_result:

                    print(f"********************** * * * * * * * * ASSET CREATED * * * * * * * ***********************")
                    return create_asset_result
                break  # Exit loop on success

            except Exception as e:
                if "nonce too low" in str(e):
                    print(f"Error creating asset: {e}. Retrying with updated nonce.")
                    nonce = web3.eth.get_transaction_count(public_key, 'pending')
                    send_new_transaction(public_key, private_key)
                    retry_count += 1
                    time.sleep(3)  # Wait before retrying
                else:
                    print(f"Error creating asset: {e}")
                    break  # Exit loop if it's not a nonce issue

        if retry_count == max_retries:
            print(f"Max retries reached for nonce handling. Asset creation failed.")

    except Exception as e:
        print(f"Unexpected error: {e}")

# Main execution
if __name__ == '__main__':
    # Path to the folder containing the CSV files
    folder_path = '/Users/leesimpson/Desktop/mock_data'

    # Load all CSV files into a single DataFrame
    df = load_csv_files(folder_path)

    # Set the required allowance value
    allowance_value = 10000000000000000000  # 10 Ether equivalent (adjust as needed)

    # Set up a large pool of threads, with more workers than private keys to process them concurrently
    max_threads = 24  # Number of maximum concurrent threads

    # Thread pool executor for parallel uploads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        # Loop through private keys and create assets for each
        for _ in range(5000):  # Create 5000 knowledge assets
            # Generate a random record from the DataFrame
            random_record = generate_random_record(df)

            # Generate a unique ID for the selected record
            random_record['id'] = generate_unique_id(
                random_record['email'],
                random_record['ip_address'],
            )

            # Create JSON-LD data
            json_ld_data = create_json_ld(random_record)

            # Use alternating private keys for parallel uploads
            private_key = private_keys[_ % len(private_keys)]

            # Submit the task to the executor for parallel execution
            futures.append(executor.submit(upload_knowledge_asset_with_increase, json_ld_data, private_key, allowance_value))

            # Throttle the task submission rate to prevent overloading the node
            time.sleep(0.1)  # Delay of 0.05 seconds between task submissions

        # Ensure all threads complete their tasks
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                print(f"Generated an exception: {exc}")

        print("All assets created.")
