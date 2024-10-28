import os
import concurrent.futures
from dotenv import load_dotenv
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from web3 import Web3
import time

# Load environment variables (assuming you have .env with blockchain details)
load_dotenv()

# Load environment variables
node_hostname = os.getenv("NODE_HOSTNAME")
node_port = os.getenv('NODE_PORT')
rpc_uri = os.getenv('BASE_TESTNET_URI')
print(rpc_uri)
# Web3 setup
web3 = Web3(Web3.HTTPProvider("https://sepolia.base.org"))

# Get private keys from environment variables
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
    os.getenv('PRIVATE_KEY_20'),
    os.getenv('PRIVATE_KEY_21'),
    os.getenv('PRIVATE_KEY_22'),
    os.getenv('PRIVATE_KEY_23'),
    os.getenv('PRIVATE_KEY_24'),
    os.getenv('PRIVATE_KEY_25'),
    os.getenv('PRIVATE_KEY_26'),
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

# Function to read UALs from a .txt file
def get_uals_from_txt(file_path):
    try:
        with open(file_path, 'r') as file:
            uals = file.read().splitlines()  # Read all lines and strip any empty ones
            uals = [ual for ual in uals if ual.strip()]
        return uals
    except Exception as e:
        print(f"Error reading UALs from {file_path}: {e}")
        return []

# Allowance cache to avoid redundant allowance checks
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

# Function to upload UAL to the Paranet with nonce management
def upload_ual_to_paranet(ual, private_key, paranet_ual, allowance_value, web3, retries=3):
    try:
        # Set up DKG
        node_provider = NodeHTTPProvider(f"http://{node_hostname}:{node_port}")
        blockchain_provider = BlockchainProvider("testnet", "base", rpc_uri=rpc_uri, private_key=private_key)
        dkg = DKG(node_provider, blockchain_provider)

        # Get the public key (address) for nonce management
        public_key = web3.eth.account.from_key(private_key).address

        # Get the current nonce
        nonce = web3.eth.get_transaction_count(public_key, 'pending')
        gas_price = web3.eth.gas_price

        for attempt in range(retries):
            try:
                # Ensure allowance is set (optional, based on your flow)
                ensure_allowance(dkg, private_key, allowance_value)

                # Submit UAL to Paranet
                result = dkg.asset.submit_to_paranet(ual, paranet_ual)
                print(f"===========Successfully submitted UAL {ual} to Paranet {paranet_ual}. ==================")
                return result
            except Exception as e:
                if "replacement transaction underpriced" in str(e):
                    # Increase gas price for retries
                    gas_price = int(gas_price * 1.2)
                    print(f"Retrying with increased gas price: {gas_price}")
                else:
                    raise e
    except Exception as e:
        print(f"Error submitting UAL {ual} to Paranet: {e}")
        return None

# Main execution
if __name__ == '__main__':
    paranet_ual = 'did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/5588244'  # Example Paranet UAL
    folder_path = './wallets_ual'  # Directory containing UAL .txt files
    allowance_value = 10000000000000000000  # 10 Ether equivalent

    # Create a function to handle the worker logic
    def process_key_and_uals(i, private_key):
        file_name = f'dkg_spend_{i}.txt'  # Assuming file names are like 'dkg_spend_1.txt', etc.
        file_path = os.path.join(folder_path, file_name)

        # Get UALs from the current file
        uals = get_uals_from_txt(file_path)

        # Process each UAL for the given private key
        for ual in uals:
            result = upload_ual_to_paranet(ual, private_key, paranet_ual, allowance_value, web3)
            if result:
                print(f"UAL submitted successfully by key {i}.")

    # Iterate over the private keys and launch one worker per key with a slight delay between launches
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        for i, private_key in enumerate(private_keys, start=1):
            # Submit a worker with a slight delay
            executor.submit(process_key_and_uals, i, private_key)
            time.sleep(0.1)  # Introduce a delay between workers (5 seconds in this case)

    print("All UALs have been processed.")
