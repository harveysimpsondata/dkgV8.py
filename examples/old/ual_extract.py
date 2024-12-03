import os
from web3 import Web3
import json
import time
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Set up the Web3 provider
rpc_url = os.getenv("BASE_TESTNET_URI")  # Your RPC URL
web3 = Web3(Web3.HTTPProvider(rpc_url))

# ERC721 token contract address and ABI (simplified ABI for Transfer event)
token_contract_address = '0xb8B904c73D2fB4D8c173298A51c27Fab70222c32'

token_abi = [
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "name": "from",
                "type": "address"
            },
            {
                "indexed": True,
                "name": "to",
                "type": "address"
            },
            {
                "indexed": True,
                "name": "tokenId",
                "type": "uint256"
            }
        ],
        "name": "Transfer",
        "type": "event"
    }
]



# Create a contract instance
token_contract = web3.eth.contract(address=token_contract_address, abi=token_abi)

# Function to convert private key to public address
def get_public_address(private_key):
    account = web3.eth.account.from_key(private_key)
    return account.address

# Wallets (private keys) to loop through from the .env file
private_keys = [
    os.getenv('PRIVATE_KEY_1'),
    os.getenv('PRIVATE_KEY_2'),
    os.getenv('PRIVATE_KEY_3'),
    # Add more wallets as needed
]

# Convert private keys to public addresses
wallets_used = {}
for i, private_key in enumerate(private_keys, start=1):
    if private_key:
        public_address = get_public_address(private_key)
        wallets_used[f"dkg_spend_{i}"] = public_address

# Function to fetch all token IDs for a specific wallet in batches
def fetch_token_ids_for_wallet(wallet_name, wallet_address, batch_size=5000):
    latest_block = web3.eth.get_block_number()
    token_ids = []
    output_file_path = f'wallets_ual/{wallet_name}.txt'

    with open(output_file_path, 'w') as file:
        # Fetch events in batches
        for from_block in range(16178746, latest_block + 1, batch_size):
            to_block = min(from_block + batch_size - 1, latest_block)
            print(f"Fetching blocks from {from_block} to {to_block} for {wallet_name}")

            try:
                events = token_contract.events.Transfer.getLogs(
                    fromBlock=from_block,
                    toBlock=to_block,
                    argument_filters={'to': wallet_address}
                )
                # Extract token IDs and write to file
                for event in events:
                    token_id = event['args']['tokenId']
                    token_ids.append(token_id)
                    file.write(f"{token_id}\n")
            except Exception as e:
                print(f"Error fetching logs for block range {from_block} - {to_block}: {e}")

    print(f"Total Token IDs for {wallet_name} ({wallet_address}): {len(token_ids)}")
    print(f"Token IDs have been saved to {output_file_path}")
    return token_ids

# Function to loop through all wallets and fetch token IDs
def fetch_all_wallets_token_ids():
    for wallet_name, wallet_address in wallets_used.items():
        fetch_token_ids_for_wallet(wallet_name, wallet_address)

# Run the function
fetch_all_wallets_token_ids()
