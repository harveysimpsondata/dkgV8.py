from web3 import Web3
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Initialize Web3
w3 = Web3()

def get_public_address(private_key):
    """Convert a private key to its public address"""
    try:
        # Remove '0x' prefix if present and ensure private key is properly formatted
        if private_key.startswith('0x'):
            private_key = private_key[2:]
            
        # Create account from private key
        account = w3.eth.account.from_key(f"0x{private_key}")
        return account.address
    except Exception as e:
        return f"Error with key: {str(e)}"

def main():
    # Loop through numbers 1 to 100
    for i in range(1, 101):
        key_name = f"PRIVATE_KEY_{i}"
        private_key = os.getenv(key_name)
        if private_key:
            address = get_public_address(private_key)
            if not address.startswith('Error'):
                print(f'"{address}",')

if __name__ == "__main__":
    main()
