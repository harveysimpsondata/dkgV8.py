import json

from hexbytes import HexBytes

from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dkg.dataclasses import ParanetNodesAccessPolicy, ParanetMinersAccessPolicy

node_provider = NodeHTTPProvider("http://localhost:8900")
blockchain_provider = BlockchainProvider(
    "development",
    "hardhat2:31337",
    private_key="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
)

dkg = DKG(node_provider, blockchain_provider)

def divider():
    print("==================================================")
    print("==================================================")
    print("==================================================")


def print_json(json_dict: dict):
    def convert_hexbytes(data):
        if isinstance(data, dict):
            return {k: convert_hexbytes(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [convert_hexbytes(i) for i in data]
        elif isinstance(data, HexBytes):
            return data.to_0x_hex()
        else:
            return data

    serializable_dict = convert_hexbytes(json_dict)
    print(json.dumps(serializable_dict, indent=4))


divider()

# Create a knowledge asset for the food recipe paranet
paranet_data = {
    "public": {
        "@context": ["http://schema.org"],
        "@id": "uuid:food-recipe-paranet",
        "name": "Food Recipe Paranet",
        "description": "A paranet for sharing and discovering food recipes",
    }
}

create_paranet_knowledge_asset_result = dkg.asset.create(paranet_data, 1)

print("======================== FOOD RECIPE PARANET KNOWLEDGE ASSET CREATED")
print_json(create_paranet_knowledge_asset_result)

divider()

paranet_ual = create_paranet_knowledge_asset_result["UAL"]
create_paranet_result = dkg.paranet.create(
    paranet_ual,
    "FoodRecipeParanet",
    "A paranet for sharing and discovering food recipes",
    ParanetNodesAccessPolicy.OPEN,
    ParanetMinersAccessPolicy.OPEN
)

print("======================== FOOD RECIPE PARANET CREATED")
print_json(create_paranet_result)

divider()

# Add a service to the food recipe paranet
paranet_service_data = {
    "public": {
        "@context": ["http://schema.org"],
        "@id": "uuid:recipe-service",
        "service": "Recipe Service",
        "description": "Service for managing and querying food recipes",
    }
}

create_paranet_service_knowledge_asset_result = dkg.asset.create(
    paranet_service_data, 1
)

print("======================== RECIPE SERVICE KNOWLEDGE ASSET CREATED")
print_json(create_paranet_service_knowledge_asset_result)

divider()

paranet_service_ual = create_paranet_service_knowledge_asset_result["UAL"]
create_paranet_service_result = dkg.paranet.create_service(
    paranet_service_ual,
    "RecipeService",
    "Service for managing and querying food recipes",
    ["0x03C094044301E082468876634F0b209E11d98452"],
)

print("======================== RECIPE SERVICE CREATED")
print_json(create_paranet_service_result)

divider()

add_services_result = dkg.paranet.add_services(paranet_ual, [paranet_service_ual])

print("======================== ADDED RECIPE SERVICE TO FOOD RECIPE PARANET")
print_json(add_services_result)



