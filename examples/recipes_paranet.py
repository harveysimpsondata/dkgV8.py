import json
import time

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
blockchain_provider2 = BlockchainProvider(
    "development",
    "hardhat2:31337",
    private_key="0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
)

dkg = DKG(node_provider, blockchain_provider)
dkg2 = DKG(node_provider, blockchain_provider2)

node1_identity_id = dkg.node.get_identity_id(dkg.blockchain_provider.account.address)
node2_identity_id = dkg2.node.get_identity_id(dkg2.blockchain_provider.account.address)

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
        elif isinstance(data, bytes):
            return data.decode("utf-8")
        elif isinstance(data, HexBytes):
            return data.to_0x_hex()
        else:
            return data

    serializable_dict = convert_hexbytes(json_dict)
    print(json.dumps(serializable_dict, indent=4))


divider()

paranet_data = {
    "public": {
        "@context": ["http://schema.org"],
        "@id": "uuid:food_recipes",
        "name": "Food Recipes",
        "description": "A curated paranet for food recipes",
    }
}

create_paranet_knowledge_asset_result = dkg.asset.create(paranet_data, 1)

print("======================== PARANET KNOWLEDGE ASSET CREATED")
print_json(create_paranet_knowledge_asset_result)

divider()

paranet_ual = create_paranet_knowledge_asset_result["UAL"]
create_paranet_result = dkg.paranet.create(
    paranet_ual,
    "RecipeParanet",
    "A curated paranet for food recipes",
    ParanetNodesAccessPolicy.CURATED,
    ParanetMinersAccessPolicy.CURATED
)

print("======================== RECIPE PARANET CREATED")
print_json(create_paranet_result)

divider()

identity_ids = [node1_identity_id]
dkg.paranet.add_curated_nodes(paranet_ual, identity_ids)
curated_nodes = dkg.paranet.get_curated_nodes(paranet_ual)
print("======================== ADDED NODE TO A CURATED RECIPE PARANET")
print_json(curated_nodes)

divider()

identity_ids = [node2_identity_id]
dkg.paranet.add_curated_nodes(paranet_ual, identity_ids)
curated_nodes = dkg.paranet.get_curated_nodes(paranet_ual)
print("======================== ADDED ANOTHER NODE TO A CURATED RECIPE PARANET")
print_json(curated_nodes)

divider()

recipe_knowledge_asset_data = {
    "public": {
        "@context": ["http://schema.org"],
        "@id": "uuid:recipe1",
        "name": "Spaghetti Bolognese",
        "ingredients": [
            {"@id": "uuid:ingredient1", "name": "Spaghetti"},
            {"@id": "uuid:ingredient2", "name": "Ground Beef"},
            {"@id": "uuid:ingredient3", "name": "Tomato Sauce"},
        ],
        "instructions": "Cook spaghetti. Brown ground beef. Mix with tomato sauce. Combine with spaghetti.",
    }
}

create_recipe_knowledge_asset_result = dkg.asset.create(recipe_knowledge_asset_data, 1)

print("======================== RECIPE KNOWLEDGE ASSET CREATED")
print_json(create_recipe_knowledge_asset_result)

divider()

recipe_ual = create_recipe_knowledge_asset_result["UAL"]
submit_recipe_result = dkg.asset.submit_to_paranet(recipe_ual, paranet_ual)

print("======================== RECIPE KNOWLEDGE ASSET SUBMITTED TO THE PARANET")
print_json(submit_recipe_result)

divider()

