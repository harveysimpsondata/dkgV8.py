import os
import pandas as pd
import random
import json
from dkg import DKG
from dkg.providers import BlockchainProvider, NodeHTTPProvider
from dkg.dataclasses import ParanetNodesAccessPolicy, ParanetMinersAccessPolicy
from dotenv import load_dotenv
import uuid
import time
import concurrent.futures
from web3 import Web3
from hexbytes import HexBytes

def print_json(json_dict: dict):
    def convert_hexbytes(data):
        if isinstance(data, dict):
            return {k: convert_hexbytes(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [convert_hexbytes(i) for i in data]
        elif isinstance(data, tuple) and hasattr(data, '_asdict'):
            return convert_hexbytes(data._asdict())
        elif isinstance(data, bytes):
            return data.decode("utf-8")
        elif isinstance(data, HexBytes):
            return data.to_0x_hex()
        else:
            return data

    serializable_dict = convert_hexbytes(json_dict)
    print(json.dumps(serializable_dict, indent=4))


####### Load environment variables #######
load_dotenv()
RPC_URI = "https://sepolia.base.org"

# Node 1 NAS
NODE_1_HOSTNAME = "192.168.1.37"
NODE_1_PORT = os.getenv('NODE_PORT')
PRIVATE_KEY_OP_1 = os.getenv('PRIVATE_KEY_V01')

# NODE 2 RASPBERRY PI
# NODE_2_HOSTNAME = "192.168.1.31"
# NODE_2_PORT = os.getenv('NODE_PORT')
# PRIVATE_KEY_OP_2 = os.getenv('PRIVATE_KEY_POOP_OP1')
mangement="13848342483de571dd49234c258267cb7cfd816716071651d5c5b97a2a27636d"
NODE_1_PROVIDER = NodeHTTPProvider(f"http://{NODE_1_HOSTNAME}:{NODE_1_PORT}")
BLOCKCHAIN_1_PROVIDER = BlockchainProvider(
    "testnet",
    "base", 
    rpc_uri=RPC_URI,
    private_key=PRIVATE_KEY_OP_1,
)

# NODE_2_PROVIDER = NodeHTTPProvider(f"http://{NODE_2_HOSTNAME}:{NODE_2_PORT}")
# BLOCKCHAIN_2_PROVIDER = BlockchainProvider(
#     "testnet",
#     "base",
#     rpc_uri=RPC_URI,
#     private_key=PRIVATE_KEY_OP_2,
# )

### Get Node 1 info
dkg_1 = DKG(NODE_1_PROVIDER, BLOCKCHAIN_1_PROVIDER)

paranet_ual = "did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/6737441"
dkg_1.paranet.add_curated_miners(paranet_ual, "0x81E9219e73B83b6f786e384b1547428983acB0d3")
miner_addresses = [
    # "0x87803385D2bCe9cbc7c532AD7ab2B9C665C2023E",
    # "0x2C107f170fc57FFe97A3088BB26BdfA876936AC3",
    # "0x253D6F4EDdeC078d40bf3CD76A2b6b5E9059E123",
    # "0xE5F284Cb38D7764BfC0e9FfFFb95eAD3F0F105d7",
    # "0x41E2596C5e597D2984Aa7B60d73a617c5F36eDF7",
    # "0xc579a2626e2c715a58d825B8F857e8021FaCF2B8",
    # "0xFF227e7F7A0426F409aa9BE1d72d12A28337e4d4",
    # "0xE6A3fdb044B9fd8fd05FB49E41BE80414646f084",
    # "0x90Cfb4E446fE94F70A960eEEd66eBa129Ccc9bD4",
    # "0x75a0757cC1ABfbebC4670E136Cafb756C502F03B",
    # "0x863AD930c62c8E4debFc51ec933D3Be213e7b16f",
    # "0x322e89E0F3CaF47C3D51cc39d04622FC2ebf917d",
    # "0xb58F02Ae3363e6EccA33F9B03c10Ed69fcB1d60D",
    # "0x2e314de28520AD5Ec38426955A0Eb53b899E431E",
    # "0x9d5193cd63757542e5bE3128ff7dB5f515156f82",
    # "0x8798E74d5FDD4768c7Cffe7BA20E1B5275454844",
    # "0x121df10E6B053342C64FC23dd9F2AF88aBb3ae64",
    "0xf7da07e1a618D15730a9c1fadAb5909f4E0b5674",
    "0x4a267F47B8C00C28226aeC2050bF8C230DF3d475",
    "0x49bEd47ec5721984C4E7EB1CB2Be8Aa858668B72",
    "0x4d7a90180D311D06EE0e87325216e727231147d3",
    "0x7f9E5e769adB92576A55be1632274A12105E518D",
    "0x2d438Ee834a83A622bCf71574eEf670ca2d44599",
    "0x92ab96c619B8fE65EfeaE6Ba1fB7f46E57bFDc30",
    "0x3F208455f565CDbB7996A73D9fE8b7153BE1Fe13",
    "0x49edA2d33361f7fA7276c0121eAd1B42A6f7674e",
    "0xf5A8496Fd2fB0a2F08E492bfb295CB50035749d6",
    "0xd54b45E6c2Bc2E5C7eB64D6AaD51bc774Fcfc14d",
    "0xA757E43f8480eEA546cE1Bb0A9c9D57b3E0240F7",
    "0xCC0fb793BCbE116D7564E93b69FC9c486436D5E6",
    "0x34d001C0eC02132b37D0c2b566a9Ac58829502e9",
    "0x1797Bd77ac76911897d4c7591405087CeBe5Ad73",
    "0x241Ad9249D8636a3c3746f78C7e25b19E5AB4841",
    "0x37ED158e3Fc79c1aF3c4e5b4CBdafE339CD34B23",
    "0x97B9B2BE2E46b9A17C73563669484f665dF71493",
    "0x9A02aB2Fbeb6ea3d66b27CF3175d5Eb04048dA29",
    "0x04F131108316b6270f1F15187D685ab98E775e2B",
    "0xC2AA2A9186652d6844c0Db0231d34fB04516674D",
    "0xD0A1880590D5e9eC31019D4110a494B63911A5a7",
    "0xf5fc178ddb5513DE8CFe77917F4803e3770f66B9",
    "0x91fc11F52Fea4d72D2F90f94Fdde95c0F6EC0Abd",
    "0x81b40E49B850F6347DBD369Ee75Ab9296d5b455F",
    "0x37d5E41eCD875b8f94409A7799803701EBd18109",
    "0x9e8F24C7050f8D3AdE2946EeEB292581a9F57274",
    "0x07Efbd54BdB7dB8a1d429FF00ca23aF69612b6A6",
    "0x3ea7f5c2E568c04996AB64F9952Be0EdaF55eEa9",
    "0x1088929FC7b516a2Fd1090f8bb643e1334895265",
    "0x4a23bfcB29682F6FF88beab64254606Cfb30616D",
    "0x0eC0B6AED0FDFd46bFf04E45B99F8CD0FD5D4A9E",
    "0x4d1c9F1b08E411b01aCf68894C1568E9745D333F",
    "0x23d1036E8CcB9577fF79B4FcE79Df8235839b8F6",
    "0xD24F1EA1635a6C12B5CEC83Ac64B1b9EC1DAeE4b",
    "0x74A685A3A5465C00780E32EC0A1D09C8760A6967",
    "0x9E5Ba7581A4999EC1b73E60C5d9Cfa6DaA20718C",
    "0x365A8A3A57D8746F7FE1b4a2EdE7E6D7F1FF706a",
    "0x0693D1B52277C3E7741dad3062becef41BCd4c7d",
    "0x175257CC581bA211972Cf2Cd110665c7A8CE3a8f",
    "0x509FBA28E98b0b9C725a27587bb8cD2e871Bcb28",
    "0xf34687f0B984a201BAB47e9927bC94EDE4d7d0E9",
    "0xe86D81804b186bEa1699D5f06856906F41e95e61",
    "0xF2ceEEb9c0723e687C4CE62575cC38c9110Aca5c",
    "0x5112627e0e320Fa8CA25C7307239A42aE3A0D7A2",
    "0x4D459093d13CA69F10AF9C8719b41e4115d23A23",
    "0xdDCF8a1431F5bb6ED94FDffCC108D76b34b3253f",
    "0x9952A35dECdc6707f52EfA317D8a73a1eEF3C0d8",
    "0xD4bF6a5a37E4A03d4f7992a8143FF12500680dbE",
    "0x589b4dFC29d6352302956Cd72d553b1065c837a7",
    "0x87122A98E7358435F1132403dB21C1B5f48018a0",
    "0x10124678DcDEA7312eeE77BFFaA7B95137e5b3f0",
    "0xd155ee84d4c27Dd9c5bfbb011A5B70d04f8A64F0",
    "0x9DC58EA6f330B8e2261E73d726FA284A97b04485",
    "0xeF3458570496f817f79eD558146007FDf5E8d443",
    "0x249f1E7D12162eE0BD7e3dC7F7479E7F93e30A2d",
    "0x88825353f4606d71e7186A5BcC3b3bC008A794ab",
    "0xF03B375fB24DE8BdA5a97f0417Bd72E23FAD3dEF",
    "0xdB34b9d307150C50669e27653FE4fA1A54904d47",
    "0xC85f01fcEB11ED370Fb51d5bcae8D3c1bCae38E6",
    "0x16d312768Fa43fbed0EfC99628B5131eDb489e7c",
    "0x14eD3f0B0ffD759A16A27bE9D109B5C8c313560a",
    "0x72982862E10A855919709F5Cf6020E86CB1D9C45",
    "0x3A0F9875eDBe498bc93FC6f5E6b62688b50dfb8D",
    "0xA84d9E75fc345ACff79f0daCB052C5D1495AE30D",
    "0x7D56eCa62CE28c78C71419c06225960329372eA6",
    "0xe11B562c045628E9A948707cC2Ae5A325a460c05",
    "0xCEcbD5CA318b3A0914b6A3E3Aa4805a6629f227A",
    "0x9d4270e8D7c035f5E8048D1072fe1f21A610e79C",
    "0xeeEf853E90855bb19Bf5027A7DfBFe68BC39c516",
    "0x86d17dbFF26A3CBb3a16e480A9D8B8cECd022654",
    "0xbe54f4D6431f116645DeE20b3B844B0A2b30fD6F",
    "0x7DCA48D342d0758867fD125404343c551750E715",
    "0x34F80A6C8082F0982cf453c2d0566cF3D9b74897",
    "0xd2E923f9a1c2e2c5DA2b90C6A81B08ee397c1854",
    "0x67D27532Ccb1D0636D3D6F4Ea73779363d0f5cB6",
    "0x9C0ffa6E411c34B85549Bb3e96353c0bc170900c",
    "0x68f5bbfFD9b86519666f49802f86EacC882390a8",
    "0x2392896E3d0789ed4102E2444D93D73f870c7363",
    "0x8211a7a3FaE156ceb14E31CCBF7B9ff7a3160149",
    "0x81E9219e73B83b6f786e384b1547428983acB0d3",
    "0xd8CAFb99CE266d2aD822A87cA951cc96D701011c",
]

# dkg_1.paranet.add_curated_miners(paranet_ual, miner_addresses)

# knowledge_miners = dkg_1.paranet.get_knowledge_miners(paranet_ual)
# print("======================== ADDED KNOWLEDGE MINERS TO A CURATED PARANET")
# print_json(knowledge_miners)



### Get Node 2 info
# dkg_2 = DKG(NODE_2_PROVIDER, BLOCKCHAIN_2_PROVIDER)
# info_result_2 = dkg_2.node.info
# print_json(info_result_2)

# node_2_identity_id = dkg_2.node.get_identity_id(dkg_2.blockchain_provider.account.address)
# public_address_2 = dkg_2.blockchain_provider.account.address
# print(f"Node 2 Identity ID: {node_2_identity_id}, Node 2 Public Address: {public_address_2}")


###### Curated Paranets for both nodes #######
# paranet_data = {
#     "private": {
#         "@context": ["http://schema.org"],
#         "@id": "uuid:food_recipes",
#         "name": "Food Recipes",
#         "description": "A paranet for food recipes",
#     }
# }

# create_paranet_knowledge_asset_result = dkg_1.asset.create(paranet_data, 6)

# if create_paranet_knowledge_asset_result:
#     print("======================== FOOD RECIPE PARANET KNOWLEDGE ASSET CREATED")
#     print_json(create_paranet_knowledge_asset_result)

# paranet_ual = create_paranet_knowledge_asset_result["UAL"]

# print(f"Food Recipe Paranet UAL: {paranet_ual}")

# create_paranet_result = dkg_1.paranet.create(
#     paranet_ual,
#     "FoodRecipeParanet",
#     "A paranet for sharing and discovering food recipes",
#     ParanetNodesAccessPolicy.CURATED,
#     ParanetMinersAccessPolicy.CURATED
# )

# if create_paranet_result:
#     print("======================== A CURATED PARANET REGISTERED")
#     print_json(create_paranet_result)

# identity_ids = [node_1_identity_id]
# dkg_1.paranet.add_curated_nodes(paranet_ual, identity_ids)

# if curated_nodes:
#     print("======================== ADDED NODES TO A CURATED PARANET")
#     print_json(curated_nodes)

# miner_addresses = [
#     "0x81E9219e73B83b6f786e384b1547428983acB0d3",
#     "0x6f7c3248022Edd9C81AF5C0fe27e14f548172F2c",
# ]
# dkg_1.paranet.add_curated_miners(paranet_ual, miner_addresses)
# knowledge_miners = dkg_1.paranet.get_knowledge_miners(paranet_ual)
# print("======================== ADDED KNOWLEDGE MINERS TO A CURATED PARANET")
# print_json(knowledge_miners)

# local_store_first_asset_result = dkg_1.asset.local_store(
#     paranet_data,
#     6,
#     paranet_ual=paranet_ual,
# )

# if local_store_first_asset_result:
#     print("======================== MINT A KA, LOCAL STORE AND SUBMIT IT TO A CURATED PARANET - KNOWLEDGE MINER IS APPROVED")
#     print_json(local_store_first_asset_result)

# local_store_second_asset_result = dkg_2.asset.local_store(
#     paranet_data,
#     6,
#     paranet_ual="did:dkg:base:84532/0xb8b904c73d2fb4d8c173298a51c27fab70222c32/6737441",
# )

# if local_store_second_asset_result:
#     print("======================== MINT A KA, LOCAL STORE AND SUBMIT IT TO A CURATED PARANET - KNOWLEDGE MINER IS APPROVED")
#     print_json(local_store_second_asset_result)







