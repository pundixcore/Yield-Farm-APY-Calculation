import json
import web3
from web3 import Web3
from web3.logs import STRICT, IGNORE, DISCARD, WARN
from typing import List
from math import *
import requests
from ast import literal_eval
import time
import decimal
import schedule
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import urllib.parse
import logging

# Connect Ethereum node
try:
    start_time = time.time()
    fxcore_testnet = "https://fx-json-web3.functionx.io:8545"  # fxcore mainnet
    web3 = Web3(Web3.HTTPProvider(fxcore_testnet))
    print("Connected to the fxcore ?", web3.isConnected())
    print("Latest block number ?", web3.eth.blockNumber)
    rewarderJson = open("./abi/RewarderViaMultiplier.json")
    rewarderAbi = json.load(rewarderJson)
    lpJson = open("./abi/FXSwapV2Pair.json")
    lpAbi = json.load(lpJson)
    Master_Farm_Json = open("./abi/MasterChefV2.json")
    Master_Farm_Json_ABI = json.load(Master_Farm_Json)

    # Mainnet proxy contract address
    Master_Farm_Contract_Address = "0x4bd522b2E25f6b1A874C78518EF25f5914C522dC"
    Master_Farm_Contract = web3.eth.contract(address=Master_Farm_Contract_Address, abi=Master_Farm_Json_ABI)

    FXG_WFX_LP_Token_Contract_Address = "0xC82287debda995665622d139dA8AEFA164748B21"
    FXG_WFX_LP_Token_Contract = web3.eth.contract(address=FXG_WFX_LP_Token_Contract_Address, abi=lpAbi)

    load_dotenv()
    mongoDBUser = os.getenv("MONGODB_USERNAME")
    mongoDBPW = os.getenv("MONGODB_PASSWORD")

except Exception as e:
    print("Unable to connect to the fxcore")
    logging.error(e)


# ##########################################################################################################
# Query ERC20 transfer event
# ##########################################################################################################

def queryData():
    response_json = dict()
    try:
        response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=fx-coin%2Cpundi-x-2%2Cpundi-x-purse%2Ctether%2Cusd-coin%2Cdai%2Cweth%2Cbaklava&vs_currencies=usd")
        response_json = response.json()
    except Exception as exception:
        print("Could not access the API endpoint from CoinGecko")
        logging.error(exception)
    print(response_json)
    pool_length = Master_Farm_Contract.functions.poolLength().call()
    total_alloc_point = Master_Farm_Contract.functions.totalAllocPoint().call()
    reward_per_block = Master_Farm_Contract.functions.rewardPerBlock().call()
    bonus_multiplier = Master_Farm_Contract.functions.BONUS_MULTIPLIER().call(block_identifier="latest")

    # fxg_LP_TSupply = FXG_WFX_LP_Token_Contract.functions.totalSupply().call()
    token_reserve = FXG_WFX_LP_Token_Contract.functions.getReserves().call()
    fxg_per_fx = token_reserve[0]/token_reserve[1]
    response_json["FXG"] = {"usd": response_json["fx-coin"]["usd"]/fxg_per_fx}
    # response_json.add("FXG", {"usd":fxg_per_fx * response_json["fx-coin"]["usd"]})
    print(response_json["FXG"]["usd"])

    tvl_dict = {"tvl": dict()}
    apr_dict = {"apr": dict()}
    apy_daily_dict = {"apyDaily": dict()}
    lp_token_value_per_coin_dict = {"lpTokenValue": dict()}
    token_symbol_to_name_mapping = {
        "DAI": "dai",
        "USDC": "usd-coin",
        "WFX": "fx-coin",
        "USDT": "tether",
        "PUNDIX": "pundi-x-2",
        "PURSE": "pundi-x-purse",
        "WETH": "weth",
        "FXG": "FoxGaming",
        "BAVA": "baklava"
    }

    six_decimal_tokens = {"USDC", "USDT"}

    for pool_index in range(pool_length):
        reward_Multiplier = [1]
        reward_Token = ["0x0"]
        [lp_token_address,
         alloc_point,
         last_reward_block,
         acc_reward_per_share] = Master_Farm_Contract.functions.poolInfo(pool_index).call()
        rewarder_address = Master_Farm_Contract.functions.rewarder(pool_index).call(block_identifier="latest")

        lp_token_contract = web3.eth.contract(address=lp_token_address, abi=lpAbi)
        lp_token_a_address = lp_token_contract.functions.token0().call()
        lp_token_b_address = lp_token_contract.functions.token1().call()
        lp_token_a_contract = web3.eth.contract(address=lp_token_a_address, abi=lpAbi)
        lp_token_b_contract = web3.eth.contract(address=lp_token_b_address, abi=lpAbi)
        # How many tokens are deposited at lp_token_a_contract and lp_token_b_contract
        lp_token_a_balance = lp_token_a_contract.functions.balanceOf(lp_token_address).call()
        lp_token_b_balance = lp_token_b_contract.functions.balanceOf(lp_token_address).call()
        lp_token_total_supply = lp_token_contract.functions.totalSupply().call()
        lp_token_deposited = lp_token_contract.functions.balanceOf(Master_Farm_Contract_Address).call()
        # print("lp_token_deposited: ", lp_token_deposited)
        lp_token_a_symbol = lp_token_a_contract.functions.symbol().call()
        lp_token_b_symbol = lp_token_b_contract.functions.symbol().call()
        
        print("lp_token_a_symbol", lp_token_a_symbol, "lp_token_b_symbol", lp_token_b_symbol)
        if lp_token_a_symbol == "FXG":
            token_a_price = response_json[token_symbol_to_name_mapping["WFX"]]["usd"] / fxg_per_fx
        else:
            token_a_price = response_json[token_symbol_to_name_mapping[lp_token_a_symbol]]["usd"]
        
        if lp_token_b_symbol == "FXG":
            token_b_price =  response_json[token_symbol_to_name_mapping["WFX"]]["usd"] / fxg_per_fx
        else:
            token_b_price = response_json[token_symbol_to_name_mapping[lp_token_b_symbol]]["usd"]
        
        # we are checking if a token is a 6 decimal coin
        if lp_token_a_symbol in six_decimal_tokens:
            token_a_price *= 10 ** 12
        if lp_token_b_symbol in six_decimal_tokens:
            token_b_price *= 10 ** 12
        lp_token_value_per_coin = (
                                          lp_token_a_balance * token_a_price + lp_token_b_balance * token_b_price) / lp_token_total_supply
        # below is same as: lp_token_value_per_coin * lp_token_deposited / 10*18
        tvl = web3.fromWei(lp_token_value_per_coin * lp_token_deposited, "ether")
        if tvl == 0 or total_alloc_point == 0:
            apr = ""
            apy_daily = ""
            apy_monthly = ""
        else:
            wfx_price = response_json[token_symbol_to_name_mapping["WFX"]]["usd"]
            if rewarder_address == "0x0000000000000000000000000000000000000000":
                apr = (
                          (
                                  15000
                                  * 365
                                  * bonus_multiplier
                                  * alloc_point
                                  * web3.fromWei(reward_per_block, "ether")
                                  * decimal.Decimal(wfx_price)
                          )
                          / (tvl * total_alloc_point)
                  ) * 100
            else:
                # this is only for FXG and BAVA (2023-03-13)
                # this is only for pools that have BONUS tokens
                print(1)
                rewarder_contract = web3.eth.contract(address=rewarder_address, abi=rewarderAbi)
                reward_Multiplier = rewarder_contract.functions.getRewardMultipliers().call()
                reward_Token = rewarder_contract.functions.getRewardTokens().call()
                rewardTokenAmount = (
                                  15000
                                  * 365
                                  * bonus_multiplier
                                  * alloc_point
                                  * web3.fromWei(reward_per_block, "ether")
                          )
                rewardValue = rewardTokenAmount * decimal.Decimal(wfx_price)
                for token_index in range(len(reward_Token)):
                    reward_token_contract = web3.eth.contract(address=reward_Token[token_index], abi=lpAbi)
                    reward_token_symbol = reward_token_contract.functions.symbol().call()
                    print("reward_token_symbol", reward_token_symbol)
                    if reward_token_symbol == "BAVA":
                        rewardValue += web3.fromWei(reward_Multiplier[token_index], 'ether') * rewardTokenAmount * decimal.Decimal(response_json["baklava"]["usd"])
                    else:
                        rewardValue += web3.fromWei(reward_Multiplier[token_index], 'ether') * rewardTokenAmount * decimal.Decimal(response_json[reward_token_symbol]["usd"])
                apr = rewardValue / (tvl * total_alloc_point) * 100
                    
            apy_daily = ((1 + apr / 36500) ** 365 - 1) * 100
            apy_weekly = ((1 + apr / 5200) ** 52 - 1) * 100
            apy_monthly = ((1 + apr / 1200) ** 12 - 1) * 100

        lp_token_pair_symbol = pool_index
        tvl_dict["tvl"][lp_token_pair_symbol] = str(tvl)
        apr_dict["apr"][lp_token_pair_symbol] = str(apr)
        apy_daily_dict["apyDaily"][lp_token_pair_symbol] = str(apy_daily)
        lp_token_value_per_coin_dict["lpTokenValue"][lp_token_pair_symbol] = str(lp_token_value_per_coin)

        print("Apr", apr, "APY", apy_daily)

    # **************************************** Update data *******************

    with open("TVL.json", "w") as tvl_file:
        json.dump(tvl_dict, tvl_file, indent=4)

    with open("APR.json", "w") as apr_file:
        json.dump(apr_dict, apr_file, indent=4)

    with open("APYDaily.json", "w") as apy_file:
        json.dump(apy_daily_dict, apy_file, indent=4)

    with open("LpTokenValue.json", "w") as lpTokenValue_file:
        json.dump(lp_token_value_per_coin_dict, lpTokenValue_file, indent=4)

    with open("AllData.json", "w") as allData_file:
        all_data_file = {**tvl_dict, **apr_dict, **apy_daily_dict, **lp_token_value_per_coin_dict}
        json.dump(all_data_file, allData_file, indent=4)


##########################################################################
# Update and Retreive BDL Total and Past 30 Days Amount from MongoDB
##########################################################################


def connectDB():
    # CONNECTION_STRING = "mongodb+srv://"+mongoDBUser+":"+mongoDBPW+"@pundix.ruhha.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    CONNECTION_STRING = (
            "mongodb+srv://" +
            mongoDBUser +
            ":" +
            urllib.parse.quote(mongoDBPW) +
            "@cluster0.9ibig9g.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    # s = MongoClient("mongodb+srv://"+mongoDBUser+":"+urllib.parse.quote(mongoDBPW)+"@cluster0.adqfx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=certifi.where())
    client = MongoClient(CONNECTION_STRING, tls=True,
                         tlsAllowInvalidCertificates=True)
    return client["TVLAmount_Prod"]  # name of the db


def updateDB():
    dbName = connectDB()  # get the database

    collectionName21 = dbName["All_Prod"]  # name of collection

    with open("AllData.json") as allData:
        data21 = json.load(allData)
        collectionName21.delete_many({})
        # if isinstance(data21, list):
        #     collectionName21.insert_many(data21)
        # else:
        #     collectionName21.insert_one(data21)
        collectionName21.insert_one(data21)


##########################################################################
# Read mongo database
##########################################################################


# This is actually not ran.

def getDB():
    dbName = connectDB()
    collectionName1 = dbName["TVL"]
    collectionName2 = dbName["APR"]
    collectionName3 = dbName["APYDaily"]
    print("done")

    cursor1 = collectionName1.find({})
    for data1 in cursor1:
        tvl = data1["tvl"]

    cursor2 = collectionName2.find({})
    for data2 in cursor2:
        apr = data2["apr"]

    cursor3 = collectionName3.find({})
    for data3 in cursor3:
        apy = data3["apyDaily"]


# ######################################################################################
# Build flow function
# ######################################################################################


def minCheck():
    try:
        queryData()
        connectDB()
        updateDB()
        print("done query data")
    except Exception as e:
        print("MinCheck Error happen")
        logging.error(e)


# ######################################################################################
# Build schedule function
# ######################################################################################


def scheduleUpdate():
    schedule.every(3).minutes.do(minCheck)

    while True:
        schedule.run_pending()
        time.sleep(1)


# #############################################################################################################
# Main code
# #############################################################################################################


def main():
    queryData()
    print("done query data")
    connectDB()
    updateDB()
    # getDB()

    print("--- %s seconds ---" % (time.time() - start_time))
    scheduleUpdate()


# __name__ is a built-in variable in Python which evaluates to the name of
# the current module.
if __name__ == "__main__":
    main()
