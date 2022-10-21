import json
from web3 import Web3
from web3.logs import STRICT, IGNORE, DISCARD, WARN
from typing import List
from math import *
import pandas as pd
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

start_time = time.time()
#Connect Ethereum node 
# avarpc = "https://api.avax.network/ext/bc/C/rpc"
avarpc = "https://rpc.ankr.com/avalanche"
web3 = Web3(Web3.HTTPProvider(avarpc))
# web3 = Web3(Web3.WebsocketProvider(bscrpc))

print(web3.isConnected())
print(web3.eth.blockNumber)
latestBlk = web3.eth.blockNumber

full_path = os.getcwd()
# Load BAVAABI data
bavaJson = open(full_path+'/abi/'+'Bava.json')
bavaAbi = json.load(bavaJson)

# Load LpTokenABI data
lpJson = open(full_path+'/abi/'+'LpToken.json')
lpAbi = json.load(lpJson)

# Load bavaMasterFarmAbi data
bavaMasterFarmJson = open(full_path+'/abi/'+'BavaMasterFarm.json')
bavaMasterFarmAbi = json.load(bavaMasterFarmJson)
bavaMasterFarmV1Json = open(full_path+'/abi/'+'BavaMasterFarmV1.json')
bavaMasterFarmV1Abi = json.load(bavaMasterFarmV1Json)
bavaMasterFarmV2_2Json = open(full_path+'/abi/'+'BavaMasterFarmV2_2.json')
bavaMasterFarmV2_2Abi = json.load(bavaMasterFarmV2_2Json)
bavaMasterFarmV2_3Json = open(full_path+'/abi/'+'BavaMasterFarmV2_3.json')
bavaMasterFarmV2_3Abi = json.load(bavaMasterFarmV2_3Json)
bavaCompoundPoolJson = open(full_path+'/abi/'+'BavaCompoundPool.json')
bavaCompoundPoolAbi = json.load(bavaCompoundPoolJson)

# Load Pool data
farmJson = open(full_path+'/farm/'+'farm.json')
farm = json.load(farmJson)
farmV1Json = open(full_path+'/farm/'+'farmV1.json')
farmV1 = json.load(farmV1Json)
farmV2_2Json = open(full_path+'/farm/'+'farmV2_2.json')
farmV2_2 = json.load(farmV2_2Json)
farmV2_3Json = open(full_path+'/farm/'+'farmV2_3.json')
farmV2_3 = json.load(farmV2_3Json)

bavaPGL = '0xeB69651B7146F4A42EBC32B03785C3eEddE58Ee7'
bavaAddress = '0xe19A1684873faB5Fb694CfD06607100A632fF21c'
avaxAddress = '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7'
bavaContract = web3.eth.contract(address=bavaAddress, abi=bavaAbi["abi"])
avaxContract = web3.eth.contract(address=avaxAddress, abi=bavaAbi["abi"])

bavaMasterFarm = "0xb5a054312A73581A3c0FeD148b736911C02f4539"
bavaMasterFarmContract = web3.eth.contract(address=bavaMasterFarm, abi=bavaMasterFarmAbi["abi"])
bavaMasterFarmV1 = "0x221C6774CF60277b36D21a57A31154EC96299d50"
bavaMasterFarmContractV1 = web3.eth.contract(address=bavaMasterFarmV1, abi=bavaMasterFarmV1Abi["abi"])
bavaMasterFarmV2_2 = "0xfD6b09A76f81c83F7E7D792070Ab2A05550F887C"
bavaMasterFarmContractV2_2 = web3.eth.contract(address=bavaMasterFarmV2_2, abi=bavaMasterFarmV2_2Abi["abi"])
bavaMasterFarmV2_3 = "0x25Fc2D200F31485A58AE704403316791e65fAB0E"
bavaMasterFarmContractV2_3 = web3.eth.contract(address=bavaMasterFarmV2_3, abi=bavaMasterFarmV2_3Abi["abi"])

totalSupply = bavaContract.functions.totalSupply().call(block_identifier= 'latest')
print("......")
bonusMultiplier = 206
load_dotenv()
infuraKey = os.getenv("INFURA_KEY")
mongoDBUser = os.getenv("MONGODB_USERNAME")
print(mongoDBUser)
mongoDBPW = os.getenv("MONGODB_PASSWORD")

# ##########################################################################################################
# Query ERC20 transfer event
# ##########################################################################################################
def queryData():
# receipt = web3.eth.get_transaction_receipt("0x59c4f19ea4a6af4876f617419b812248bae8c5d915db5b6cc67ded5ede7ff593")   # or use tx_hash deifined on above command line
# event = proxyContract.events.Transfer().processReceipt(receipt, errors= DISCARD)
    response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=joe%2Cwrapped-avax%2Cpangolin%2Cweth%2Cbaklava%2Cusd-coin%2Ctether%2Cbenqi%2Cterra-luna&vs_currencies=usd")
    responseJson = response.json()
    tokenPriceArray=[]

    AVAXPrice = responseJson["wrapped-avax"]["usd"]
    tokenPrice = {"avaxPrice":str(AVAXPrice)}
    tokenPriceArray.append(tokenPrice)

    BAVAPrice = responseJson["baklava"]["usd"]
    tokenPrice = {"bavaPrice":str(BAVAPrice)}
    tokenPriceArray.append(tokenPrice)

    PNGPrice = responseJson["pangolin"]["usd"]
    tokenPrice = {"pngPrice":str(PNGPrice)}
    tokenPriceArray.append(tokenPrice)

    LUNAPrice = responseJson["terra-luna"]["usd"]
    tokenPrice = {"lunaPrice":str(LUNAPrice)}
    tokenPriceArray.append(tokenPrice)

    WETHPrice = responseJson["weth"]["usd"]
    USDTPrice = responseJson["tether"]["usd"]
    USDCPrice = responseJson["usd-coin"]["usd"]

    JOEPrice = responseJson["joe"]["usd"]
    tokenPrice = {"joePrice":str(JOEPrice)}
    tokenPriceArray.append(tokenPrice)

    QIPrice = responseJson["benqi"]["usd"]
    tokenPrice = {"qiPrice":str(QIPrice)}
    tokenPriceArray.append(tokenPrice)

    tvlArray=[]
    aprArray=[]
    apyArray=[]
    returnRatioArray=[]
    lpTokenValueArray=[]

    tvlArrayV2_2=[]
    aprArrayV2_2=[]
    apyArrayV2_2=[]
    returnRatioArrayV2_2=[]
    lpTokenValueArrayV2_2=[]

    tvlArrayV2_3=[]
    aprArrayV2_3=[]
    apyArrayV2_3=[]
    returnRatioArrayV2_3=[]
    lpTokenValueArrayV2_3=[]

    bavatvlArray=[]
    bavaaprArray=[]
    bavaapyArray=[]
    bavalpTokenValueArray=[]


    rewardPerBlock = bavaMasterFarmContract.functions.REWARD_PER_BLOCK().call()
    rewardPerBlockV1 = bavaMasterFarmContractV1.functions.REWARD_PER_BLOCK().call()
    rewardPerBlockV2_2 = bavaMasterFarmContractV2_2.functions.REWARD_PER_BLOCK().call()
    rewardPerBlockV2_3 = bavaMasterFarmContractV2_3.functions.REWARD_PER_BLOCK().call()

    totalAllocPoint = bavaMasterFarmContract.functions.totalAllocPoint().call()
    totalAllocPointV1 = bavaMasterFarmContractV1.functions.totalAllocPoint().call()
    totalAllocPointV2_2 = bavaMasterFarmContractV2_2.functions.totalAllocPoint().call()
    totalAllocPointV2_3 = bavaMasterFarmContractV2_3.functions.totalAllocPoint().call()
    poolLength = bavaMasterFarmContract.functions.poolLength().call()
    poolLengthV1 = bavaMasterFarmContractV1.functions.poolLength().call()
    poolLengthV2_2 = bavaMasterFarmContractV2_2.functions.poolLength().call()
    poolLengthV2_3 = bavaMasterFarmContractV2_3.functions.poolLength().call()

    for x in range(poolLengthV2_3):
        event = farmV2_3["farm"][x]
        poolInfo = bavaMasterFarmContractV2_3.functions.poolInfo(event["pid"]).call()
        poolAddress = poolInfo[1]
        poolContract = web3.eth.contract(address=poolAddress, abi=bavaCompoundPoolAbi["abi"])
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpReceiptInContract = poolContract.functions.totalSupply().call()
        lpTokenInContract = (poolContract.functions.poolInfo().call())[1]

        if lpReceiptInContract == 0 :
            returnRatio = 1
        else:
            returnRatio = lpTokenInContract/lpReceiptInContract

        lpTokenTSupply = lpContract.functions.totalSupply().call()
        lpTokenABalanceContract = lpTokenA.functions.balanceOf(event["lpAddresses"]["43114"]).call()
        lpTokenBBalanceContract = lpTokenB.functions.balanceOf(event["lpAddresses"]["43114"]).call()

        if event["token"]["MAINNET"]["symbol"] == "BAVA" :
            tokenAPrice = BAVAPrice
        elif event["token"]["MAINNET"]["symbol"] == "AVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "PNG" :
            tokenAPrice = PNGPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDT.e") :
            tokenAPrice = USDTPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "WETH.e") :
            tokenAPrice = WETHPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDC.e") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "JOE") :
            tokenAPrice = JOEPrice
        elif (event["token"]["MAINNET"]["symbol"] == "QI") :
            tokenAPrice = QIPrice    

        if event["quoteToken"]["MAINNET"]["symbol"] == "BAVA" :
            tokenBPrice = BAVAPrice
        if event["quoteToken"]["MAINNET"]["symbol"] == "AVAX" :
            tokenBPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "PNG" :
            tokenBPrice = PNGPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDT.e" :
            tokenBPrice = USDTPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "WETH.e" :
            tokenBPrice = WETHPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDC.e" :
            tokenBPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "JOE" :
            tokenBPrice = JOEPrice
        elif (event["quoteToken"]["MAINNET"]["symbol"] == "QI") :
            tokenBPrice = QIPrice 

        lpTokenValue = ((lpTokenABalanceContract * tokenAPrice) + (lpTokenBBalanceContract * tokenBPrice)) / lpTokenTSupply
        if event["lpTokenPairsymbol"] == "XJOE" or event["lpTokenPairsymbol"] == "PNG" :
            tvl = web3.fromWei(tokenAPrice * lpTokenInContract, 'ether')
            lpTokenValue = tokenAPrice
        else:
            tvl = web3.fromWei(lpTokenValue * lpTokenInContract, 'ether')

        if tvl == 0 :
            apr = ""
            apyDaily = ""
            apyMonthly = ""
        else:
            apr = ((28000 * 365 * bonusMultiplier * event["allocPoint"] * web3.fromWei(rewardPerBlockV2_3, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPointV2_3)) * 100
            apyDaily = ((1 + apr/36500)**365 -1) * 100
            apyWeekly = ((1 + apr/5200)**52 -1) * 100
            apyMonthly = ((1 + apr/1200)**12 -1) * 100

        tvlV2_3 = {"tvl":str(tvl)}
        aprV2_3 = {"apr":str(apr)}
        apyDailyV2_3 = {"apyDaily":str(apyDaily)}
        returnRatioV2_3 = {"returnRatio":str(returnRatio)}
        lpTokenValueV2_3 = {"lpTokenValue":str(lpTokenValue)}

        tvlArrayV2_3.append(tvlV2_3)
        aprArrayV2_3.append(aprV2_3)
        apyArrayV2_3.append(apyDailyV2_3)
        returnRatioArrayV2_3.append(returnRatioV2_3)
        lpTokenValueArrayV2_3.append(lpTokenValueV2_3)


    for x in range(poolLength):
        event = farm["farm"][x]
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpTokenInContract = bavaMasterFarmContract.functions.poolInfo(event["pid"]).call()
        lpReceiptInContract = lpTokenInContract[5]
        lpTokenInContract = lpTokenInContract[4]
        returnRatio = lpTokenInContract/lpReceiptInContract

        lpTokenTSupply = lpContract.functions.totalSupply().call()
        lpTokenABalanceContract = lpTokenA.functions.balanceOf(event["lpAddresses"]["43114"]).call()
        lpTokenBBalanceContract = lpTokenB.functions.balanceOf(event["lpAddresses"]["43114"]).call()

        if event["token"]["MAINNET"]["symbol"] == "BAVA" :
            tokenAPrice = BAVAPrice
        elif event["token"]["MAINNET"]["symbol"] == "AVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "PNG" :
            tokenAPrice = PNGPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDT.e") :
            tokenAPrice = USDTPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "WETH.e") :
            tokenAPrice = WETHPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDC.e") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "JOE") :
            tokenAPrice = JOEPrice
        elif (event["token"]["MAINNET"]["symbol"] == "QI") :
            tokenAPrice = QIPrice    

        if event["quoteToken"]["MAINNET"]["symbol"] == "BAVA" :
            tokenBPrice = BAVAPrice
        if event["quoteToken"]["MAINNET"]["symbol"] == "AVAX" :
            tokenBPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "PNG" :
            tokenBPrice = PNGPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDT.e" :
            tokenBPrice = USDTPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "WETH.e" :
            tokenBPrice = WETHPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDC.e" :
            tokenBPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "JOE" :
            tokenBPrice = JOEPrice
        elif (event["quoteToken"]["MAINNET"]["symbol"] == "QI") :
            tokenBPrice = QIPrice 

        lpTokenValue = ((lpTokenABalanceContract * tokenAPrice) + (lpTokenBBalanceContract * tokenBPrice)) / lpTokenTSupply
        if event["lpTokenPairsymbol"] == "XJOE" or event["lpTokenPairsymbol"] == "PNG" :
            tvl = web3.fromWei(tokenAPrice * lpTokenInContract, 'ether')
            lpTokenValue = tokenAPrice
        else:
            tvl = web3.fromWei(lpTokenValue * lpTokenInContract, 'ether')

        apr = ((28000 * 365 * 281 * event["allocPoint"] * web3.fromWei(rewardPerBlock, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPoint)) * 100
        apyDaily = ((1 + apr/36500)**365 -1) * 100
        apyWeekly = ((1 + apr/5200)**52 -1) * 100
        apyMonthly = ((1 + apr/1200)**12 -1) * 100

        tvl = {"tvl":str(tvl)}
        apr = {"apr":str(apr)}
        apyDaily = {"apyDaily":str(apyDaily)}
        returnRatio={"returnRatio":str(returnRatio)}
        lpTokenValue={"lpTokenValue":str(lpTokenValue)}

        tvlArray.append(tvl)
        aprArray.append(apr)
        apyArray.append(apyDaily)
        returnRatioArray.append(returnRatio)
        lpTokenValueArray.append(lpTokenValue)

    for x in range(poolLengthV1):
        event = farmV1["farm"][x]
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpTokenInContract = bavaMasterFarmContractV1.functions.poolInfo(event["pid"]).call()
        lpTokenInContract = lpTokenInContract[4]

        lpTokenTSupply = lpContract.functions.totalSupply().call()
        lpTokenABalanceContract = lpTokenA.functions.balanceOf(event["lpAddresses"]["43114"]).call()
        lpTokenBBalanceContract = lpTokenB.functions.balanceOf(event["lpAddresses"]["43114"]).call()

        if event["token"]["MAINNET"]["symbol"] == "BAVA" :
            tokenAPrice = BAVAPrice
        elif event["token"]["MAINNET"]["symbol"] == "AVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "PNG" :
            tokenAPrice = PNGPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDT.e") :
            tokenAPrice = USDTPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "WETH.e") :
            tokenAPrice = WETHPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDC.e") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "JOE") :
            tokenAPrice = JOEPrice
        elif (event["token"]["MAINNET"]["symbol"] == "QI") :
            tokenAPrice = QIPrice    

        if event["quoteToken"]["MAINNET"]["symbol"] == "BAVA" :
            tokenBPrice = BAVAPrice
        if event["quoteToken"]["MAINNET"]["symbol"] == "AVAX" :
            tokenBPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "PNG" :
            tokenBPrice = PNGPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDT.e" :
            tokenBPrice = USDTPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "WETH.e" :
            tokenBPrice = WETHPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDC.e" :
            tokenBPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "JOE" :
            tokenBPrice = JOEPrice
        elif (event["quoteToken"]["MAINNET"]["symbol"] == "QI") :
            tokenBPrice = QIPrice 

        lpTokenValue = ((lpTokenABalanceContract * tokenAPrice) + (lpTokenBBalanceContract * tokenBPrice)) / lpTokenTSupply
        if event["lpTokenPairsymbol"] == "XJOE" or event["lpTokenPairsymbol"] == "PNG" :
            tvl = web3.fromWei(tokenAPrice * lpTokenInContract, 'ether')
            lpTokenValue = tokenAPrice
        else:
            tvl = web3.fromWei(lpTokenValue * lpTokenInContract, 'ether')

        if tvl == 0 :
            apr = ""
            apyDaily = ""
            apyMonthly = ""
        else:
            apr = ((28000 * 365 * bonusMultiplier * event["allocPoint"] * web3.fromWei(rewardPerBlockV1, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPointV1)) * 100
            apyDaily = ((1 + apr/36500)**365 -1) * 100
            apyWeekly = ((1 + apr/5200)**52 -1) * 100
            apyMonthly = ((1 + apr/1200)**12 -1) * 100

        bavatvl = {"tvl":str(tvl)}
        bavaapr = {"apr":str(apr)}
        bavaapyDaily = {"apyDaily":str(apyDaily)}
        bavalpTokenValue = {"lpTokenValue":str(lpTokenValue)}

        bavatvlArray.append(bavatvl)
        bavaaprArray.append(bavaapr)
        bavaapyArray.append(bavaapyDaily)
        bavalpTokenValueArray.append(bavalpTokenValue)

    for x in range(poolLengthV2_2):
        event = farmV2_2["farm"][x]
        lpContract = web3.eth.contract(address=event["lpAddresses"]["43114"], abi=lpAbi["abi"])
        lpTokenA = web3.eth.contract(address=event["token"]["MAINNET"]["address"], abi=lpAbi["abi"])
        lpTokenB = web3.eth.contract(address=event["quoteToken"]["MAINNET"]["address"], abi=lpAbi["abi"])

        lpTokenInContract = bavaMasterFarmContractV2_2.functions.poolInfo(event["pid"]).call()
        lpReceiptInContract = lpTokenInContract[5]
        lpTokenInContract = lpTokenInContract[4]
        if lpReceiptInContract == 0 :
            returnRatio = 1
        else: 
            returnRatio = lpTokenInContract/lpReceiptInContract

        lpTokenTSupply = lpContract.functions.totalSupply().call()
        lpTokenABalanceContract = lpTokenA.functions.balanceOf(event["lpAddresses"]["43114"]).call()
        lpTokenBBalanceContract = lpTokenB.functions.balanceOf(event["lpAddresses"]["43114"]).call()

        if event["token"]["MAINNET"]["symbol"] == "BAVA" :
            tokenAPrice = BAVAPrice
        elif event["token"]["MAINNET"]["symbol"] == "AVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "PNG" :
            tokenAPrice = PNGPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDT.e") :
            tokenAPrice = USDTPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "WETH.e") :
            tokenAPrice = WETHPrice
        elif (event["token"]["MAINNET"]["symbol"] == "USDC.e") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "JOE") :
            tokenAPrice = JOEPrice
        elif (event["token"]["MAINNET"]["symbol"] == "QI") :
            tokenAPrice = QIPrice    

        if event["quoteToken"]["MAINNET"]["symbol"] == "BAVA" :
            tokenBPrice = BAVAPrice
        if event["quoteToken"]["MAINNET"]["symbol"] == "AVAX" :
            tokenBPrice = AVAXPrice
        elif event["token"]["MAINNET"]["symbol"] == "sAVAX" :
            tokenAPrice = AVAXPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "PNG" :
            tokenBPrice = PNGPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDT.e" :
            tokenBPrice = USDTPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "WETH.e" :
            tokenBPrice = WETHPrice
        elif event["quoteToken"]["MAINNET"]["symbol"] == "USDC.e" :
            tokenBPrice = USDCPrice * 1000000000000
        elif (event["token"]["MAINNET"]["symbol"] == "USDC") :
            tokenAPrice = USDCPrice * 1000000000000
        elif event["quoteToken"]["MAINNET"]["symbol"] == "JOE" :
            tokenBPrice = JOEPrice
        elif (event["quoteToken"]["MAINNET"]["symbol"] == "QI") :
            tokenBPrice = QIPrice 

        lpTokenValue = ((lpTokenABalanceContract * tokenAPrice) + (lpTokenBBalanceContract * tokenBPrice)) / lpTokenTSupply
        if event["lpTokenPairsymbol"] == "XJOE" or event["lpTokenPairsymbol"] == "PNG" :
            tvl = web3.fromWei(tokenAPrice * lpTokenInContract, 'ether')
            lpTokenValue = tokenAPrice
        else:
            tvl = web3.fromWei(lpTokenValue * lpTokenInContract, 'ether')

        if tvl == 0 :
            apr = ""
            apyDaily = ""
            apyMonthly = ""
        else:
            apr = ((28000 * 365 * bonusMultiplier * event["allocPoint"] * web3.fromWei(rewardPerBlockV2_2, 'ether') * decimal.Decimal(BAVAPrice) ) / (tvl * totalAllocPointV2_2)) * 100
            apyDaily = ((1 + apr/36500)**365 -1) * 100
            apyWeekly = ((1 + apr/5200)**52 -1) * 100
            apyMonthly = ((1 + apr/1200)**12 -1) * 100

        tvlV2_2 = {"tvl":str(tvl)}
        aprV2_2 = {"apr":str(apr)}
        apyDailyV2_2 = {"apyDaily":str(apyDaily)}
        returnRatioV2_2 = {"returnRatio":str(returnRatio)}
        lpTokenValueV2_2 = {"lpTokenValue":str(lpTokenValue)}

        tvlArrayV2_2.append(tvlV2_2)
        aprArrayV2_2.append(aprV2_2)
        apyArrayV2_2.append(apyDailyV2_2)
        returnRatioArrayV2_2.append(returnRatioV2_2)
        lpTokenValueArrayV2_2.append(lpTokenValueV2_2)

# **************************************** Update data ******************************************************

    with open("TVL.json", 'w') as tvl_file:
        tvlFile = {"tvl":tvlArray}
        json.dump(tvlFile, tvl_file, indent=4)
    
    with open("APR.json", 'w') as apr_file:
        aprFile = {"apr":aprArray}
        json.dump((aprFile), apr_file, indent=4)

    with open("APYDaily.json", 'w') as apy_file:
        apyFile = {"apyDaily":apyArray}
        json.dump((apyFile), apy_file, indent=4)

    with open("ReturnRatio.json", 'w') as return_file:
        returnFile = {"returnRatio":returnRatioArray}
        json.dump(returnFile, return_file, indent=4) 

    with open("TVLV2_2.json", 'w') as tvl_file:
        tvlFile = {"tvl":tvlArrayV2_2}
        json.dump(tvlFile, tvl_file, indent=4)
    
    with open("APRV2_2.json", 'w') as apr_file:
        aprFile = {"apr":aprArrayV2_2}
        json.dump((aprFile), apr_file, indent=4)

    with open("APYDailyV2_2.json", 'w') as apy_file:
        apyFile = {"apyDaily":apyArrayV2_2}
        json.dump((apyFile), apy_file, indent=4)

    with open("ReturnRatioV2_2.json", 'w') as return_file:
        returnFile = {"returnRatio":returnRatioArrayV2_2}
        json.dump(returnFile, return_file, indent=4) 

    with open("BAVATVL.json", 'w') as bavatvl_file:
        bavatvlFile = {"tvl":bavatvlArray}
        json.dump(bavatvlFile, bavatvl_file, indent=4)
    
    with open("BAVAAPR.json", 'w') as bavaapr_file:
        bavaaprFile = {"apr":bavaaprArray}
        json.dump(bavaaprFile, bavaapr_file, indent=4)

    with open("BAVAAPYDaily.json", 'w') as bavaapy_file:
        bavaapyFile = {"apyDaily":bavaapyArray}
        json.dump(bavaapyFile, bavaapy_file, indent=4)

    with open("TVLV2_3.json", 'w') as tvl_file:
        tvlFile = {"tvl":tvlArrayV2_3}
        json.dump(tvlFile, tvl_file, indent=4)
    
    with open("APRV2_3.json", 'w') as apr_file:
        aprFile = {"apr":aprArrayV2_3}
        json.dump((aprFile), apr_file, indent=4)

    with open("APYDailyV2_3.json", 'w') as apy_file:
        apyFile = {"apyDaily":apyArrayV2_3}
        json.dump((apyFile), apy_file, indent=4)

    with open("ReturnRatioV2_3.json", 'w') as return_file:
        returnFile = {"returnRatio":returnRatioArrayV2_3}
        json.dump(returnFile, return_file, indent=4) 

    with open("LpTokenValue.json", 'w') as lpTokenValue_file:
        lpTokenValueFile = {"lpTokenValue":lpTokenValueArray}
        json.dump(lpTokenValueFile, lpTokenValue_file, indent=4) 

    with open("LpTokenValueV2_2.json", 'w') as lpTokenValue_file:
        lpTokenValueFile = {"lpTokenValue":lpTokenValueArrayV2_2}
        json.dump(lpTokenValueFile, lpTokenValue_file, indent=4) 

    with open("LpTokenValueV2_3.json", 'w') as lpTokenValue_file:
        lpTokenValueFile = {"lpTokenValue":lpTokenValueArrayV2_3}
        json.dump(lpTokenValueFile, lpTokenValue_file, indent=4) 

    with open("BAVALpTokenValue.json", 'w') as lpTokenValue_file:
        lpTokenValueFile = {"lpTokenValue":bavalpTokenValueArray}
        json.dump(lpTokenValueFile, lpTokenValue_file, indent=4) 

    with open("AllData.json", 'w') as allData_file:
        allDataFile = {"TVL":tvlArray, "TVLV2_2":tvlArrayV2_2, "TVLV2_3":tvlArrayV2_3, "Bavatvl":bavatvlArray, "APR":aprArray, "APRV2_2":aprArrayV2_2, "APRV2_3":aprArrayV2_3, "BavaAPR":bavaaprArray, "ApyDaily":apyArray, "ApyDailyV2_2":apyArrayV2_2, "ApyDailyV2_3":apyArrayV2_3, "BavaApyDaily":bavaapyArray, "ReturnRatio":returnRatioArray, "ReturnRatioV2_2":returnRatioArrayV2_2, "ReturnRatioV2_3":returnRatioArrayV2_3, "LpTokenValue":lpTokenValueArray, "LpTokenValueV2_2":lpTokenValueArrayV2_2, "LpTokenValueV2_3":lpTokenValueArrayV2_3, "BavaLpTokenValue":bavalpTokenValueArray }
        json.dump(allDataFile, allData_file, indent=4) 

##############################################################################################################
# Update and Retreive BDL Total and Past 30 Days Amount from MongoDB
##############################################################################################################

def connectDB():
    # CONNECTION_STRING = "mongodb+srv://"+mongoDBUser+":"+mongoDBPW+"@pundix.ruhha.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    CONNECTION_STRING = "mongodb+srv://"+mongoDBUser+":"+urllib.parse.quote(mongoDBPW)+"@cluster0.adqfx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    # s = MongoClient("mongodb+srv://"+mongoDBUser+":"+urllib.parse.quote(mongoDBPW)+"@cluster0.adqfx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=certifi.where())
    client = MongoClient(CONNECTION_STRING,tls=True, tlsAllowInvalidCertificates=True)
    return client['TVLAmount']

def updateDB():
    dbName = connectDB()
    
    collectionName21 = dbName["All"]

    with open("AllData.json") as allData:
        data21 = json.load(allData)
        collectionName21.delete_many({})
        if isinstance(data21, list):
            collectionName21.insert_many(data21)  
        else:
            collectionName21.insert_one(data21)

##############################################################################################################
# Read mongo database
##############################################################################################################


def getDB():
    dbName = connectDB() 
    collectionName1 = dbName["TVL"]
    collectionName2 = dbName["APR"]
    collectionName3 = dbName["APYDaily"]

    collectionName5 = dbName["BAVATVL"]
    collectionName6 = dbName["BAVAAPR"]
    collectionName7 = dbName["BAVAAPYDaily"]
    collectionName8 = dbName["ReturnRatio"]
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

    cursor5 = collectionName5.find({})
    for data5 in cursor5:
        tvl = data5["tvl"]

    cursor6 = collectionName6.find({})
    for data6 in cursor6:
        apr = data6["apr"]
        
    cursor7 = collectionName7.find({})
    for data7 in cursor7:
        apy = data7["apyDaily"]

    cursor8 = collectionName8.find({})
    for data8 in cursor8:
        returnRatio = data8["returnRatio"]

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

if __name__ == "__main__":     # __name__ is a built-in variable in Python which evaluates to the name of the current module.
    main()