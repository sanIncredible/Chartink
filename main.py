# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas as pd
import requests
import json
import schedule
from datetime import datetime, timedelta , time
import time as sleeper
import numpy as np
import jinja2
import dataframe_image
import traceback


gapUpCaption="Gap Up Stocks for Today"
pdhCaption="Gap Up Stocks with LTP>PDH"
opLowCaption="Gap Up Stocks with OpenLow"
gapMcCaption="Gap Up and MC"
gapTinyCaption="Gap Up and TinyIc"
newHighCaption="Gap Up and newHigh"
orbCaption="Gap up and ORB"

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
url="http://www.nseindia.com/api/market-data-pre-open?key=NIFTY&json=true"
url1="http://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050";

pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 500)

styles = [dict(selector="caption",
    props=[("text-align", "center"),
    ("font-size", "120%"),
    ("color", 'black')])]

proxies={'http':'http://78.141.212.200:8888'}
def telegram_bot_sendtext(bot_message,title):
    print("Blocked text message")

    # bot_message=bot_message.replace('&','-')
    # bot_message= datetime.now().strftime("%m/%d/%Y, %H:%M:%S").__str__() +"\n\n"+ title + "\n\n" + bot_message
    # bot_token='1323516991:AAEmL6Yd4Z8GW990YOsRTqwJT0hx-wawBYo'
    # bot_chatID='@NimblrORB'
    # send_text='https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    # response=requests.get(send_text)


def telegram_bot_send_image(image):
    url = "https://api.telegram.org/bot1323516991:AAEmL6Yd4Z8GW990YOsRTqwJT0hx-wawBYo/sendPhoto";
    files = {'photo': open(image, 'rb')}
    data = {'chat_id': "@NimblrORB"}
    r = requests.post(url, files=files, data=data)

gapUpStocksList=[]
pdhDict={}
openlow=[]
backupStocksl=1
ORB=1
def get_pdh_stock():
    try:
        with requests.session() as ses:
            ses.get("http://nseindia.com", headers=headers, timeout=30)
            pdhdata = ses.get(url1.strip(), headers=headers, timeout=30).json()
            pdhdatan=pd.json_normalize(pdhdata,record_path='data')
            pdhfil=pd.DataFrame.filter(pdhdatan,["symbol","dayHigh"])
            #pdhfil.to_json("pdh.json",orient="index")

            gpdfjson = pd.read_json('pdh.json')
            dd = gpdfjson.to_dict()
            print(dd)
            global pdhDict
            for i in dd:
                pdhDict[dd[i]['symbol']] = dd[i]['dayHigh']
    except:
        print("Exception in pdh parsing")
        print(traceback.print_exc())

def get_gapup_stocks():
    try:
        with requests.session() as sess:
            sess.get("https://nseindia.com", headers=headers, timeout=2)
            fetchedData = sess.get(url.strip(), headers=headers, timeout=2).json()
            rawData = pd.json_normalize(fetchedData, record_path='data')
            stocks = pd.DataFrame.filter(rawData, ["metadata.symbol", "metadata.previousClose", "metadata.lastPrice",
                                               "metadata.pChange"])
            gapUpStocks = stocks[stocks["metadata.pChange"] > 0]
            stockList = gapUpStocks["metadata.symbol"]
            global gapUpStocksList
            gapUpStocksList=stockList
            stocks['gapUp'] = np.where(stocks['metadata.pChange'] > 0, 'Y', 'N')
            gpm = stocks[stocks['gapUp'] != 'N']
            if not gpm.empty:
                gpm=gpm.drop(['gapUp'],axis=1)
                gpm=gpm.rename(columns={"metadata.symbol":"symbol", "metadata.previousClose":"previousClose","metadata.lastPrice":"lastPrice","metadata.pChange":"pChange"})
                gpm=gpm.reset_index(drop=True)
                gpm=gpm.style.set_caption(gapUpCaption).set_table_styles(styles)
                #telegram_bot_sendtext(gapUpStocksList.to_string(),"GapUpStocks PreOpenMarket")
                dataframe_image.export(gpm, "b.png")
                telegram_bot_send_image("b.png")

    except:
        print("Exception in gapup stocks parsing")
        print(traceback.print_exc())



def get_live_data():
    with requests.session() as sessi:
        curtimee=datetime.now().time()
        global backupStocksl
        try:
            if  curtimee >= time(9,15,0) and curtimee <=time(15,30,0):
                sessi.get("https://nseindia.com", headers=headers, timeout=2)
                fetchedLData = sessi.get(url1.strip(), headers=headers, timeout=2).json()
                rawLData = pd.json_normalize(fetchedLData, record_path='data')
                stocksL = pd.DataFrame.filter(rawLData, ["symbol","open","dayHigh","dayLow","lastPrice", "pChange"])
                stocksL = stocksL[stocksL["symbol"].isin(gapUpStocksList)]

                if(not isinstance(backupStocksl,int)):
                    stocksL=stocksL.sort_values(by=['symbol']).reset_index(drop=True)
                    backupStocksl=backupStocksl.sort_values(by=['symbol']).reset_index(drop=True)


                    stocksL['newH']=np.where(stocksL['dayHigh']>backupStocksl['dayHigh'],'Y','N')
                    nH = stocksL[stocksL['newH'] != 'N']
                    if not nH.empty:
                        nH = nH.drop(['newH','oplow','gapMC','tinyIc','ORB'],axis=1,errors='ignore')
                        nH=nH.style.set_caption(newHighCaption).set_table_styles(styles)
                        #telegram_bot_sendtext(nH.to_string(), "newHigh")
                        dataframe_image.export(nH, "cc.png")
                        telegram_bot_send_image("cc.png")



                #### filter ltp greater than pdh
                uidf = pd.DataFrame(list(pdhDict.items()), columns=['symbol', 'pdh'])
                ltpgpdh = stocksL.merge(uidf, on=['symbol'], how='inner')
                ltpgpdh=ltpgpdh[ltpgpdh['lastPrice'] > ltpgpdh['pdh']]
                if not ltpgpdh.empty:
                    ltpgpdh = ltpgpdh.drop(['newH','oplow','gapMC','tinyIc','ORB'],axis=1,errors='ignore')
                    ltpgpdh=ltpgpdh.reset_index(drop=True)
                    ltpgpdh=ltpgpdh.style.set_caption(pdhCaption).set_table_styles(styles)
                    #telegram_bot_sendtext(ltpgpdh.to_string(),"LTP greater than PDH")
                    dataframe_image.export(ltpgpdh, "c.png")
                    telegram_bot_send_image("c.png")


            #### filter open = low stocks
                stocksL['oplow']=np.where(stocksL['lastPrice']>stocksL['open'],'Y','N')
                opl=stocksL[stocksL['oplow']!='N']
                opl=opl.reset_index(drop=True)

                if not opl.empty:
                    opl = opl.drop(['newH','oplow','gapMC','tinyIc','ORB'],axis=1,errors='ignore')
                    opl=opl.style.set_caption(opLowCaption).set_table_styles(styles)
                    #telegram_bot_sendtext(stocksL[stocksL['oplow']!='N']['symbol'].to_string(),"Open is equal to Low")
                    dataframe_image.export(opl,"d.png")
                    telegram_bot_send_image("d.png")

                stocksL['gapMC'] = np.where((abs((stocksL['open'] - stocksL['lastPrice'])) / (stocksL['dayHigh'] - stocksL['dayLow'])) >= .9, 'Y', 'N')
                gpmor = stocksL[stocksL['gapMC'] != 'N']
                gpmor=gpmor.reset_index(drop=True)

                if not gpmor.empty:
                    gpmor = gpmor.drop(['newH','oplow','gapMC','tinyIc','ORB'],axis=1,errors='ignore')
                    gpmor=gpmor.style.set_caption(gapMcCaption).set_table_styles(styles)
                    #telegram_bot_sendtext(gpmor.to_string(), "GapUp and Morubozu")
                    if curtimee >= time(9, 15, 0) and curtimee <= time(9, 35, 0):
                        dataframe_image.export(gpmor, "e.png")
                        telegram_bot_send_image("e.png")

                stocksL['tinyIc'] = np.where((abs((stocksL['dayHigh']-stocksL['dayLow']))*100/stocksL['open'])<1.5,"Y",'N')
                tinym = stocksL[stocksL['tinyIc']!='N']
                tinym = tinym[tinym['gapMC']!='Y']
                tinym=tinym.reset_index(drop=True)

                if not tinym.empty:
                    tinym=tinym.drop(['newH','oplow','gapMC','tinyIc','ORB'],axis=1,errors='ignore')
                    tinym=tinym.style.set_caption(gapTinyCaption).set_table_styles(styles)
                    #telegram_bot_sendtext(tinym.to_string(), "Gapup and TinyIc")
                    if curtimee >= time(9, 15, 0) and curtimee <= time(9, 35, 0):
                        dataframe_image.export(tinym, "f.png")
                        telegram_bot_send_image("f.png")
                global ORB
                if curtimee >= time(9, 30, 0) and curtimee <= time(9, 32, 0):
                    ORB=stocksL

                if curtimee >= time(9, 32, 0) and (not isinstance(ORB,int)):
                    stocksL = stocksL.sort_values(by=['symbol']).reset_index(drop=True)
                    ORB = ORB.sort_values(by=['symbol']).reset_index(drop=True)

                    stocksL['ORB'] = np.where(stocksL['lastPrice'] > ORB['dayHigh'], 'Y', 'N')
                    orbL = stocksL[stocksL['ORB'] != 'N']
                    if not orbL.empty:
                        orbL = orbL.drop(['newH', 'oplow', 'gapMC', 'tinyIc','ORB'], axis=1, errors='ignore')
                        orbL = orbL.style.set_caption(orbCaption).set_table_styles(styles)
                        # telegram_bot_sendtext(nH.to_string(), "newHigh")
                        dataframe_image.export(orbL, "orcc.png")
                        telegram_bot_send_image("orcc.png")

                if not stocksL.empty:
                    backupStocksl=stocksL
            else:
                print("Market closed!!!!!")
        except:
            print(traceback.print_exc())



if __name__ == '__main__':
    get_pdh_stock()
    #schedule.every().days.at("21:21").do(get_pdh_stock)
    schedule.every().days.at("09:13").do(get_gapup_stocks)
    get_gapup_stocks()
    schedule.every(1).minutes.do(get_live_data)
    #get_pdh_stock()
    #get_live_data()





while True:
    schedule.run_pending()
    sleeper.sleep(1)

