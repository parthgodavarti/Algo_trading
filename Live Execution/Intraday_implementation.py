from SmartApi import SmartConnect
import pandas as pd
import pyotp
import datetime
import holidays
import requests
import telebot
from logzero import logger
import time
import numpy as np
import os

def token_list():
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    response = requests.get(url)
    data = response.json()

    # Convert the JSON data into a list of dictionaries
    data_list = []
    for item in data:
        data_dict = {}
        for key, value in item.items():
            data_dict[key] = value
        data_list.append(data_dict)

    # Create a Pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(data_list)

    # List of trading symbols
    trading_symbols = [
      "adanipower","adanient","adanigreen","adaniports","apollohosp","atgl","axisbank",
      "bajaj-auto","bajfinance","biocon","boschltd","britannia","cipla","colpal",
      "divislab","dmart","drreaddy","eichermot","gland","grasim",
      "havells","hdfcamc","heromotoco","hindunilvr","icicigi","indigo","ltim",
      "marico","maruti","mcdowell-n","motherson","mphasis","muthootfin","naukri","nestleind","ntpc","ongc","paytm",
      "pghh","pidilitind","piind","powergrid","reliance","sail",
      "sbicard","sbilife","sbin","shreecem",
      "siemens","srf","sunpharma","tataconsum","tatamotors","tatapower",
      "tatasteel","tcs","techm","titan",
      "torntpharm","ultracemco","upl","vedl"]

    # Create an empty DataFrame to store filtered data
    filtered_data = pd.DataFrame(columns=['token', 'name'])

    # Filter the DataFrame based on trading symbols
    for symbol in trading_symbols:
        symbol_upper = symbol.upper() + '-EQ'
        data = df[df['symbol'] == symbol_upper][['token', 'name']]
        filtered_data = pd.concat([filtered_data, data], ignore_index=True)

    # Create a dictionary to store symbol and token pairs
    symbol_token_dict = dict(zip(filtered_data['name'], filtered_data['token']))

    return symbol_token_dict

def sendMessage(message_text):
    try:
        bot = telebot.TeleBot('Your Bot details')
        chat_id ='Your Chat ID'
        bot.send_message(chat_id,message_text)
    except telebot.apihelper.ApiTelegramException as e:
        print(f"Failed to send message: {e.description}")

def is_indian_trading_day(date):
  if date.isoweekday() in [6, 7]:
    return False

  # Check if the date is a holiday.
  india_holidays = holidays.India()
  if india_holidays.get(date):
    return False

  return True

def historical_data(token):
    now = datetime.datetime.now()
    previous_trading_day = now - datetime.timedelta(days=1)
    start_date = previous_trading_day - datetime.timedelta(days=30)
    end_date = previous_trading_day
    start_time = "09:00"
    end_time = "15:30"
    start_timestamp = start_date.strftime("%Y-%m-%d %H:%M")
    end_timestamp = end_date.strftime("%Y-%m-%d %H:%M")
    try:
        historicParam={
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_MINUTE",
        "fromdate": start_timestamp,
        "todate": end_timestamp
        }
        data=smartApi.getCandleData(historicParam)
        data = pd.DataFrame(data["data"], columns=["datetime", "open", "high", "low", "close", "volume"])
        data['datetime'] = data['datetime'].apply(lambda x: x.replace("T",' '))
        data['datetime'] = data['datetime'].apply(lambda x: x.split("+")[0])
        return data
    except Exception as e:
        print("Historic Api failed: {}".format(e.message))

def macd_crossover_strategy(symbol, df):
    return symbol,False
    "This part of the code calculated my strategy and passes True or False based on that order is placed or is checked for exit criteria"
    
def place_trade(symbol, token, quantity):
    try:    
        entry_price = smartApi.get_ltp('NSE', f'{symbol}-EQ', token)['data']['close']
        stoploss_price = entry_price * 0.95
        target_price = entry_price * 1.1

        order_params = {
            "variety": "STOPLOSS_LIMIT",  # Use STOPLOSS_LIMIT or STOPLOSS_MARKET
            "tradingsymbol": f'{symbol}-EQ',
            "symboltoken": token,  # Include the API token
            "transactiontype": "BUY",
            "exchange": "NSE",
            "ordertype": "STOPLOSS_LIMIT",  # Changed from MARKET
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": target_price,  # Specify target price for STOPLOSS_LIMIT
            "triggerprice": stoploss_price,  # Specify trigger price
            "squareoff": target_price,
            "stoploss": stoploss_price,
            "quantity": quantity
        }

        orderId = smartApi.placeOrder(order_params)
        print("The order id is: {}".format(orderId))
        sendMessage("Order Placed" + symbol)

    except Exception as e:
        sendMessage("Order failed" + symbol)
        print("Order placement failed: {}".format(e.message))
        pass

      
def cancelorder(symbol, token,quantity):
    # Assuming you've retrieved the position quantity
    order_params = {
        "variety": "NORMAL",  # Adjust if needed
        "tradingsymbol": symbol + "-EQ",
        "symboltoken": token,
        "transactiontype": "SELL",
        "exchange": "NSE",  # Adjust if needed
        "ordertype": "MARKET",  # Or other suitable order type
        "producttype": "INTRADAY",  # Adjust if needed
        "duration": "DAY",  # Adjust if needed
        "price": "0",  # For MARKET order
        "quantity": quantity
    }

    response = smartApi.placeOrder(order_params)  # Replace with the actual function

    if response.status_code == 200:
        print("Trade exited successfully:", symbol)
        sendMessage("Trade Exited" + symbol)
    else:
        print("Error exiting trade:", response.status_code)
        sendMessage("Trade Couldn't Exit" + symbol)


def get_quantity(symbol):
    budget = float(smartApi.rmsLimit()['data']['availablecash'])
    price_history = historical_data(symbols_dict[symbol])
    price_history = price_history['close']
    standard_deviation = np.std(price_history)
    
    open_price = smartApi.ltpData('NSE', f'{symbol}-EQ', symbols_dict[symbol])['data']['close']
    print(budget, open_price)

    # Initial calculation
    quantity = int(budget / open_price)

    if standard_deviation < 0.1:
        quantity = int(quantity * 1.5)
    elif standard_deviation < 0.2:s
        quantity = int(quantity * 1.25)
    elif standard_deviation < 0.3:
        # No need to modify quantity in this case
        pass
    else:
        quantity = int(quantity * 0.75)

    sendMessage(symbol+"quantity done")

    return quantity
  

def login():
    api_key = ''
    clientId = ''
    pwd = ''
    token = ""
    totp=pyotp.TOTP(token).now()
    smartApi = SmartConnect(api_key)
    # login api call
    data = smartApi.generateSession(clientId, pwd,totp)
    feedToken = smartApi.getfeedToken()
    # print(data)
    authToken = data['data']['jwtToken']
    refreshToken = data['data']['refreshToken']
    res = smartApi.getProfile(refreshToken)
    smartApi .generateToken(refreshToken)
    res=res['data']['clientcode']
    print(res)
    sendMessage('Login Successful')
    return smartApi,feedToken,authToken

def main():
    try:
        global Trade_data
        Trade_data = pd.DataFrame(columns=['symbol', 'quantity', 'stoploss', 'target'])
        while True:
            now = datetime.datetime.now()
            current_time = now.time()
            global symbols_dict
            symbols_dict = token_list()
            trading_start_time = datetime.time(9, 15)
            trading_end_time = datetime.time(15, 30)

            if trading_start_time <= current_time <= trading_end_time:
                global smartApi,feedToken,authToken
                smartApi,feedToken,authToken = login()
                
                for symbol in symbols_dict:
                    df = pd.read_csv('/home/parth/Python/my_data.csv')ss
                    signal_data = df[df['symbol'] == symbols_dict[symbol]]
                    signal_data['datetime'] = pd.to_datetime(signal_data['datetime'])
                    signal_data.set_index('datetime', inplace=True)
                    tokens=symbols_dict[symbol]
                    history_data = historical_data(tokens)
                    history_data['datetime'] = pd.to_datetime(history_data['datetime'])
                    history_data.set_index('datetime', inplace=True)
                    
                    minute_data = pd.concat([history_data, signal_data])
                    sendMessage(f'Historical data done for {symbol}')
                    
                    symbol, signal = macd_crossover_strategy(symbol, minute_data)
                    sendMessage(f'Trade Signal for {symbol}: {signal}')
                    if signal==True and current_time >= datetime.time(9, 15, 0):
                        if Trade_data.empty or symbol not in Trade_data['symbol'].values:
                            sendMessage('Entered the order placing loop')
                            token = symbols_dict[symbol]
                            Temp_log = pd.DataFrame(columns=['symbol', 'quantity', 'stoploss', 'target'])
                            quantity = get_quantity(symbol)
                            place_trade(symbol, token, quantity)
                            price = smartApi.ltpData('NSE', f'{symbol}-EQ', token)['data']['close']
                            stoploss_price = price * (1 - (1 / 100))
                            target_price = price * (1 + (2 / 100))
                            Temp_log = {'symbol': symbol, 'quantity': quantity, 'stoploss': stoploss_price, 'target': target_price}
                            Temp_log = pd.DataFrame([Temp_log])
                            Trade_data = pd.concat([Trade_data, Temp_log])
                            sendMessage(f'Order placed for {symbol} ({quantity} quantity)')
                    else:
                        if symbol in Trade_data['symbol'].values:
                            sendMessage('Entered order exiting part')
                            trade_row = Trade_data[Trade_data['symbol'] == symbol].iloc[0]
                            quantity = trade_row['quantity']
                            stop_loss = trade_row['stoploss']
                            target = trade_row['target']
                            current_time = datetime.datetime.now().time()
                            ltp = smartApi.ltpData('NSE', f'{symbol}-EQ', symbols_dict[symbol])['data']['close']
                            if current_time >= datetime.time(15, 30, 0) or ltp <= stop_loss or ltp >= target:
                                #cancelorder(symbol, symbols_dict[symbol], quantity)
                                sendMessage(f'Order Exited for {symbol} ({quantity} quantity)')

                                    # Remove the symbol from the DataFrame
                                Trade_data = Trade_data[Trade_data['symbol'] != symbol]
                sendMessage('Sleeping now')
                time.sleep(60)
            else:
                time.sleep(120)

    except Exception as e:
        sendMessage(f"Error in order placement and monitoring loop: {str(e)}")
        print(f"Error in order placement and monitoring loop: {str(e)}")
        time.sleep(60)

if __name__ == "__main__":
    main()
