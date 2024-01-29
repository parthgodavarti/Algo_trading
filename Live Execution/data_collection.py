from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pandas as pd
import pyotp
import datetime
import holidays
import requests
import telebot
from logzero import logger
import time
import csv
import os

def sendMessage(message_text):
    try:
        bot = telebot.TeleBot(BOT Details)
        chat_id =Your chat ID
        bot.send_message(chat_id, message_text)
    except telebot.apihelper.ApiTelegramException as e:
        print(f"Failed to send message: {e.description}")


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
    trading_symbols =[
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "HINDUNILVR", "INFY", "ITC", "SBIN", "BHARTIARTL",
    "KOTAKBANK", "BAJFINANCE", "LICI", "LT", "HCLTECH", "ASIANPAINT", "AXISBANK", "MARUTI", "SUNPHARMA",
    "TITAN", "DMART", "ULTRACEMCO", "BAJAJFINSV", "WIPRO", "ADANIENT", "ONGC", "NTPC", "JSWSTEEL",
    "POWERGRID", "M&M", "LTIM", "TATAMOTORS", "ADANIGREEN", "ADANIPORTS", "COALINDIA", "TATASTEEL",
    "HINDZINC", "PIDILITIND", "SIEMENS", "SBILIFE", "IOC", "BAJAJ-AUTO", "GRASIM", "TECHM", "HDFCLIFE",
    "BRITANNIA", "VEDL", "GODREJCP", "DABUR", "ATGL", "HAL", "HINDALCO", "VBL", "DLF", "BANKBARODA",
    "INDUSINDBK", "EICHERMOT", "DRREDDY", "DIVISLAB", "BPCL", "HAVELLS", "ADANIPOWER", "INDIGO", "CIPLA",
    "AMBUJACEM", "SRF", "ABB", "BEL", "SBICARD", "GAIL", "TATACONSUM", "ICICIPRULI", "CHOLAFIN", "MARICO",
    "APOLLOHOSP", "TATAPOWER", "BERGEPAINT", "JINDALSTEL", "MCDOWELL-N", "UPL", "AWL", "ICICIGI", "TORNTPHARM",
    "CANBK", "PNB", "TVSMOTOR", "ZYDUSLIFE", "TIINDIA", "TRENT", "IDBI", "NAUKRI", "SHRIRAMFIN", "HEROMOTOCO",
    "INDHOTEL", "PIIND", "IRCTC", "CGPOWER", "UNIONBANK", "MOTHERSON", "CUMMINSIND", "LODHA", "ZOMATO", "YESBANK",
    "POLYCAB", "MAXHEALTH", "IOB", "COLPAL"
]
    
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

def is_indian_trading_day(date):
  if date.isoweekday() in [6, 7]:
    return False

  # Check if the date is a holiday.
  india_holidays = holidays.India()
  if india_holidays.get(date):
    return False

  return True

def login():
    api_key = ''
    Secret=''
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


def on_open(wsapp):
    logger.info("on open")

    # Extract tokens from the dictionary
    tokens = list(tokenlist.values())

    # Divide the tokens into batches of 10
    batch_size = 10
    token_batches = [tokens[i:i+batch_size] for i in range(0, len(tokens), batch_size)]

    for token_batch in token_batches:
        token_list = [
            {
                "exchangeType": 1,
                "tokens": token_batch
            }
        ]

        logger.info(f"Subscribing to tokens: {token_batch}")
        sws.subscribe("abc123", 3, token_list)


def on_data(wsapp, message):
    global last_processed_timestamp, accumulated_data,data_collection_complete

    try:
        timestamp = message["exchange_timestamp"]
        unix_timestamp = timestamp / 1000
        dt = datetime.datetime.fromtimestamp(unix_timestamp)

        # Check if a minute has passed since the last processed message
        if last_processed_timestamp is None or (dt - last_processed_timestamp).total_seconds() >= 60:
            # Process accumulated data for each token
            data_list = []

            for symbol, token_data in accumulated_data.items():
                symbol_dt = token_data['datetime']
                open_price = token_data['open']
                high_price = token_data['high']
                low_price = token_data['low']
                last_traded_price = token_data['close']
                volume = token_data['volume']

                # Your calculations or processing logic here
                data_list.append({
                    'datetime': symbol_dt,
                    'symbol': symbol,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': last_traded_price,
                    'volume': volume
                })

            # Convert data_list to a DataFrame
            df = pd.DataFrame(data_list)

            df.to_csv('/home/parth/Python/my_data.csv',index=False)
            # Reset accumulated data
            accumulated_data = {}

            # Update the last processed timestamp
            last_processed_timestamp = dt
            print(df)
            # Return the DataFrame

        # Accumulate data for the current message
        symbol = message["token"]
        temporary_data = {
            'symbol': symbol,
            'datetime': dt,
            'open': message["open_price_of_the_day"]/100,
            'high': message["high_price_of_the_day"]/100,
            'low': message["low_price_of_the_day"]/100,
            'close': message["last_traded_price"]/100,
            'volume': message['volume_trade_for_the_day']
        }

        # Store data for the symbol
        accumulated_data[symbol] = temporary_data
        if datetime.time()>= datetime.time(9,9):
            data_collection_complete= True

            with open("live_data.csv",'w') as flag_file:
                flag_file.write("Data Collection Complete")

    except Exception as e:
        logger.error(e)
        return None  # Return None if there's an error

def on_error(wsapp, error):
    logger.error(error)

def on_close(wsapp, status_code, message, exception):
    logger.info("Close")
    logger.info(f"Status code: {status_code}, Message: {message}, Exception: {exception}")

def close_connection():
    sws.close_connection()


def delete_file(file_path):
    """
    Check if a file is present and delete it if it exists.

    Parameters:
    - file_path (str): The path to the file.

    Returns:
    - bool: True if the file was deleted, False otherwise.
    """
    try:
        # Check if the file exists
        if os.path.exists(file_path):
            # Delete the file
            os.remove(file_path)
            print(f"The file '{file_path}' has been deleted.")
            return True
        else:
            print(f"The file '{file_path}' does not exist.")
            return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


last_processed_timestamp=None
accumulated_data={}
data_collection_complete=False
while True:
    try:
        delete_file('/home/parth/Python/my_data.csv')
        now = datetime.datetime.now()
        current_time =datetime.datetime.now().time()
        tokenlist = token_list()
        trading_start_time = datetime.time(9,00)
        trading_end_time = datetime.time(15, 00)

        if trading_start_time <= current_time <= trading_end_time:
            # Log in to the trading platform and establish WebSocket connection
            smartApi, feedToken, authToken = login()
            login_successful=True
            token_data = {
                "exchangeType": 1,
                "tokens": list(tokenlist.values())
            }
            sendMessage('token_list working')
            sws = SmartWebSocketV2(authToken, '1vvfqSG8', 'P53117244', feedToken)
            sws.on_open = on_open
            sws.on_data = on_data
            sws.on_error = on_error
            sws.on_close = on_close
            sendMessage("login and webhook working")
            print('Token list working')
            sws.connect()

        else:
            print('Not trading time, sleeping...')
            time.sleep(120)

    except Exception as e:
        print(f"Error in data collection loop: {str(e)}")
        time.sleep(60)  # Adjust as needed

