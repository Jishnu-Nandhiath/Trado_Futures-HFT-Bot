import pandas as pd
import os
import ta  # Technical Analysis library
from datetime import datetime
import ccxt
import json
import asyncio
import websockets
from kucoin_futures.client import WsToken
import time
import pytz
from influxdb import InfluxDBClient
import numpy as np

# Ensure the directory exists
os.makedirs('./data', exist_ok=True)



exchange_id = 'huobi'  # Using KuCoin exchange
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class()

ccxt_symbols = ['BTC/USDT', 'ETH/USDT']
original_symbols = ['XBTUSDTM', 'ETHUSDTM']

timeframe = '1m'  # Using 1-minute interval
start_date = '2024-05-01T00:00:00Z'
end_date = '2024-06-05T00:00:00Z'
limit = 1500

recent_klines = {}
window_width = 14

log_lines = []
    
influx_client = InfluxDBClient(host='localhost', port=8086, database="ohlc")

def write_to_influx(symbol, df):
    influx_points = []

    
    for index, row in df.iterrows():
        
        print(row)
        point = {
            "measurement": symbol,
            "time": index.isoformat(),
            "fields": {
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close'],
                "volume": row['volume'],
                "SMA": row['SMA'] if not np.isnan(row['SMA']) else None,
                "RSI": row['RSI'] if not np.isnan(row['RSI']) else None,
                "MACD": row['MACD'] if not np.isnan(row['MACD']) else None,
                "MACD_signal": row['MACD_signal'] if not np.isnan(row['MACD_signal']) else None,
                "MACD_diff": row['MACD_diff'] if not np.isnan(row['MACD_diff']) else None,
                "double_top": row['double_top'],
                "double_bottom": row['double_bottom'],
                "flag": row['flag'],
                "head_and_shoulders": row['head_and_shoulders'],
                "inverse_head_and_shoulders": row['inverse_head_and_shoulders'],
                "ascending_triangle": row['ascending_triangle'],
                "descending_triangle": row['descending_triangle'],
                "rising_wedge": row['rising_wedge'],
                "falling_wedge": row['falling_wedge'],
                "triple_top": row['triple_top'],
                "triple_bottom": row['triple_bottom'],
                "cup_and_handle": row['cup_and_handle'],
                "pennant": row['pennant'],
                "support": row['support'] if not np.isnan(row['MACD']) else None,
                "resistance": row['resistance'] if not np.isnan(row['MACD']) else None
            }
        }
        
        print("appending: ", point)
        
        influx_client.write_points([point])
        
        print("write complete")
    #     influx_points.append(point)
    # influx_client.write_points(influx_points)

# Fetch historical data for initialization
def fetch_historical_data(symbol, start_date, end_date, original_symbol):
    all_data = []
    since = exchange.parse8601(start_date)
    until = exchange.parse8601(end_date)
    
    while since < until:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            if not ohlcv:
                break
            since = ohlcv[-1][0] + 60000  # move to next timestamp, +1 minute
            all_data.extend(ohlcv)
            
            print("new ohlc : ", ohlcv)
            
        except ccxt.BaseError as e:
            print(f"Error fetching data for {symbol} at {since}: {e}")
            log_lines.append(f"Error fetching data for {symbol} at {since}: {e}")
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Unexpected error: {e}")
            log_lines.append(f"Unexpected error: {e}")
            break

    if all_data:
        recent_klines[original_symbol] = all_data[-(window_width*3):]
        df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Montreal')
        df.set_index('timestamp', inplace=True)

        # Calculate technical indicators
        df['SMA'] = ta.trend.sma_indicator(df['close'], window=window_width)
        df['RSI'] = ta.momentum.rsi(df['close'], window=window_width)
        df['MACD'] = ta.trend.macd(df['close'])
        df['MACD_signal'] = ta.trend.macd_signal(df['close'])
        df['MACD_diff'] = df['MACD'] - df['MACD_signal']

        # Identify patterns
        df['double_top'] = ((df['high'].shift(1) > df['high']) & (df['high'].shift(1) > df['high'].shift(2)))
        df['double_bottom'] = ((df['low'].shift(1) < df['low']) & (df['low'].shift(1) < df['low'].shift(2)))
        df['flag'] = ((df['close'] > df['SMA']) & (df['RSI'] < 70))

        # Identify head and shoulders patterns
        df['head_and_shoulders'] = ((df['high'].shift(2) < df['high'].shift(1)) & (df['high'].shift(1) > df['high']) &
                                    (df['high'].shift(1) > df['high'].shift(3)) & (df['high'] < df['high'].shift(1)))
        df['inverse_head_and_shoulders'] = ((df['low'].shift(2) > df['low'].shift(1)) & (df['low'].shift(1) < df['low']) &
                                            (df['low'].shift(1) < df['low'].shift(3)) & (df['low'] > df['low'].shift(1)))

        # Identify ascending and descending triangles
        df['ascending_triangle'] = ((df['low'].shift(1) > df['low'].shift(2)) & (df['high'].shift(1) < df['high']))
        df['descending_triangle'] = ((df['high'].shift(1) < df['high'].shift(2)) & (df['low'].shift(1) > df['low']))

        # Identify wedges
        df['rising_wedge'] = ((df['high'] > df['high'].shift(1)) & (df['low'] > df['low'].shift(1)))
        df['falling_wedge'] = ((df['high'] < df['high'].shift(1)) & (df['low'] < df['low'].shift(1)))

        # Identify triple tops and bottoms
        df['triple_top'] = ((df['high'].shift(1) > df['high']) & (df['high'].shift(1) > df['high'].shift(2)) &
                            (df['high'].shift(1) > df['high'].shift(3)))
        df['triple_bottom'] = ((df['low'].shift(1) < df['low']) & (df['low'].shift(1) < df['low'].shift(2)) &
                               (df['low'].shift(1) < df['low'].shift(3)))

        # Identify cup and handle patterns
        df['cup_and_handle'] = ((df['close'] < df['close'].shift(1)) & (df['close'] < df['close'].shift(2)) &
                                (df['close'] < df['close'].shift(3)) & (df['close'] > df['close'].shift(4)) &
                                (df['close'] > df['close'].shift(5)))

        # Identify pennants
        df['pennant'] = ((df['high'] < df['high'].shift(1)) & (df['low'] > df['low'].shift(1)) &
                         (df['high'].shift(1) < df['high'].shift(2)) & (df['low'].shift(1) > df['low'].shift(2)))

        # Identify support and resistance levels
        df['support'] = df['low'].rolling(window=window_width).min()
        df['resistance'] = df['high'].rolling(window=window_width).max()

        write_to_influx(original_symbol, df)
        print(f"Data written to InfluxDB for {original_symbol}")
        log_lines.append(f"Data written to InfluxDB for {original_symbol}")

        # data_path = f'./data/{original_symbol.lower()}_futures_ohlcv_with_indicators.csv'
        # df.to_csv(data_path)
        # print(f"Data saved to {data_path}")
        # log_lines.append(f"Data saved to {data_path}")
    else:
        print(f"No data collected for {symbol}")
        log_lines.append(f"No data collected for {symbol}")


def is_new_kline(kline):
    symbol = kline['symbol']
    timestamp = int(kline['candles'][0]) * 1000
    last_timestamp = recent_klines[symbol][-1][0]
    return  last_timestamp != timestamp


async def handle_message(msg):
    global log_lines
    try:
        data = json.loads(msg)
        print(f"Received message: {data}")

        if 'data' in data:
            kline = data['data']
        else:
            kline = None

        if kline and isinstance(kline, dict):
            if is_new_kline(kline):
                symbol = kline['symbol']
                timestamp = int(kline['candles'][0]) * 1000
                open = float(kline['candles'][1])
                high = float(kline['candles'][2])
                low = float(kline['candles'][3])
                close = float(kline['candles'][4])
                volume = float(kline['candles'][5])

                recent_klines[symbol].pop(0)
                recent_klines[symbol].append([timestamp, open, high, low, close, volume])

                df = pd.DataFrame(recent_klines[symbol], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Montreal')
                df.set_index('timestamp', inplace=True)

                # Calculate technical indicators
                df['SMA'] = ta.trend.sma_indicator(df['close'], window=window_width)
                df['RSI'] = ta.momentum.rsi(df['close'], window=window_width)
                df['MACD'] = ta.trend.macd(df['close'])
                df['MACD_signal'] = ta.trend.macd_signal(df['close'])
                df['MACD_diff'] = df['MACD'] - df['MACD_signal']

                # Identify patterns
                df['double_top'] = ((df['high'].shift(1) > df['high']) & (df['high'].shift(1) > df['high'].shift(2)))
                df['double_bottom'] = ((df['low'].shift(1) < df['low']) & (df['low'].shift(1) < df['low'].shift(2)))
                df['flag'] = ((df['close'] > df['SMA']) & (df['RSI'] < 70))

                # Identify head and shoulders patterns
                df['head_and_shoulders'] = ((df['high'].shift(2) < df['high'].shift(1)) & (df['high'].shift(1) > df['high']) &
                                            (df['high'].shift(1) > df['high'].shift(3)) & (df['high'] < df['high'].shift(1)))
                df['inverse_head_and_shoulders'] = ((df['low'].shift(2) > df['low'].shift(1)) & (df['low'].shift(1) < df['low']) &
                                                    (df['low'].shift(1) < df['low'].shift(3)) & (df['low'] > df['low'].shift(1)))

                # Identify ascending and descending triangles
                df['ascending_triangle'] = ((df['low'].shift(1) > df['low'].shift(2)) & (df['high'].shift(1) < df['high']))
                df['descending_triangle'] = ((df['high'].shift(1) < df['high'].shift(2)) & (df['low'].shift(1) > df['low']))

                # Identify wedges
                df['rising_wedge'] = ((df['high'] > df['high'].shift(1)) & (df['low'] > df['low'].shift(1)))
                df['falling_wedge'] = ((df['high'] < df['high'].shift(1)) & (df['low'] < df['low'].shift(1)))

                # Identify triple tops and bottoms
                df['triple_top'] = ((df['high'].shift(1) > df['high']) & (df['high'].shift(1) > df['high'].shift(2)) &
                                    (df['high'].shift(1) > df['high'].shift(3)))
                df['triple_bottom'] = ((df['low'].shift(1) < df['low']) & (df['low'].shift(1) < df['low'].shift(2)) &
                                    (df['low'].shift(1) < df['low'].shift(3)))

                # Identify cup and handle patterns
                df['cup_and_handle'] = ((df['close'] < df['close'].shift(1)) & (df['close'] < df['close'].shift(2)) &
                                        (df['close'] < df['close'].shift(3)) & (df['close'] > df['close'].shift(4)) &
                                        (df['close'] > df['close'].shift(5)))

                # Identify pennants
                df['pennant'] = ((df['high'] < df['high'].shift(1)) & (df['low'] > df['low'].shift(1)) &
                                (df['high'].shift(1) < df['high'].shift(2)) & (df['low'].shift(1) > df['low'].shift(2)))

                # Identify support and resistance levels
                df['support'] = df['low'].rolling(window=window_width).min()
                df['resistance'] = df['high'].rolling(window=window_width).max()

                # Append new data to CSV
                data_path = f'./data/{symbol.lower()}_futures_ohlcv_with_indicators.csv'
                df = df.iloc[-1:]
                df.to_csv(data_path, mode='a', header=not os.path.exists(data_path))
                print(f"Data saved to {data_path}")
                log_lines.append(f"Data saved to {data_path}")
        else:
            print("Received data has no 'data' key")
            log_lines.append("Received data has no 'data' key")
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        log_lines.append(f"JSON decode error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")
        log_lines.append(f"Error processing message: {e}")


async def connect_to_websocket():
    client = WsToken()
    ws_token = client.get_ws_token()

    async with websockets.connect(ws_token['instanceServers'][0]['endpoint'] + "?token=" + ws_token['token']) as ws:
        subscribe_message = {
            "type": "subscribe",
            "topic": "/contractMarket/limitCandle:XBTUSDTM_1min,ETHUSDTM_1min",
            "response": True
        }
        await ws.send(json.dumps(subscribe_message))
        print("WebSocket connection opened")

        async for message in ws:
            await handle_message(message)

# Initial fetch to get historical data
for symbol, original_symbol in zip(ccxt_symbols, original_symbols):
    fetch_historical_data(symbol, start_date, end_date, original_symbol)

print(recent_klines)

# # WebSocket setup and connection
# loop = asyncio.get_event_loop()
# loop.run_until_complete(connect_to_websocket())

# Save log to file
try:
    log_file_path = './data/data_collection_log.txt'
    print(f"Writing log to {log_file_path}")
    with open(log_file_path, 'w') as log_file:
        log_file.write('\n'.join(log_lines))
    print("Log file written successfully.")
except Exception as e:
    print(f"Error writing log file: {e}")

print("Data collection completed.")
