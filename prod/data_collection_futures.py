import pandas as pd
import ta  # Technical Analysis library
import ccxt
import json
import asyncio
import websockets
from kucoin_futures.client import WsToken
from utils import write_to_influx_ohlc
from real_time_signal_generation import generate_trade_signal

exchange_id = 'huobi'  # Using KuCoin exchange
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class()

ccxt_symbols = ['BTC/USDT', 'ETH/USDT']
original_symbols = ['XBTUSDTM', 'ETHUSDTM']

timeframe = '1m'  # Using 1-minute interval
start_date = '2024-05-01T00:00:00Z'
end_date = '2024-06-05T00:00:00Z'
limit = 1500

recent_klines = {'XBTUSDTM': [], 'ETHUSDTM': []}
window_width = 14

log_lines = []

STOP_LOSS_FACTOR = 0.0055  # Adjusted to 0.55%
TAKE_PROFIT_FACTOR = 0.015  # Adjusted to 1.5%

def is_new_kline(kline):
    symbol = kline['symbol']
    timestamp = int(kline['candles'][0]) * 1000
    
    if len(recent_klines[symbol]) != 0:
        last_timestamp = recent_klines[symbol][-1][0]
        return  last_timestamp != timestamp
    return True

all_data = []

async def handle_message(msg):
    global log_lines
    try:
        data = json.loads(msg)
        # print(f"Received message: {data}")

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
                    
                all_data.append([timestamp, open, high, low, close, volume])
                recent_klines[symbol] = all_data[-(window_width*3):]

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
                
                print("candle df : ", df)
                
                write_to_influx_ohlc(measurement_name=f"{symbol.lower()}_ohlc_candles", df=df)
                generate_trade_signal(df=df, stop_loss_factor=STOP_LOSS_FACTOR, take_profit_factor=TAKE_PROFIT_FACTOR, symbol= symbol)
                
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