import pandas as pd
import numpy as np
import logging
import os
import time
from influxdb import InfluxDBClient
from utils import write_to_influx_signals
from futures_trade import initiate_trade

logging.basicConfig(level=logging.DEBUG)


influx_client = InfluxDBClient(host='localhost', port=8086, database="ohlc")


def generate_trade_signal(symbol, df, stop_loss_factor, take_profit_factor):
    buffer = 0.00005  # More sensitive buffer value for testing
    df['signal'] = 0
    df['stop_loss'] = np.nan
    df['take_profit_1'] = np.nan

    trade_count = 0
    max_trades_per_day = 10  # Increase the limit for testing

    for i in range(1, len(df)):
        if pd.to_datetime(df.index[i]).date() != pd.to_datetime(df.index[i - 1]).date():
            trade_count = 0

        if trade_count < max_trades_per_day:
            price_change = abs(df['close'].iloc[i] - df['close'].iloc[i-1]) / df['close'].iloc[i-1]
            logging.debug(f"Index: {i}, Price Change: {price_change}, Buffer: {buffer}")
            if price_change > buffer:
                logging.debug(f"Index: {i}, Indicators: {df.iloc[i]}")

                # Use multiple conditions to determine signals
                if df['double_top'].iloc[i] or df['head_and_shoulders'].iloc[i] or df['triple_top'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = -1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (1 + stop_loss_factor)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1 - take_profit_factor)
                    logging.info(f"Sell signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                elif df['double_bottom'].iloc[i] or df['inverse_head_and_shoulders'].iloc[i] or df['triple_bottom'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = 1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (1 - stop_loss_factor)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1 + take_profit_factor)
                    logging.info(f"Buy signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                # Apply additional patterns and indicators
                if df['flag'].iloc[i]:
                    # Example logic for flags
                    df.loc[df.index[i], 'signal'] = 1 if df['RSI'].iloc[i] < 55 else -1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (0.99 if df['RSI'].iloc[i] < 55 else 1.01)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1.02 if df['RSI'].iloc[i] < 55 else 0.98)
                    logging.info(f"Flag pattern signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                if df['ascending_triangle'].iloc[i] or df['rising_wedge'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = 1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (1 - stop_loss_factor)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1 + take_profit_factor)
                    logging.info(f"Ascending pattern signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                if df['descending_triangle'].iloc[i] or df['falling_wedge'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = -1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (1 + stop_loss_factor)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1 - take_profit_factor)
                    logging.info(f"Descending pattern signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                if df['cup_and_handle'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = 1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (1 - stop_loss_factor)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1 + take_profit_factor)
                    logging.info(f"Cup and handle pattern signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                if df['pennant'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = 1 if df['RSI'].iloc[i] < 55 else -1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (0.99 if df['RSI'].iloc[i] < 55 else 1.01)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1.02 if df['RSI'].iloc[i] < 55 else 0.98)
                    logging.info(f"Pennant pattern signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                # Support and resistance levels
                if df['close'].iloc[i] <= df['support'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = 1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (1 - stop_loss_factor)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1 + take_profit_factor)
                    logging.info(f"Support level signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

                if df['close'].iloc[i] >= df['resistance'].iloc[i]:
                    df.loc[df.index[i], 'signal'] = -1
                    trade_count += 1
                    df.loc[df.index[i], 'stop_loss'] = df['close'].iloc[i] * (1 + stop_loss_factor)
                    df.loc[df.index[i], 'take_profit_1'] = df['close'].iloc[i] * (1 - take_profit_factor)
                    logging.info(f"Resistance level signal generated at {df.index[i]} with price {df['close'].iloc[i]}")

    write_to_influx_signals(measurement_name=f"{symbol.lower()}_trade_signals", df= df)
    initiate_trade(symbol=symbol, data=df, leverage=25, stop_loss_factor= 0.0055,take_profit_factor= 0.015 )
    
    return df