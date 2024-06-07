import pandas as pd
import numpy as np
import logging
import os
import time

logging.basicConfig(level=logging.DEBUG)

def generate_trade_signal(df, stop_loss_factor, take_profit_factor):
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

    return df

def process_signals():
    symbols = {
        'BTC/USDT': 'xbtusdtm_futures_ohlcv_with_indicators.csv',
        'ETH/USDT': 'ethusdtm_futures_ohlcv_with_indicators.csv'
    }
    stop_loss_factor = 0.0055  # Adjusted to 0.55%
    take_profit_factor = 0.015  # Adjusted to 1.5%

    while True:
        for symbol, file_name in symbols.items():
            logging.info(f"Processing signals for {symbol}")
            
            # Read the existing data with indicators
            data_path = f'./data/{file_name}'
            df = pd.read_csv(data_path, parse_dates=['timestamp'])

            # Ensure index is in datetime format
            df.set_index('timestamp', inplace=True)
            df.index = pd.to_datetime(df.index)

            # Generate trade signals
            df = generate_trade_signal(df, stop_loss_factor, take_profit_factor)

            # Save signals to a separate file
            signals = df[df['signal'] != 0]
            signals_file_path = f'./data/{symbol.replace("/", "_").lower()}_trade_signals.csv'
            if not os.path.exists(signals_file_path):
                signals.to_csv(signals_file_path)
            else:
                signals.to_csv(signals_file_path, mode='a', header=not os.path.exists(signals_file_path))
            logging.info(f"Trade signals updated and saved for {symbol}")

        time.sleep(60)  # Wait for a minute before processing new signals

if __name__ == "__main__":
    process_signals()
