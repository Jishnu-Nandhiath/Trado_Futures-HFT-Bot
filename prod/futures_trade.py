import pandas as pd
import logging
from datetime import datetime
import ccxt
import os

logging.basicConfig(level=logging.DEBUG)

ccxt_symbols = ["BTC/USDT", "ETH/USDT"]

API_KEY = os.environ.get("API_KEY")
API_SECRET = os.environ.get("API_SECRET")

exchange_id = "kucoinfutures"  # Using KuCoin exchange
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class(
    {
        "apiKey": API_KEY,
        "secret": API_KEY,
    }
)


def initiate_trade(
    data, leverage, stop_loss_factor, take_profit_factor, symbol
):

    trades = []
    position = None
    total_profit = 0
    trade_count = 0
    winning_trades = 0
    daily_trades = {}
    daily_wins = {}

    print(data)
    
    # best to avoid additional checks, if open orders already exist, or balance is 0
    if fetch_open_orders(symbol) or not fetch_current_balance():
        return
    current_balance = fetch_current_balance()

    for index, row in data.iterrows():
        date = index.date()

        if row["signal"] == 1:  # Buy signal
            if not position:
                position = {
                    "entry_price": row["close"],
                    "entry_index": index,
                    "type": "long",
                    "stop_loss": row["close"] * (1 - stop_loss_factor),
                    "take_profit_1": row["close"] * (1 + take_profit_factor),
                }
                logging.debug(f"Entering long position at {row['close']} on {index}")
                logging.debug(
                    f"Stop loss set at {position['stop_loss']}, Take profit set at {position['take_profit_1']}, Balance: {balance}"
                )

        elif row["signal"] == -1:  # Sell signal
            if not position:
                position = {
                    "entry_price": row["close"],
                    "entry_index": index,
                    "type": "short",
                    "stop_loss": row["close"] * (1 + stop_loss_factor),
                    "take_profit_1": row["close"] * (1 - take_profit_factor),
                }
                logging.debug(f"Entering short position at {row['close']} on {index}")
                logging.debug(
                    f"Stop loss set at {position['stop_loss']}, Take profit set at {position['take_profit_1']}, Balance: {balance}"
                )

        side = None

        if position:
            if position["type"] == "short":
                side = "sell"
            elif position["type"] == "long":
                side = "buy"
                
            order_id = place_order(
                amount=current_balance,
                entry_price=position["entry_price"],
                exit_price=position["take_profit_1"],
                side=side,
                symbol=symbol,
                leverage = leverage
            )
            
            # if position["type"] == "long":
            #     if row["low"] <= position["stop_loss"]:
            #         profit = (
            #             leverage
            #             * (position["stop_loss"] - position["entry_price"])
            #             / position["entry_price"]
            #             * balance
            #         )
            #         logging.debug(
            #             f"Long position stop loss hit: entry_price = {position['entry_price']}, stop_loss = {position['stop_loss']}, leverage = {leverage}, profit = {profit}"
            #         )
            #         balance += profit
            #         total_profit += profit
            #         trades.append(
            #             {
            #                 "entry_index": position["entry_index"],
            #                 "entry_price": position["entry_price"],
            #                 "exit_index": index,
            #                 "exit_price": position["stop_loss"],
            #                 "type": "long",
            #                 "profit": profit,
            #             }
            #         )
            #         logging.debug(
            #             f"Stop loss hit at {position['stop_loss']} on {index} with profit {profit}, Balance: {balance}"
            #         )
            #         position = None
            #         trade_count += 1
            #         daily_trades[date] = daily_trades.get(date, 0) + 1
            #         if profit > 0:
            #             winning_trades += 1
            #             daily_wins[date] = daily_wins.get(date, 0) + 1

            #     elif row["high"] >= position["take_profit_1"]:
            #         profit = (
            #             leverage
            #             * (position["take_profit_1"] - position["entry_price"])
            #             / position["entry_price"]
            #             * balance
            #         )
            #         logging.debug(
            #             f"Long position take profit hit: entry_price = {position['entry_price']}, take_profit = {position['take_profit_1']}, leverage = {leverage}, profit = {profit}"
            #         )
            #         balance += profit
            #         total_profit += profit
            #         trades.append(
            #             {
            #                 "entry_index": position["entry_index"],
            #                 "entry_price": position["entry_price"],
            #                 "exit_index": index,
            #                 "exit_price": position["take_profit_1"],
            #                 "type": "long",
            #                 "profit": profit,
            #             }
            #         )
            #         logging.debug(
            #             f"Take profit hit at {position['take_profit_1']} on {index} with profit {profit}, Balance: {balance}"
            #         )
            #         position = None
            #         trade_count += 1
            #         daily_trades[date] = daily_trades.get(date, 0) + 1
            #         if profit > 0:
            #             winning_trades += 1
            #             daily_wins[date] = daily_wins.get(date, 0) + 1

            # elif position["type"] == "short":
            #     if row["high"] >= position["stop_loss"]:
            #         profit = (
            #             leverage
            #             * (position["entry_price"] - position["stop_loss"])
            #             / position["entry_price"]
            #             * balance
            #         )
            #         logging.debug(
            #             f"Short position stop loss hit: entry_price = {position['entry_price']}, stop_loss = {position['stop_loss']}, leverage = {leverage}, profit = {profit}"
            #         )
            #         balance += profit
            #         total_profit += profit
            #         trades.append(
            #             {
            #                 "entry_index": position["entry_index"],
            #                 "entry_price": position["entry_price"],
            #                 "exit_index": index,
            #                 "exit_price": position["stop_loss"],
            #                 "type": "short",
            #                 "profit": profit,
            #             }
            #         )
            #         logging.debug(
            #             f"Stop loss hit at {position['stop_loss']} on {index} with profit {profit}, Balance: {balance}"
            #         )
            #         position = None
            #         trade_count += 1
            #         daily_trades[date] = daily_trades.get(date, 0) + 1
            #         if profit > 0:
            #             winning_trades += 1
            #             daily_wins[date] = daily_wins.get(date, 0) + 1

            #     elif row["low"] <= position["take_profit_1"]:
            #         profit = (
            #             leverage
            #             * (position["entry_price"] - position["take_profit_1"])
            #             / position["entry_price"]
            #             * balance
            #         )
            #         logging.debug(
            #             f"Short position take profit hit: entry_price = {position['entry_price']}, take_profit = {position['take_profit_1']}, leverage = {leverage}, profit = {profit}"
            #         )
            #         balance += profit
            #         total_profit += profit
            #         trades.append(
            #             {
            #                 "entry_index": position["entry_index"],
            #                 "entry_price": position["entry_price"],
            #                 "exit_index": index,
            #                 "exit_price": position["take_profit_1"],
            #                 "type": "short",
            #                 "profit": profit,
            #             }
            #         )
            #         logging.debug(
            #             f"Take profit hit at {position['take_profit_1']} on {index} with profit {profit}, Balance: {balance}"
            #         )
            #         position = None
            #         trade_count += 1
            #         daily_trades[date] = daily_trades.get(date, 0) + 1
            #         if profit > 0:
            #             winning_trades += 1
            #             daily_wins[date] = daily_wins.get(date, 0) + 1

    # return (
    #     balance,
    #     total_profit,
    #     trade_count,
    #     trades,
    #     winning_trades,
    #     daily_trades,
    #     daily_wins,
    # )
    
    # TODO: Store in another db
    return True


def place_order(
    symbol,
    side,
    amount,
    stop_loss,
    entry_price,
    exit_price,
    leverage
):
    try:
        create_order_response = exchange.createOrder(
            symbol=symbol,
            side=side,
            amount=amount,
            price=entry_price,
            params={
                "takeProfitPrice": exit_price,
                "stopLossPrice": stop_loss,
                "leverage": leverage,
            },
        )
        return create_order_response['id']
    except Exception as e:
        return False


def fetch_current_balance():
    try:
        balance_response = exchange.fetchBalance()
        return balance_response["free"]["USD"]
    except Exception as e:
        # TODO: log the error
        return 0


def fetch_open_orders(symbol):
    if exchange.has["fetchOpenOrders"]:
        open_orders = exchange.fetchOpenOrders(symbol=symbol)

    if len(open_orders) == 0:
        return False
    return True


# def main():
#     symbols = ["btc_usdt", "eth_usdt"]
#     initial_balance = 100
#     leverage = 25
#     stop_loss_factor = 0.0055  # Adjusted to 0.55% below entry price
#     take_profit_factor = 0.015  # Adjusted to 1.5% above entry price

#     balance = initial_balance
#     all_trades = []
#     total_trades = 0
#     total_wins = 0
#     total_daily_trades = {}
#     total_daily_wins = {}

#     combined_data = []

#     for symbol in symbols:
#         data = pd.read_csv(
#             f"./data/processed/{symbol}_signals.csv",
#             index_col="timestamp",
#             parse_dates=True,
#         )
#         data["symbol"] = symbol
#         combined_data.append(data)

#     combined_data = pd.concat(combined_data).sort_index()

#     (
#         balance,
#         total_profit,
#         trade_count,
#         trades,
#         winning_trades,
#         daily_trades,
#         daily_wins,
#     ) = backtest(combined_data, balance, leverage, stop_loss_factor, take_profit_factor)
