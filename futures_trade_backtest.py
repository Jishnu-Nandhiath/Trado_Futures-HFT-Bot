import pandas as pd
import logging
from datetime import datetime
import ccxt
import json
import redis
from utils import write_to_influx_trade_analysis

logging.basicConfig(level=logging.DEBUG)

ccxt_symbols = ["BTC/USDT", "ETH/USDT"]

INITIAL_BALANCE = 100
LEVERAGE = 25

r = redis.Redis(host="cache")

def initiate_trade(
    data, leverage, stop_loss_factor, take_profit_factor, symbol
):
    position = None

    logging.debug(f"Trade Initiated! Based on current signals")
    
    # best to avoid additional checks, if open orders already exist, then orders are not placed
    if fetch_open_order_status(symbol):
        current_position = r.get("ACTIVE_POSITION")
        current_position_json = json.loads(current_position)
        
        if current_position_json["symbol"] == symbol:
            logging.debug(f"Active position detected!, Validating current position!")
            validate_current_position(df=data)
        return


    for index, row in data.iterrows():        
        date = index.date()
        
        logging.debug(f"Setting a new position based on current signal: {row["signal"]}!")

        if row["signal"] == 1:  # Buy signal
            if not position:
                position = {
                    "entry_price": row["close"],
                    "entry_index": index.timestamp(),
                    "type": "long",
                    "stop_loss": row["close"] * (1 - stop_loss_factor),
                    "take_profit_1": row["close"] * (1 + take_profit_factor),
                    "symbol": symbol
                }
                position_dict_string = json.dumps(position)
                r.set('ACTIVE_POSITION', position_dict_string)
                
                logging.debug(f"Entering long position at {row['close']} on {index}")
                logging.debug(
                    f"Stop loss set at {position['stop_loss']}, Take profit set at {position['take_profit_1']}"
                )

        elif row["signal"] == -1:  # Sell signal
            if not position:
                position = {
                    "entry_price": row["close"],
                    "entry_index": index.timestamp(),
                    "type": "short",
                    "stop_loss": row["close"] * (1 + stop_loss_factor),
                    "take_profit_1": row["close"] * (1 - take_profit_factor),
                    "symbol": symbol
                }
                position_dict_string = json.dumps(position)
                r.set('ACTIVE_POSITION', position_dict_string)
                
                logging.debug(f"Entering short position at {row['close']} on {index}")
                logging.debug(
                    f"Stop loss set at {position['stop_loss']}, Take profit set at {position['take_profit_1']}"
                )

    return True

def validate_current_position(df):
    
    balance = fetch_current_balance()  
    position = r.get('ACTIVE_POSITION')
    position = json.loads(position)
    
    logging.debug(f"Validating current position against coming data! -- current position : {position}")
    
    for index, row in df.iterrows():
        print(row)
        date = index.date()
        
        if position["type"] == "long":
            if row["low"] <= position["stop_loss"]:
                
                profit = (LEVERAGE* (position["stop_loss"] - position["entry_price"])/ position["entry_price"] * balance)
                
                logging.debug(
                    f"Long position stop loss hit: entry_price = {position['entry_price']}, stop_loss = {position['stop_loss']}, leverage = {LEVERAGE}, profit = {profit}"
                )
                balance += profit
                r.set('CURRENT_BALANCE', balance)
                
                trade  = dict()
                trade.update(position)
                trade["exit_index"] = index
                trade["exit_price"] = position["stop_loss"]
                trade["balance"] = balance
                trade["profit"] = profit
                
                write_to_influx_trade_analysis(
                    measurement_name=f"{position["symbol"]}_trades",
                    df=trade
                )
                logging.debug(
                    f"Stop loss hit at {position['stop_loss']} on {index} with profit {profit}, Balance: {balance}"
                )
                r.delete('ACTIVE_POSITION')

            elif row["high"] >= position["take_profit_1"]:
                profit = (
                    LEVERAGE
                    * (position["take_profit_1"] - position["entry_price"])
                    / position["entry_price"]
                    * balance
                )
                logging.debug(
                    f"Long position take profit hit: entry_price = {position['entry_price']}, take_profit = {position['take_profit_1']}, leverage = {LEVERAGE}, profit = {profit}"
                )
                balance += profit
                r.set('CURRENT_BALANCE', balance)
                
                trade  = dict()
                trade.update(position)
                trade["exit_index"] = index
                trade["exit_price"] = position["take_profit_1"]
                trade["balance"] = balance
                trade["profit"] = profit
                
                write_to_influx_trade_analysis(
                    measurement_name=f"{position["symbol"]}_trades",
                    df=trade
                )
                logging.debug(
                    f"Take profit hit at {position['take_profit_1']} on {index} with profit {profit}, Balance: {balance}"
                )
                r.delete('ACTIVE_POSITION')

        elif position["type"] == "short":
            if row["high"] >= position["stop_loss"]:
                profit = (
                    LEVERAGE
                    * (position["entry_price"] - position["stop_loss"])
                    / position["entry_price"]
                    * balance
                )
                logging.debug(
                    f"Short position stop loss hit: entry_price = {position['entry_price']}, stop_loss = {position['stop_loss']}, leverage = {LEVERAGE}, profit = {profit}"
                )
                balance += profit
                r.set('CURRENT_BALANCE', balance)
                
                trade  = dict()
                trade.update(position)
                trade["exit_index"] = index
                trade["exit_price"] = position["stop_loss"]
                trade["balance"] = balance
                trade["profit"] = profit
                
                write_to_influx_trade_analysis(
                    measurement_name=f"{position["symbol"]}_trades",
                    df=trade
                )
                
                logging.debug(
                    f"Stop loss hit at {position['stop_loss']} on {index} with profit {profit}, Balance: {balance}"
                )
                r.delete('ACTIVE_POSITION')

            elif row["low"] <= position["take_profit_1"]:
                profit = (
                    LEVERAGE
                    * (position["entry_price"] - position["take_profit_1"])
                    / position["entry_price"]
                    * balance
                )
                logging.debug(
                    f"Short position take profit hit: entry_price = {position['entry_price']}, take_profit = {position['take_profit_1']}, leverage = {LEVERAGE}, profit = {profit}"
                )
                balance += profit
                r.set('CURRENT_BALANCE', balance)
                
                trade  = dict()
                trade.update(position)
                trade["exit_index"] = index
                trade["exit_price"] = position["take_profit_1"]
                trade["balance"] = balance
                trade["profit"] = profit
                
                write_to_influx_trade_analysis(
                    measurement_name=f"{position["symbol"]}_trades",
                    df=trade
                )
                
                logging.debug(
                    f"Take profit hit at {position['take_profit_1']} on {index} with profit {profit}, Balance: {balance}"
                )
                
                r.delete('ACTIVE_POSITION')



def fetch_open_order_status(symbol):
    current_position = r.get('ACTIVE_POSITION')
    if current_position:
        return True
    return False


def fetch_current_balance():
    current_balance = r.get("CURRENT_BALANCE")
    
    if not current_balance:
        r.set("CURRENT_BALANCE", INITIAL_BALANCE)
        return INITIAL_BALANCE
    return float(current_balance)
