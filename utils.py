from influxdb import InfluxDBClient
import numpy as np

# influx_client = 


def write_to_influx_ohlc(measurement_name, df):
    influx_points = []

    for index, row in df.iterrows():
        point = {
            "measurement": measurement_name,
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
        
        print("appending ohlc: ", point)
        
        with InfluxDBClient(host='influx', port=8086, database="ohlc") as influx_client:
            influx_client.write_points([point])
        
        # print("write complete")
    #     influx_points.append(point)
    # influx_client.write_points(influx_points)
    
    
def write_to_influx_signals(measurement_name, df):
    
    for index, row in df.iterrows():

        point = {
            "measurement": measurement_name,
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
                "resistance": row['resistance'] if not np.isnan(row['MACD']) else None,
                "signal": row["signal"],
                "stop_loss": row["stop_loss"] if not np.isnan(row['stop_loss']) else None,
                "take_profit_1": row["take_profit_1"] if not np.isnan(row['take_profit_1']) else None
            }
        }
        
        print("appending trade signal: ", point)
        
        with InfluxDBClient(host='influx', port=8086, database="ohlc") as influx_client:
            influx_client.write_points([point])
        
        
def write_to_influx_trade_analysis(measurement_name, df):
    
    index = df["exit_index"]
    
    print("inside index")
    print(type(df))
    
    print("inside trade save: ", df)
    
    point = {
        "measurement": measurement_name,
        "time": index.isoformat(),
        "fields": {
            "balance": df["balance"],
            "entry_index": df["entry_index"],
            "exit_index": df["exit_index"].isoformat(),
            "entry_price": df["entry_price"],
            "exit_price": df["exit_price"],
            "position": df["type"],
            "profit": df["profit"],
        }
    }
        
    print("appending finished trade: ", point)
        
    with InfluxDBClient(host='influx', port=8086, database="ohlc") as influx_client:
        influx_client.write_points([point])