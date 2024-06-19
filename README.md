
This is an HFT bot for trading on kucoin futures with live data analysis.

Run the program using docker:
    
    docker build .
    docker compose -f docker-compose.yml up -d

The project use kucoin websocket, to live record OHLC candles of BTC and ETH futures ordebook data to influxDB. And will apply technical analysis on the candles and generate either short or long signal, and will backtest the position based on the coming data.

Dependencies of the project involves:

        Python 3.12
        Redis: 7.2.4
        InfluxDB: 1.8.10

