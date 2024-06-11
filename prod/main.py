import asyncio
from data_collection_futures import connect_to_websocket

def initiate_auto_trade():
    asyncio.run(connect_to_websocket())

if __name__ == "__main__":
    initiate_auto_trade()