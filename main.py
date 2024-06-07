from influxdb import InfluxDBClient

client = InfluxDBClient(host='localhost', port=8086)


client.create_database('ohlc')

a = client.get_list_database()



print(a)