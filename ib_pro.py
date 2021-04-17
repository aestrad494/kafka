from ib_insync import IB, util, Future
from kafka import KafkaProducer
import json
from time import sleep

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

ib = IB()
print(ib.connect('127.0.0.1', 7497, 5))

symbol = 'MNQ'
exchange = 'GLOBEX'

expiration = ib.reqContractDetails(Future(symbol, exchange))[0].contract.lastTradeDateOrContractMonth
contract = Future(symbol=symbol, exchange=exchange, lastTradeDateOrContractMonth=expiration)

sleep(20)

bars = ib.reqRealTimeBars(contract, 5, 'MIDPOINT', False)
print(bars)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                         value_serializer=json_serializer)

while True:
    print(bars[-1])
    producer.send('first_topic', bars[-1])
    sleep(5)