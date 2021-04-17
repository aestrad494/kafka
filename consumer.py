#
# IB Consumer
#
# (c) Andres Estrada Cadavid
# QSociety

import json
from kafka import KafkaConsumer

class Consumer():
    def __init__(self, symbol):
        self.symbol = symbol

    def consume(self):
        consumer = KafkaConsumer(self.symbol, bootstrap_servers='167.99.156.175:9092')
        for record in consumer:
            print(record.value)

if __name__=='__main__':
    symbol = 'MNQ'  #input('\tsymbol: ')
    ib_consumer = Consumer(symbol)
    ib_consumer.consume()

    