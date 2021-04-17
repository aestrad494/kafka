#
# IB Producer
#
# (c) Andres Estrada Cadavid
# QSociety

from data_request import request
import json
from kafka import KafkaProducer

class LiveRange(request):
    def json_serializer(self, data):
        return json.dumps(data).encode("utf-8")

    def run_strategy(self):
        producer = KafkaProducer(bootstrap_servers=['167.99.156.175:9092'], 
                         value_serializer=self.json_serializer)
        while True:
            self.ib.sleep(1)
            print(self.bars[-1])
            producer.send('first_topic', self.bars[-1])
            
if __name__ == '__main__':
    live_range = LiveRange(symbol='MNQ', temp='1 min', client=60)
    live_range.run_strategy()