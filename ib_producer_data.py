#
# IB Producer
#
# (c) Andres Estrada Cadavid
# QSociety

import pandas as pd
from data_request import request
import json
from kafka import KafkaProducer

class Producer(request):
    def x_round(self, x):
        mult = 1/self.tick_size
        return round(x*mult)/mult

    def json_serializer(self, data):
        return json.dumps(data).encode("utf-8")

    def run_strategy(self):
        producer = KafkaProducer(bootstrap_servers=['167.99.156.175:9092'], 
                         value_serializer=self.json_serializer)
        while True:
            self.ib.sleep(10)
            if len(self.bars) > 0:
                self.ib.sleep(1)
                bar = self.bars[-1]
                bar_dic = {'time':str(pd.to_datetime(bar.time).tz_convert('US/Eastern').tz_localize(None)), 
                           'open':self.x_round(bar.open_), 'high':self.x_round(bar.high), 'low':self.x_round(bar.low), 
                           'close':self.x_round(bar.close), 'volume':bar.volume}
                print(bar_dic)
                producer.send(self.symbol, bar_dic)
            
if __name__ == '__main__':
    symbol = 'MNQ'  #input('\tsymbol: ')
    live_range = Producer(symbol='MNQ', temp='1 min', client=100)
    live_range.run_strategy()