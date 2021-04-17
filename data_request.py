#
# Python Script with data request code
#
# (c) Andres Estrada Cadavid
# QSociety

import pandas as pd
import numpy as np
from ib_insync import IB, util, Future, Stock, Forex
from datetime import datetime
import requests

class request(IB):
    def __init__(self, symbol, temp, client):
        self.symbol = symbol
        self.temp = temp
        instruments = pd.read_csv('instruments.csv').set_index('symbol')
        self.params = instruments.loc[self.symbol]
        self.market = str(self.params.market)
        self.exchange = str(self.params.exchange)
        self.tick_size = float(self.params.tick_size)
        self.digits = int(self.params.digits)
        self.leverage = int(self.params.leverage)
        self.client = client
        self.current_date()
        self._sundays_activation()
        self.ib = IB()
        print(self.ib.connect('127.0.0.1', 7497, self.client))
        self.connected = self.ib.isConnected()   #######
        self.get_contract()
        self.interrumption = False
        #self.data = self.download_data(tempo=temp, duration='1 D')
        #self.ib.reqMktData(self.contract, '', False, False); self.ticker = self.ib.ticker(self.contract)  #########
        #self.ticker = self.ib.reqTickByTickData(self.contract, 'Last', 0)
        self.bars = self.ib.reqRealTimeBars(self.contract, 5, 'MIDPOINT', False)
        self.operable = True

    def operable_schedule(self):
        if self.weekday == 4 and pd.to_datetime(self.hour).time() > pd.to_datetime('18:00:00').time():
            print('%s %s | Today is Friday and Market has Closed!'%(self.date, self.hour))
            self.operable = False
        elif self.weekday == 5:
            print('%s %s | Today is Saturday and market is not Opened'%(self.date, self.hour))
            self.operable = False
        else:
            self.operable = True

    def current_date(self):
        self.date = datetime.now().strftime('%Y-%m-%d')
        self.weekday = datetime.now().weekday()
        self.hour = datetime.now().strftime('%H:%M:%S')

    def _sundays_activation(self):
        hour = '18:00:05'
        if self.weekday == 6:
            if pd.to_datetime(self.hour).time() < pd.to_datetime(hour).time():
                print('Today is Sunday. Bot activation is at 18:00:00')
                while True:
                    self.current_date()
                    if pd.to_datetime(self.hour).time() >= pd.to_datetime(hour).time():
                        print('Activation Done')
                        self.send_telegram_message('%s %s | Bot Activation Done' % (self.date, self.hour))
                        break
    
    def continuous_check_message(self, message):
        if datetime.now().minute == 0 and datetime.now().second == 0:
            self.send_telegram_message(message, type='info')
    
    def reconnection(self):
        if self.hour == '23:44:30' or self.hour == '16:59:30':
            self.interrumption = True
            self.ib.disconnect()
            self.connected = self.ib.isConnected()
            print('%s %s | Ib disconnection' % (self.date, self.hour))
            print('Connected: %s' % self.connected)
        if self.hour == '23:46:00' or self.hour == '18:00:05':
            self.interrumption = False
            print('%s %s | Reconnecting...' % (self.date, self.hour))
            while not self.connected:
                try:
                    self.ib.connect('127.0.0.1',7497, self.client)
                    self.connected = self.ib.isConnected()
                    if self.connected:
                        print('%s %s | Connection reestablished!' % (self.date, self.hour))
                        print('Requesting Market Data...')
                        self.bars = self.ib.reqRealTimeBars(self.contract, 5, 'MIDPOINT', False)
                        print('Last Close of %s: %.2f' % (self.symbol, self.bars[-1].close))
                        print('%s Data has been Updated!' % self.symbol)
                except:
                    print('%s %s | Connection Failed! Trying to reconnect in 10 seconds...' % (self.date, self.hour))
                    self.ib.sleep(10)
            print('%s %s | %s Data has been Updated!' % (self.date, self.hour, self.symbol))

    def _local_symbol_selection(self):
        '''Selects local symbol according to symbol and current date'''
        current_date = datetime.now().date()
        # csv file selection according to symbol
        if self.symbol in ['ES', 'RTY', 'NQ', 'MES', 'MNQ', 'M2K']:
            contract_dates = pd.read_csv('D:/Archivos/futuro/Algorithmics/Codes/My_Bots/Hermes/contract_dates/indexes_globex.txt', parse_dates=True)
        elif self.symbol in ['YM', 'MYM', 'DAX']:
            contract_dates = pd.read_csv('D:/Archivos/futuro/Algorithmics/Codes/My_Bots/Hermes/contract_dates/indexes_ecbot_dtb.txt', parse_dates=True)
        elif self.symbol in ['QO', 'MGC']:
            contract_dates = pd.read_csv('D:/Archivos/futuro/Algorithmics/Codes/My_Bots/Hermes/contract_dates/QO_MGC.txt', parse_dates=True)
        elif self.symbol in ['CL', 'QM']: 
            contract_dates = pd.read_csv('D:/Archivos/futuro/Algorithmics/Codes/My_Bots/Hermes/contract_dates/CL_QM.txt', parse_dates=True)
        else: 
            contract_dates = pd.read_csv('D:/Archivos/futuro/Algorithmics/Codes/My_Bots/Hermes/contract_dates/%s.txt'%symbol, parse_dates=True)

        # Current contract selection according to current date
        for i in range(len(contract_dates)):
            initial_date = pd.to_datetime(contract_dates.iloc[i].initial_date).date()
            final_date = pd.to_datetime(contract_dates.iloc[i].final_date).date()
            if initial_date <= current_date <= final_date:
                current_contract = contract_dates.iloc[i].contract
                break
        
        # local symbol selection
        local = current_contract
        if self.symbol in ['ES', 'RTY', 'NQ', 'MES', 'MNQ', 'M2K', 'QO', 'CL', 'MGC', 'QM']:
            local = '%s%s'%(self.symbol, current_contract)
        if self.symbol in ['YM', 'ZS']: local = '%s   %s'%(self.symbol, current_contract)
        if self.symbol == 'MYM': local = '%s  %s'%(self.symbol, current_contract)
        if self.symbol == 'DAX': local = 'FDAX %s'%current_contract
        
        return local

    def get_contract(self):
        if self.market == 'futures':
            local = self._local_symbol_selection()
            self.contract = Future(symbol=self.symbol, exchange=self.exchange, localSymbol=local)
            print(self.ib.reqContractDetails(self.contract)[0].contract.lastTradeDateOrContractMonth)
            '''expiration = self.ib.reqContractDetails(Future(self.symbol,self.exchange))[0].contract.lastTradeDateOrContractMonth
            self.contract = Future(symbol=self.symbol, exchange=self.exchange, lastTradeDateOrContractMonth=expiration)'''
        elif self.market == 'forex':
            self.contract = Forex(self.symbol)
        elif self.market == 'stocks':
            self.contract = Stock(symbol=self.symbol, exchange=self.exchange, currency='USD')
    
    def download_data(self, tempo, duration):
        pr = (lambda market: 'MIDPOINT' if market == 'forex' else 'TRADES')(self.market)
        historical = self.ib.reqHistoricalData(self.contract, endDateTime='', durationStr=duration,
                        barSizeSetting=tempo, whatToShow=pr, useRTH=False, keepUpToDate=False)
        return historical    
    
    def send_telegram_message(self, message, type='trades'):
        bot_token = '1204313430:AAGonra1LaFhyI1gCVOHsz8yAohJUeFgplo'
        bot_chatID = '-499850995' if type=='trades' else '-252750334'
        url = 'https://api.telegram.org/bot%s/sendMessage?chat_id=%s&text=%s'%(bot_token, bot_chatID, message)
    
        requests.get(url)

if __name__ == '__main__':
    symbol = input('\tsymbol: ')
    live = request(symbol=symbol, temp='1 min', client=100)