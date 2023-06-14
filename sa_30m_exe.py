from operators import *
from binance_f import RequestClient
from binance_f.model import *
import numpy as np
import pandas as pd
import json
import sys
import gc
import logging
import os
import datetime
from binance.client import Client
import concurrent.futures
import random
import time

prod = 1
path = os.getcwd()
f = open(sys.argv[1], "r")
configs = json.loads(f.read())
key = configs['key']
secret = configs['secret']
alloc_usdt = configs['allocation']
filename = 'MD_5M_rev30M'
sub = '/data/'
file_path = path + sub + filename + '.csv'
lookback = 500
window = 80
window0 = 60
window1 = 65
window2 = 70
window3 = 75
window4 = 85
window5 = 90
window6 = 95
window7 = 100
window8 = 105
window9 = 110
trade_interval = 30


if not os.path.exists(path + sub):
    os.mkdir(path + sub)
elif os.path.exists(file_path):
    close = pd.read_csv(file_path, index_col=[0]).iloc[-lookback:, :]
os.environ['NUMEXPR_MAX_THREADS'] = '32'

logging.basicConfig(filename="sa_rev_balanced.log", format='%(asctime)s %(message)s', filemode='w', level=logging.INFO)
request_client = RequestClient(api_key=key, secret_key=secret)
binance_client = Client(key, secret)

q_map = {'BTCUSDT': 3, 'ETHUSDT': 3, 'BCHUSDT': 3, 'XRPUSDT': 1, 'EOSUSDT': 1, 'LTCUSDT': 3, 'TRXUSDT': 0, 'ETCUSDT': 2, 'LINKUSDT': 2, 'XLMUSDT': 0, 'ADAUSDT': 0, 'XMRUSDT': 3, 'DASHUSDT': 3, 'ZECUSDT': 3, 'XTZUSDT': 1, 'BNBUSDT': 2, 'ATOMUSDT': 2, 'ONTUSDT': 1, 'IOTAUSDT': 1, 'BATUSDT': 1, 'VETUSDT': 0, 'NEOUSDT': 2, 'QTUMUSDT': 1, 'IOSTUSDT': 0, 'THETAUSDT': 1, 'ALGOUSDT': 1, 'ZILUSDT': 0, 'KNCUSDT': 0, 'ZRXUSDT': 1, 'COMPUSDT': 3, 'OMGUSDT': 1, 'DOGEUSDT': 0, 'SXPUSDT': 1, 'KAVAUSDT': 1, 'BANDUSDT': 1, 'RLCUSDT': 1, 'WAVESUSDT': 1, 'MKRUSDT': 3, 'SNXUSDT': 1, 'DOTUSDT': 1, 'DEFIUSDT': 3, 'YFIUSDT': 3, 'BALUSDT': 1, 'CRVUSDT': 1, 'TRBUSDT': 1, 'YFIIUSDT': 3, 'RUNEUSDT': 0, 'SUSHIUSDT': 0, 'SRMUSDT': 0, 'BZRXUSDT': 0, 'EGLDUSDT': 1, 'SOLUSDT': 0, 'ICXUSDT': 0, 'STORJUSDT': 0, 'BLZUSDT': 0, 'UNIUSDT': 0, 'AVAXUSDT': 0, 'FTMUSDT': 0, 'HNTUSDT': 0, 'ENJUSDT': 0, 'FLMUSDT': 0, 'TOMOUSDT': 0, 'RENUSDT': 0, 'KSMUSDT': 1, 'NEARUSDT': 0, 'AAVEUSDT': 1, 'FILUSDT': 1, 'RSRUSDT': 0, 'LRCUSDT': 0, 'MATICUSDT': 0, 'OCEANUSDT': 0, 'CVCUSDT': 0, 'BELUSDT': 0, 'CTKUSDT': 0, 'AXSUSDT': 0, 'ALPHAUSDT': 0, 'ZENUSDT': 1, 'SKLUSDT': 0, 'GRTUSDT': 0, '1INCHUSDT': 0, 'BTCBUSD': 3, 'AKROUSDT': 0, 'CHZUSDT': 0, 'SANDUSDT': 0, 'ANKRUSDT': 0, 'LUNAUSDT': 0, 'BTSUSDT': 0, 'LITUSDT': 1, 'UNFIUSDT': 1, 'DODOUSDT': 1, 'REEFUSDT': 0, 'RVNUSDT': 0, 'SFPUSDT': 0, 'XEMUSDT': 0, 'BTCSTUSDT': 1, 'COTIUSDT': 0, 'CHRUSDT': 0, 'MANAUSDT': 0, 'ALICEUSDT': 1, 'BTCUSDT_210625': 3, 'ETHUSDT_210625': 3, 'HBARUSDT': 0, 'ONEUSDT': 0, 'LINAUSDT': 0, 'STMXUSDT': 0, 'DENTUSDT': 0, 'CELRUSDT': 0, 'HOTUSDT': 0, 'MTLUSDT': 0, 'OGNUSDT': 0, 'BTTUSDT': 0, 'NKNUSDT': 0, 'SCUSDT': 0, 'DGBUSDT': 0, '1000SHIBUSDT': 0, 'ICPUSDT': 2, 'BAKEUSDT': 0, 'GTCUSDT': 1, 'ETHBUSD': 3, 'BTCUSDT_210924': 3, 'ETHUSDT_210924': 3}
p_map = {'BTCUSDT': 2, 'ETHUSDT': 2, 'BCHUSDT': 2, 'XRPUSDT': 4, 'EOSUSDT': 3, 'LTCUSDT': 2, 'TRXUSDT': 5, 'ETCUSDT': 3, 'LINKUSDT': 3, 'XLMUSDT': 5, 'ADAUSDT': 5, 'XMRUSDT': 2, 'DASHUSDT': 2, 'ZECUSDT': 2, 'XTZUSDT': 3, 'BNBUSDT': 3, 'ATOMUSDT': 3, 'ONTUSDT': 4, 'IOTAUSDT': 4, 'BATUSDT': 4, 'VETUSDT': 6, 'NEOUSDT': 3, 'QTUMUSDT': 3, 'IOSTUSDT': 6, 'THETAUSDT': 4, 'ALGOUSDT': 4, 'ZILUSDT': 5, 'KNCUSDT': 5, 'ZRXUSDT': 4, 'COMPUSDT': 2, 'OMGUSDT': 4, 'DOGEUSDT': 6, 'SXPUSDT': 4, 'KAVAUSDT': 4, 'BANDUSDT': 4, 'RLCUSDT': 4, 'WAVESUSDT': 4, 'MKRUSDT': 2, 'SNXUSDT': 3, 'DOTUSDT': 3, 'DEFIUSDT': 1, 'YFIUSDT': 1, 'BALUSDT': 3, 'CRVUSDT': 3, 'TRBUSDT': 3, 'YFIIUSDT': 1, 'RUNEUSDT': 4, 'SUSHIUSDT': 4, 'SRMUSDT': 4, 'BZRXUSDT': 4, 'EGLDUSDT': 3, 'SOLUSDT': 4, 'ICXUSDT': 4, 'STORJUSDT': 4, 'BLZUSDT': 5, 'UNIUSDT': 4, 'AVAXUSDT': 4, 'FTMUSDT': 6, 'HNTUSDT': 4, 'ENJUSDT': 5, 'FLMUSDT': 4, 'TOMOUSDT': 4, 'RENUSDT': 5, 'KSMUSDT': 2, 'NEARUSDT': 4, 'AAVEUSDT': 3, 'FILUSDT': 3, 'RSRUSDT': 6, 'LRCUSDT': 5, 'MATICUSDT': 5, 'OCEANUSDT': 5, 'CVCUSDT': 5, 'BELUSDT': 5, 'CTKUSDT': 5, 'AXSUSDT': 5, 'ALPHAUSDT': 5, 'ZENUSDT': 3, 'SKLUSDT': 5, 'GRTUSDT': 5, '1INCHUSDT': 4, 'BTCBUSD': 1, 'AKROUSDT': 5, 'CHZUSDT': 5, 'SANDUSDT': 5, 'ANKRUSDT': 6, 'LUNAUSDT': 4, 'BTSUSDT': 5, 'LITUSDT': 3, 'UNFIUSDT': 3, 'DODOUSDT': 3, 'REEFUSDT': 6, 'RVNUSDT': 5, 'SFPUSDT': 4, 'XEMUSDT': 4, 'BTCSTUSDT': 3, 'COTIUSDT': 5, 'CHRUSDT': 4, 'MANAUSDT': 4, 'ALICEUSDT': 3, 'BTCUSDT_210625': 1, 'ETHUSDT_210625': 2, 'HBARUSDT': 5, 'ONEUSDT': 5, 'LINAUSDT': 5, 'STMXUSDT': 5, 'DENTUSDT': 6, 'CELRUSDT': 5, 'HOTUSDT': 6, 'MTLUSDT': 4, 'OGNUSDT': 4, 'BTTUSDT': 6, 'NKNUSDT': 5, 'SCUSDT': 6, 'DGBUSDT': 5, '1000SHIBUSDT': 6, 'ICPUSDT': 2, 'BAKEUSDT': 4, 'GTCUSDT': 3, 'ETHBUSD': 2, 'BTCUSDT_210924': 1, 'ETHUSDT_210924': 2}

all_tickers = ['BTCUSDT', 'ETHUSDT',  'BNBUSDT',  'XRPUSDT',  'ADAUSDT',  'DOGEUSDT', 'SOLUSDT',  'MATICUSDT',    'TRXUSDT',  'LTCUSDT',  'DOTUSDT',  'AVAXUSDT', 'ATOMUSDT', 'UNIUSDT',  'XMRUSDT',  'ETCUSDT',  'XLMUSDT',  'BCHUSDT',  'FILUSDT',  'HBARUSDT']

def load_hist_data_all(all_tickers):
    print('!!!!')
    def load_hist_data(ticker):
        t = random.randint(1, 10)
        time.sleep(t)
        while True:
            try:
                logging.info("start loading " + ticker)
                klines = binance_client.get_historical_klines(ticker, Client.KLINE_INTERVAL_30MINUTE, "100 hours ago UTC")
                break
            except Exception as e:
                logging.error(e)
        data = pd.DataFrame(klines, columns=['Starttime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Timestamp', 'Volume_qa', 'Numtrades', 'Takerbuyvol', 'Takerbuyvol_qa', 'Ignore'])
        data['Timestamp'] = pd.to_datetime(data['Timestamp'], unit='ms').dt.round('s')
        data = data[['Timestamp', 'Close']]
        data.columns = ['Timestamp', ticker]
        data.set_index('Timestamp', inplace=True)
        data = data.astype('float')
        return data
    logging.info("no hist data, loading it now")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        result = executor.map(load_hist_data, all_tickers)
    data = pd.concat(result, axis=1, sort=True)
    data = data[all_tickers]
    data = data.iloc[:-1, ]
    logging.info('\n')
    logging.info(data.tail())
    data.to_csv(file_path)
    logging.info('saved data to ' + file_path)
    gc.collect()
    return data


def get_price(ticker):
    result = request_client.get_candlestick_data(symbol=ticker, interval=CandlestickInterval.MIN30, startTime=None, endTime=None, limit=1)
    return [ticker, float(result[0].close)]


def append_price(close, all_tickers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(all_tickers)) as executor:
        data_new = executor.map(get_price, all_tickers)
    data_new = {i[0]: i[1] for i in data_new}
    last_update_time = close.index[-1]
    data_new = pd.DataFrame.from_dict(data_new, orient='index').transpose()[all_tickers]
    data_new.index = [datetime.datetime.utcnow().replace(second=0, microsecond=0)]
    data_new = data_new[data_new.index > last_update_time]
#   close = close.append(data_new).iloc[-lookback:, :]
    close = pd.concat([close, data_new]).iloc[-lookback:, :]
    logging.info('\n')
    logging.info(close.tail())
    return data_new, close


def run_strategy(close):
    data_new, close = append_price(close, all_tickers)
    logging.info('finisihed loading new data')
    # mon2 = np.sign(ts_ma_crossover(close, 50, 150))
    # alpha = (-ts_ret(close, window) - ts_ret(close, window0) - ts_ret(close, window1) - ts_ret(close, window2) - ts_ret(close, window3) - ts_ret(close, window4)
            #  - ts_ret(close, window5) - ts_ret(close, window6) - ts_ret(close, window7) - ts_ret(close, window8) - ts_ret(close, window9)) / 10
    alpha = ts_mean(-( close /ts_delay( close ,3)),10)
    # alpha = cs_reg_residue(mon2, alpha)
    alpha = cs_booksize(cs_mktneut(alpha), alloc_usdt)
    alpha = cs_expand_balance(cs_poslimit(alpha, 0.01 * alloc_usdt), alloc_usdt)
    rec = alpha.iloc[-1]
    # print(rec[rec > 0])
    # print(rec[rec < 0])
    ticker_target = rec.to_dict()
    open_pos = request_client.get_position_v2()
    open_pos = {i.symbol: float(i.positionAmt) for i in open_pos}
    ticker_target_open = []
    for i in ticker_target.keys():
        if i in open_pos.keys():
            ticker_target_open.append([i, ticker_target[i], open_pos[i]])
        else:
            ticker_target_open.append([i, ticker_target[i], 0])
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(ticker_target_open)) as executor:
        executor.map(execute_orders, ticker_target_open)
    logging.info('finisihed execution')
    check_open_pos()
    data_new.to_csv(file_path, mode='a', header=False)
    gc.collect()
    return close


def execute_orders(ticker_target_open):
    print(ticker_target_open)
    ticker = ticker_target_open[0]
    target_usdt = ticker_target_open[1]
    open_pos = ticker_target_open[2]
    data = request_client.get_symbol_orderbook_ticker(ticker)[0]
    ask = data.askPrice
    bid = data.bidPrice
    mp = 0.5 * (ask + bid)
    p = int(mp) if p_map[ticker] == 0 else round(float(mp), p_map[ticker])
    diffq = int(target_usdt / float(mp) - float(open_pos)) if q_map[ticker] == 0 else round(target_usdt / float(mp) - float(open_pos), q_map[ticker])
    ask = int(ask) if p_map[ticker] == 0 else round(float(ask), p_map[ticker])
    bid = int(bid) if p_map[ticker] == 0 else round(float(bid), p_map[ticker])
    side = 'BUY' if diffq > 0 else 'SELL'
    request_client.cancel_all_orders(ticker)
    if diffq != 0:
        if abs(diffq * p) >= 5:
            if prod == 1:
                if diffq > 0:
                    try:
                        print(request_client.post_order(symbol=ticker, price=str(bid), side=side, ordertype='LIMIT', quantity=str(diffq), timeInForce='GTC'))
                    except Exception as e:
                        logging.error(ticker, e)
                else:
                    try:
                        request_client.post_order(symbol=ticker, price=str(ask), side=side, ordertype='LIMIT', quantity=str(-diffq), timeInForce='GTC')
                    except Exception as e:
                        logging.error(ticker, e)
        else:
            if prod == 1:
                if diffq > 0 and open_pos < 0:
                    if p * diffq < -open_pos:
                        try:
                            request_client.post_order(symbol=ticker, price=str(bid), side=side, ordertype='LIMIT', quantity=str(diffq), timeInForce='GTC', reduceOnly=True)
                        except Exception as e:
                            logging.error(ticker, e)
                elif diffq < 0 and open_pos > 0:
                    if -p * diffq < open_pos:
                        try:
                            request_client.post_order(symbol=ticker, price=str(ask), side=side, ordertype='LIMIT', quantity=str(-diffq), timeInForce='GTC', reduceOnly=True)
                        except Exception as e:
                            logging.error(ticker, e)


def check_open_pos():
    open_pos = request_client.get_position_v2()
    long_pos = 0
    long_pos_pnl = 0
    short_pos = 0
    short_pos_pnl = 0
    for i in open_pos:
        if float(i.positionAmt) > 0:
            long_pos += round(float(i.positionAmt) * float(i.markPrice), 2)
            long_pos_pnl += i.unrealizedProfit
        else:
            short_pos += round(float(i.positionAmt) * float(i.markPrice), 2)
            short_pos_pnl += i.unrealizedProfit
    logging.info('open positions: long_pos ' + str(round(long_pos, 2)) + ' long_pos_pnl ' + str(round(long_pos_pnl, 2)))
    logging.info('open positions: short_pos ' + str(round(short_pos, 2)) + ' short_pos_pnl ' + str(round(short_pos_pnl, 2)))
    return


if __name__ == '__main__':
    logging.info('trading started with interval of ' + str(trade_interval) + 'm')

    if not os.path.exists(file_path):
        load_hist_data_all(all_tickers)
    close = pd.read_csv(file_path, index_col=[0]).iloc[-lookback:, :]
    while True:
        current_time = datetime.datetime.utcnow()
        if current_time.minute % trade_interval == 0 and current_time.second < 2:
            logging.info('utc time: ' + str(current_time))
            try:
                close = run_strategy(close)
            except Exception as e:
                logging.error(e)
        time.sleep(0.5)

   # close = run_strategy(close)
