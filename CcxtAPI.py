import socket

import ccxt
import time
import requests

# TEST

class CcxtAPI(object):
    def __init__(self):
        self.msec = 1000
        self.minute = 60 * self.msec
        self.minute_frame = 1
        self.hold = 10
        return

    def get_all_exchange_from_ccxt(self):
        return list(ccxt.exchanges)

    def get_all_symbols_from_exchange(self, exchange):
        return list(exchange.load_markets().keys())

    def get_ohlcv_from_ccxt(self, coin_symbol, exchange_id, from_timestamp, to_timestamp, time_frame):
        """
            通过CCXT获取加密货币数据
            :param coin_symbol    加密货币标志（e.g. BTC/USDT；需要通过币的名字转化）
            :param exchange_id
            :param from_date
            :param to_date
            :param time_frame   s, m, h, d, w, M, Y
            此处exchange对象不在CcxtAPI类中初始化，防止重复实例化多个同一交易所对象，导致超过最大请求limit
            :return 二维list数组
        """
        exchange = eval ('ccxt.%s ()' % exchange_id)
        # exchange.load_markets()
        # from_timestamp = exchange.parse8601(from_date)
        # to_timestamp = exchange.parse8601(to_date)
        now = min(exchange.milliseconds(), to_timestamp)
        fetch_failed_times = 0
        data = []

        while from_timestamp < now and fetch_failed_times < 2:
            try:
                ohlcvs = exchange.fetch_ohlcv(coin_symbol, time_frame, from_timestamp, limit=1500)

                if ohlcvs and len(ohlcvs) > 0:
                    first = ohlcvs[0][0]
                    last = ohlcvs[-1][0]
                    print('First candle epoch', first, exchange.iso8601(first))
                    print('Last candle epoch', last, exchange.iso8601(last))
                    ohlcvs = [line for line in ohlcvs if line[0] < now]
                    data += ohlcvs

                    from_timestamp = last + self.minute * self.minute_frame
                else:
                    fetch_failed_times += 1
                    from_timestamp = from_timestamp + self.minute * self.minute_frame

            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable,
                    ccxt.RequestTimeout, IndexError, ccxt.BaseError, ccxt.NetworkError, socket.timeout) as error:
                print('Got an error', type(error).__name__, error.args, ', retrying in',
                      self.hold, 'seconds...')
                time.sleep(self.hold)
        return data

if __name__ == '__main__':
    ccxt_api = CcxtAPI()
    exchange = eval('ccxt.%s ()' % 'binance')
    print(ccxt_api.get_all_symbols_from_exchange(exchange))

