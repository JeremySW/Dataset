import ccxt
import re

class CoinDictParser(object):

    def __init__(self):
        exchanges_list = ccxt.exchanges
        coin_list = []

        for i in exchanges_list:
            # exc = getattr(ccxt, i)
            exc = eval ('ccxt.%s ()' % i)
            try:
                coin_list.append(list(exc.load_markets().keys()))
            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout,
                        IndexError, ccxt.NetworkError) as error:
                coin_list.append(error)

        coin_dict = {}
        i = 0
        while i < len(coin_list):
            if(type(coin_list[i]) == list):
                coin_dict[exchanges_list[i]] = {'all_assets':coin_list[i]}
            i += 1

        i = 0
        while i < len(coin_dict):
            pair, future, swap, option, others = self.convert_contract(coin_dict[list(coin_dict.keys())[i]]['all_assets'])
            coin_dict[list(coin_dict.keys())[i]]['pair'] = {'size': len(pair),'detail':pair}
            coin_dict[list(coin_dict.keys())[i]]['future'] = {'size': len(future),'detail':future}
            coin_dict[list(coin_dict.keys())[i]]['swap'] = {'size': len(swap),'detail':swap}
            coin_dict[list(coin_dict.keys())[i]]['option'] = {'size': len(option),'detail':option}
            coin_dict[list(coin_dict.keys())[i]]['others'] = {'size': len(others),'detail':others}
            i += 1
        self.coin_dict = coin_dict
        return

    def convert_contract(self, data):
        pair = []
        future = []
        swap = []
        option = []
        others = []

        pair_re = re.compile(r'\w{1,}/\w{1,}$')
        future_re = re.compile(r'\w{1,}/\w{1,}:\w{1,}-\d{6}$')
        swap_re = re.compile(r'\w{1,}/\w{1,}:\w{1,}$')
        option_re = re.compile(r'\w{1,}/\w{1,}:\w{1,}-\d{6}-\d{1,}-[P|C]$')
        option_re1 = re.compile(r'\w{1,}/\w{1,}:\w{1,}-\d{6}:\d{1,}:[P|C]$')
        i = 0
        while i < len(data):
            if pair_re.match(data[i]):
                pair.append(data[i])
            elif future_re.match(data[i]):
                future.append(data[i])
            elif swap_re.match(data[i]):
                swap.append(data[i])
            elif option_re.match(data[i]) or option_re1.match(data[i]):
                option.append(data[i])
            else:
                others.append(data[i])
            i += 1
        return pair, future, swap, option, others

    def getAssetType(self):
        return ['all_assets', 'pair', 'future', 'swap', 'option', 'others']

    def getExchangeList(self):
        return list(self.coin_dict.keys())

    def getAssetInOneExchange(self, exchange_name, asset_type):
        '''
        :param exchange_name: e.g. 'binance'
        :param asset_type: 'all_assets', 'pair', 'future', 'swap', 'option', 'others'
        :return: asset_list
        '''
        return self.coin_dict[exchange_name][asset_type]['detail']

    def getAssetListInOneExchange(self, exchange_name):
        return self.coin_dict[exchange_name]['all_assets']

    def searchPairIncludeBaseCurrency(self, coin_name, exchange_name):
        pair_list = self.coin_dict[exchange_name]['pair']['detail']
        i = 0
        result_base = []
        result_quote = []
        while i < len(pair_list):
            if re.match(r'.{0,}/'+coin_name+'$', pair_list[i]):
                result_quote.append(pair_list[i])
            elif re.match(coin_name+'/.{0,}'+'$', pair_list[i]):
                result_base.append(pair_list[i])
            i += 1

        return result_quote, result_base
