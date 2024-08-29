from CcxtAPI import CcxtAPI
from MongoDBLink import MongoDBLink
from ParseDatetime import ParseDatetime
from CoinDictParser import CoinDictParser
from datetime import datetime

MONGODB_INDEX = {
    'url': 'localhost',
    'port': 27017,
    'database_name': 'db_trade_record_temp',
    'collection_name': ''
}

def request_data_and_save(asset_name, exchange_name, from_stamp, to_stamp, target_collection, ccxt_link, db_link):
    # time_now = ParseDatetime()
    # ccxt_link = CcxtAPI()
    # 从前一天到后一天；统一timestamp为13位精确到ms
    # data = ccxt_link.get_ohlcv_from_ccxt(asset_name, exchange_name,
    #                                      time_now.s_to_ms_timestamp(time_now.get_now_stamp_chime(-1, 'days')),
    #                                      time_now.s_to_ms_timestamp(time_now.get_now_stamp_chime(0, 'days')),
    #                                      '1s')
    data = ccxt_link.get_ohlcv_from_ccxt(asset_name, exchange_name, from_stamp, to_stamp, '1m')
    if len(data) == 0:
        return 1
    # 连接MongoDB数据库
    # db_link = MongoDBLink(MONGODB_INDEX['url'], MONGODB_INDEX['port'], MONGODB_INDEX['database_name'])
    # print(data[0][0])
    data = db_link.parse_ohlcvs_to_json(data, asset_name)
    db_link.save_ohlcv_data_to_mongo(target_collection, data)
    return 0


if __name__ == '__main__':
    time_now = ParseDatetime()
    print('Time Parser initialized! ')
    coin_parser = CoinDictParser()
    print('CoinDict Parser initialized! ')
    ccxt_link = CcxtAPI()
    print('CCXT controller initialized! ')
    db_link = MongoDBLink(MONGODB_INDEX['url'], MONGODB_INDEX['port'], MONGODB_INDEX['database_name'])
    print('MongoDB controller initialized! ')
    exchange_name = 'binance'
    empty_data_asset = []

    from_stamp = time_now.s_to_ms_timestamp(time_now.get_now_stamp_chime(-1, 'days'))
    # to_stamp = time_now.s_to_ms_timestamp(time_now.get_now_stamp_chime(0, 'days'))
    to_stamp = from_stamp + 86400000

    assets_in_binance = coin_parser.getAssetInOneExchange(exchange_name, 'pair')
    print('Asset list initialized! ')
    # btc_binance_quote, btc_binance_base = coin_parser.searchPairIncludeBaseCurrency('BTC', exchange_name)
    data_dict = {}
    i = 0
    while i < len(assets_in_binance):
        # collection_name = exchange_name+'_'+assets_in_binance[i]
        collection_name = exchange_name
        print('Iteration '+str(i)+' in '+str(len(assets_in_binance))+': '+assets_in_binance[i])

        # success = request_data_and_save(assets_in_binance[i], exchange_name, from_stamp, to_stamp, collection_name, ccxt_link, db_link)
        # if success == 0:
        #     print(assets_in_binance[i]+' collected and saved!')
        # else:
        #     empty_data_asset.append(i)
        #     print(assets_in_binance[i]+' data not found!')

        data = ccxt_link.get_ohlcv_from_ccxt(assets_in_binance[i], exchange_name, from_stamp, to_stamp, '1m')
        if data != None:
            data_dict[assets_in_binance[i]] = data
        i += 1

    i = 0
    data_store = {}
    pair_manif = list(data_dict.keys())
    while i < len(pair_manif):
        j = 0
        while j < len(data_dict[pair_manif[i]]):
            data_store[datetime.utcfromtimestamp(data_dict[pair_manif[i]][j][0]/1000)] = {'Open': data_dict[pair_manif[i]][j][1],
                                                                                          'High': data_dict[pair_manif[i]][j][2],
                                                                                          'Low': data_dict[pair_manif[i]][j][3],
                                                                                          'Close': data_dict[pair_manif[i]][j][4],
                                                                                          'Volume': data_dict[pair_manif[i]][j][5]}
            j += 1
        i += 1
    store_list = []
    for i in data_store:
        store_list.append({i: data_store[i]})
    db_link.save_ohlcv_data_to_mongo('binance', data)

    print('Finished! ')
    print("Pairs with no records: "+empty_data_asset)
