from downloadManager import mutilDownloadMarketData
from moer_chanlunArgs import moer_analy
from moer_chanlun_third_buy import moer_chanlun_third_buy

if __name__ == '__main__':
    period = '2d'
    interval_list = ['1d', '4h', '30m', '5m']
    # interval_list = ['1wk']
    mutilDownloadMarketData(interval_list, period)
    moer_analy()
    moer_chanlun_third_buy()
