import akshare as ak

def get_all_stock_list():
    # 获取当前沪深 A 股的股票列表
    stock_info_a_code_name_df = ak.stock_info_a_code_name()
    print(stock_info_a_code_name_df)
    stock_info_a_code_name_df.to_csv("./akshare_all_stock_list.csv", index=False, encoding='utf-8-sig')

def get_stock_data_single():
    data = ak.stock_zh_a_minute('sh000001', '5')
    data2 = ak.stock_zh_a_minute('sz000001', '5')
    dd = ak.index_zh_a_hist_min_em('000001', '5', '2025-05-22', '2025-05-29')
    print('t')

if __name__ == '__main__':
    # get_all_stock_list()
    get_stock_data_single()