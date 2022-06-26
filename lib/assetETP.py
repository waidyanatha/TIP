#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS with essential timeseries evaluation properties and methods:
        1) 
'''
class ExchangeTradeProtocol():

    import pandas as pd

    ''' Function
            name: __init__
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def __init__(self, name : str="data"):

        self.name = name
        ''' Paramenter default values '''
        self.days_offset = 0     # start window at minimum date point
        self.window_length = 7   # window length set to 7 days
        self.p_val = 1.0         # default null hypothesis testing & returns all results < p_val cutt off

        return None

    ''' Function
            name: get_expected_returns
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def sum_weighted_returns(self, data_df : pd.DataFrame, weights):

        import traceback
        import pandas as pd
        import numpy as np

#        expected_returns_df = pd.DataFrame()
        _l_exp_ret = []

        try:
            if not (data_df.shape[0] > 0):
                raise ValueError("Invalid dataframe")
            _l_dates = list(data_df['Date'].unique())

            for date in _l_dates:
                _top_assets_byDate_df = data_df.loc[data_df['Date'] == date]
                _top_asset_arr = np.array(_top_assets_byDate_df['market_cap'])
                weighted_return_arr = np.multiply(_top_asset_arr,weights)
                sum_weighted_returns = np.sum(weighted_return_arr, axis=1)
                _l_exp_ret.append({'Date' : date, 'Expected Return' : sum_weighted_returns})  

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <get_expected_returns>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_exp_ret

    ''' Function
            name: get_simple_returns
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def get_simple_returns(self, data_df):

        _l_coin_ids = [col for col in data_df.columns if col != 'Date']
        simple_returns_df = data_df[_l_coin_ids].pct_change(periods=1)
        simple_returns_df["Date"] = data_df["Date"].astype('datetime64[ns]')

        return simple_returns_df

    ''' Function
            name: get_weighted_index
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def get_topN_assets(self,data_df, N = 3):

        import traceback
        import pandas as pd

        try:
            _l_dates = data_df['Date'].unique()
            _l_topNassets = []
            topNAssets = pd.DataFrame([],columns=['Date','ID','market_cap'])
            for date in _l_dates:
                ''' get assets and sort by market cap '''
                assets = data_df.loc[data_df['Date'] == date]
                assets = assets.sort_values(by='market_cap',axis=0, ascending = False)
                _l_assetsID = []
                _l_marketCap = []
                for row in assets.head(N).iterrows():
                    asset_dict = {'Date' : date, 'ID' : row[1][1], 'market_cap' : row[1][3]}
                    topNAssets = pd.concat([topNAssets,pd.DataFrame([asset_dict])])

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <get_topN_assets>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return topNAssets

    ''' Function
            name: weights_matrix
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def weights_matrix(self, N=3,S=10):

        import numpy as np   #numpy.random
        import traceback

        try:
            rand_arr = []
            rand_arr.append(np.random.dirichlet(np.ones(N),size=S))

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <weights_matrix>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return np.around(rand_arr,4)

    ''' Function
            name: rolling_mean
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def rolling_mean(self, data_df, period=7):

        import traceback
        import pandas as pd

        _rolling_mean = pd.DataFrame()

        try:
            _l_coin_ids = [col for col in data_df if col !='Date']
            _rolling_mean = data_df.copy()
            _rolling_mean[_l_coin_ids] = _rolling_mean[_l_coin_ids].rolling(period).mean()

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <rolling_mean>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _rolling_mean

    ''' Function
            name: rolling_stdv
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def rolling_stdv(self, data_df, period=7):

        import traceback
        import pandas as pd

        _rolling_stdv = pd.DataFrame()

        try:
            _l_coin_ids = [col for col in data_df if col !='Date']
            _rolling_stdv = data_df.copy()
            _rolling_stdv[_l_coin_ids] = _rolling_stdv[_l_coin_ids].rolling(period).std()

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <rolling_stdv>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _rolling_stdv

    ''' Function
            name: rolling_corr
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def __main__(self, start_dt, end_dt):

        import pandas as pd
        import datetime
        from datetime import timedelta, date

        try:
            print('Under construction')

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <__main__>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return corr_matrix

