#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS with essential timeseries evaluation properties and methods:
        1) 
'''
class ExchangeTradeProtocol():

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
            name: value_index
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def get_value_index(self, data_df):

        _l_coin_ids = [col for col in data_df.columns if col != 'Date']
        index_df = data_df[_l_coin_ids].div(data_df[_l_coin_ids].sum(axis=1),axis=0)
        index_df['Date'] = data_df['Date'].astype('datetime64[ns]')

        return index_df

    ''' Function
            name: value_index
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def get_holding_period_return(self, data_df):

        _l_coin_ids = [col for col in data_df.columns if col != 'Date']
        _min_dt = data_df['Date'].min()
        _max_dt = data_df['Date'].max()

        curr_mcap = data_df[data_df['Date'] == _max_dt][_l_coin_ids]
        orig_mcap = data_df[data_df['Date'] == _min_dt][_l_coin_ids]
#        hpr_df = curr_mcap.iloc[0].sub(orig_mcap.iloc[0])

        return (curr_mcap.iloc[0].sub(orig_mcap.iloc[0])).div(orig_mcap.iloc[0])

    ''' Function
            name: value_index
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
            topNAssets = pd.DataFrame([],columns=['Date','ID','Value'])
            for date in _l_dates:
                ''' get assets and sort by market cap '''
                assets = data_df.loc[data_df['Date'] == date]
                assets = assets.sort_values(by='market_cap',axis=0, ascending = False)
                _l_assetsID = []
                _l_marketCap = []
                for row in assets.head(N).iterrows():
                    asset_dict = {'Date' : date, 'ID' : row[1][1], 'Value' : row[1][3]}
                    topNAssets = pd.concat([topNAssets,pd.DataFrame([asset_dict])])

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <get_topN_assets>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return topNAssets

    ''' Function
            name: sharp_ratio
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def sharp_ratio(self, data_df, investment = 100, risk_free_rate=0.02, **params):

        import traceback
        import pandas as pd

        avg_simple_returns = pd.Series()
        std_simple_returns = pd.Series()
        sharp_ratio = pd.Series()

        try:
            simple_returns = self.get_simple_returns(data_df)
            _l_coin_ids = [col for col in simple_returns if col != 'Date']
            avg_simple_returns = simple_returns[_l_coin_ids].mean()
            std_simple_returns = simple_returns[_l_coin_ids].std()
            risk_free_rate = avg_simple_returns['bitcoin']
            print(risk_free_rate)
            sharp_ratio = (avg_simple_returns - risk_free_rate) / std_simple_returns

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <sharp_ratio>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return sharp_ratio

    ''' Function
            name: sortino_ratio
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def sortino_ratio(self, data_df, investment = 100, risk_free_rate=0.02, **params):

        import traceback
        import pandas as pd

        avg_simple_returns = pd.Series()
        std_simple_returns = pd.Series()
        sortino_ratio = pd.Series()

        try:
            simple_returns = self.get_simple_returns(data_df)
            _l_coin_ids = [col for col in simple_returns if col != 'Date']
            avg_simple_returns = simple_returns[_l_coin_ids].mean()
            _down_simple_returns = simple_returns.copy()
            _l_cols = [col for col in _down_simple_returns if col !='Date']
            _down_simple_returns[_l_cols] = _down_simple_returns[_down_simple_returns[_l_cols] < 0][_l_cols]
            std_simple_returns = _down_simple_returns[_l_coin_ids].std()
            risk_free_rate = avg_simple_returns['bitcoin']
            sortino_ratio = (avg_simple_returns - risk_free_rate) / std_simple_returns

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <sortino_ratio>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return sortino_ratio

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
            name: rolling_corr
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def rolling_corr(self, data_df, **params):

        import traceback
        import pandas as pd
        import datetime
        from datetime import timedelta, date

        corr_matrix = data_df.copy()

        try:
            ''' validate dataframe '''
            if not isinstance(data_df,pd.DataFrame) or not data_df.shape[0] > 0:
                raise ValueError("Invalid pandas DataFrame")
            ''' set the number of days to offset from the begining of the time line'''
            if "days_offset" in params.keys():
                self.days_offset = params["days_offset"]
            ''' set the window length in number of days'''
            if "window_length" in params.keys():
                self.window_length = params["window_length"]
            ''' set the whether or not to return statistically significant values '''
            if "p_val_cutoff" in params.keys():
                self.p_val = params["p_val_cutoff"]

            for row_idx in range(0,data_df.shape[0]-self.window_length):
                ''' get the date value from the current datframe'''
                rolling_day = data_df.iloc[row_idx]["Date"]

                ''' Set the left and right side date of the sliding window '''
                rolling_win_left_dt = rolling_day + timedelta(days=self.days_offset)
                rolling_win_right_dt = rolling_win_left_dt + timedelta(days=self.window_length-1)

                mask = (data_df['Date'] >= rolling_win_left_dt) & (data_df['Date'] <= rolling_win_right_dt)
                corr_df = data_df[mask]
                print(corr_df.corr())
                break

        except Exception as err:
            _s_fn_id = "Class <ExchangeTradeProtocol> Function <rolling_corr>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return corr_matrix

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

