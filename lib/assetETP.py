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
            name: value_index
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