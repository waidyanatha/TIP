#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

try:
    # from distutils import filelist
    # from tkinter import NS
    # from turtle import pd

    import numpy as np
    from datetime import datetime, timedelta, date
    import pandas as pd
    import traceback

    print("All packages loaded successfully!")

except Exception as e:
    print("Some packages didn't load\n{}".format(e))

'''
    CLASS that calculates the ortfolio performance and returns the indicators:
        1) 
'''
class PortfolioPerformance():

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
        self.p_val = 1.0         # default null hypothesis testing & returns all results < p_val cutt off

        return None

    ''' Function
            name: sharp_ratio
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def sharp_ratio(self, data_df, investment = 100, risk_free_rate=0.02, **params):

        avg_simple_returns = pd.Series(dtype='float64')
        std_simple_returns = pd.Series(dtype='float64')
        sharp_ratio = pd.Series(dtype='float64')

        try:
            _l_coin_ids = [col for col in data_df.columns if col != 'Date']
            simple_returns = data_df[_l_coin_ids].pct_change(periods=1)
            simple_returns["Date"] = data_df["Date"].astype('datetime64[ns]')
            avg_simple_returns = simple_returns[_l_coin_ids].mean()
            std_simple_returns = simple_returns[_l_coin_ids].std()
            risk_free_rate = avg_simple_returns['bitcoin']
            print(risk_free_rate)
            sharp_ratio = (avg_simple_returns - risk_free_rate) / std_simple_returns

        except Exception as err:
            _s_fn_id = "Class <PortfolioPerformance> Function <sharp_ratio>"
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

        avg_simple_returns = pd.Series(dtype='float64')
        std_simple_returns = pd.Series(dtype='float64')
        sortino_ratio = pd.Series(dtype='float64')

        try:
            _l_coin_ids = [col for col in data_df.columns if col != 'Date']
            simple_returns = data_df[_l_coin_ids].pct_change(periods=1)
            simple_returns["Date"] = data_df["Date"].astype('datetime64[ns]')
#            _l_coin_ids = [col for col in simple_returns if col != 'Date']
            avg_simple_returns = simple_returns[_l_coin_ids].mean()
            _down_simple_returns = simple_returns.copy()
            _l_cols = [col for col in _down_simple_returns if col !='Date']
            _down_simple_returns[_l_cols] = _down_simple_returns[_down_simple_returns[_l_cols] < 0][_l_cols]
            std_simple_returns = _down_simple_returns[_l_coin_ids].std()
            risk_free_rate = avg_simple_returns['bitcoin']
            sortino_ratio = (avg_simple_returns - risk_free_rate) / std_simple_returns

        except Exception as err:
            _s_fn_id = "Class <PortfolioPerformance> Function <sortino_ratio>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return sortino_ratio

    ''' Function
            name: get_adx
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def get_adx(self,
                ticker_data : pd.DataFrame,
                window_start_date : date,
                window_end_date : date,
                rolling_window_length=7,
                value_col_name='market_cap',
                ):

        adx_df = pd.DataFrame()

        try:
            if not (ticker_data.shape[0] > 0):
                raise ValueError("Invalid dataframe with %d rows" % (ticker_data.shape[0]))
            adx_df = ticker_data.copy()
            print(adx_df.columns)
            ''' Calculate the True range which is the log_ROR available in the input dataframe '''
            ''' TODO fix the get ror to give an error and be a function outside of this function to compute ror'''
#            if not 'ror' in adx_df.columns:
            if not value_col_name in adx_df.columns:
                raise ValueError("No data run function first")
#                 ''' Initialize class to calculate ror '''
#                 import sys
#                 # sys.path.insert(1, '../lib')
#                 import clsETPreturns as returns
#                 clsROR = returns.RateOfReturns(name="adxData")
#                 adx_df = clsROR.get_logarithmic_returns(adx_df, value_col_name=value_col_name)
#                 adx_df.dropna(axis=0, how='any', inplace=True)
            ''' Positive Directional Movement --> log_ROR <= 1; else set to 0 '''
            adx_df['+DM']=adx_df[value_col_name]
            adx_df['+DM']=np.where(adx_df['+DM'] <= 0, adx_df['+DM'].abs(), 0)
            ''' Negative Directional Movement --> log_ROR > 1; else set to 0 '''
            adx_df['-DM']=adx_df[value_col_name]
            adx_df['-DM']=np.where(adx_df['-DM'] > 0, adx_df['-DM'].abs(), 0)
            ''' Smoothed values '''
            import sys
            sys.path.insert(1, '../lib')
            import clsDataETL as etl
            # ''' TODO fix the path dependency in ETL '''
            ''' REMOVE after debuggin complete '''
            import importlib
            etl = importlib.reload(etl)
            clsETL = etl.ExtractLoadTransform()
            _cal_ops_dict = {
                "simp_move_sum" : "+DM",
                "simp_move_avg" : "+DM",
                }
            adx_df = clsETL.get_rolling_measures(ticker_data=adx_df,
                                                rolling_window_length=7,
                                                window_start_date = window_start_date,
                                                window_end_date = window_end_date,
                                                rolling_measure_dict = _cal_ops_dict,)
            _cal_ops_dict = {
                "simp_move_sum" : "-DM",
                "simp_move_avg" : "-DM",
                }
            adx_df = clsETL.get_rolling_measures(ticker_data=adx_df,
                                                rolling_window_length=7,
                                                window_start_date = window_start_date,
                                                window_end_date = window_end_date,
                                                rolling_measure_dict = _cal_ops_dict,)
            ''' The Positive Index Indicator and Negative Index Indicator '''
            for col in adx_df.filter(items=['+DM','-DM']).columns:
                adx_df['shift_'+col]=adx_df[col].shift(1, axis = 0)
            adx_df[adx_df.filter(like='DM').columns]=adx_df[adx_df.filter(like='DM').columns].fillna(value=0)
            adx_df['smooth+DM']=(adx_df['simp_move_sum_+DM'].subtract(adx_df['simp_move_avg_+DM'])).add(adx_df['shift_+DM'])
            adx_df['smooth-DM']=(adx_df['simp_move_sum_-DM'].subtract(adx_df['simp_move_avg_-DM'])).add(adx_df['shift_-DM'])
            ''' ADX Indicator: Final Calculations '''
            adx_df['+DI']=adx_df['simp_move_avg_+DM'].div(adx_df['smooth+DM'])
            adx_df['-DI']=adx_df['simp_move_avg_-DM'].div(adx_df['smooth-DM'])

            # _l_coin_ids = [col for col in adx_df.columns if col != 'Date']

        except Exception as err:
            _s_fn_id = "Class <PortfolioPerformance> Function <get_adx>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return adx_df

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
    @staticmethod
    def rebalance_etp(self, data_df: pd.DataFrame()):

        import pandas as pd
        import datetime as dt

        ''' initialize return variable '''
        new_etp = pd.DataFrame()

        try:
            if not (data_df.shape[0] > 0):
                raise ValueError("Invalid dataframe with %d rows" % data_df.shape[0])

            ''' initialize vars '''
            _rolling_period = 7
            _win_end_dt = dt.datetime.today()
            _win_start_dt = dt.datetime.today() - dt.timedelta(_rolling_period)

            rec_sma_marketcap_df = clsETL.rolling_mean(data_df,
                                                        period=7,
                                                        value_col_name='market_cap',
                                                        window_start_date=_win_start_dt,
                                                        window_end_date=_win_end_dt)
            rec_smd_marketcap_df = clsETL.rolling_stdv(data_df, period=7, value_col_name='market_cap')

        except Exception as err:
            _s_fn_id = "Class <PortfolioPerformance> Function <sortino_ratio>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return new_etp

