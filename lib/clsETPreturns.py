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

    print("All packages in clsETPReturns loaded successfully!")

except Exception as e:
    print("Some packages didn't load\n{}".format(e))

'''
    CLASS with essential timeseries evaluation properties and methods:
        1) 
'''
class RateOfReturns():

    # import pandas as pd
    # import numpy as np

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
    # @staticmethod
    def sum_weighted_returns(self, 
                             data_df : pd.DataFrame,
#                             weights : np.array,   # DEPRECATED because need to account for topN<N
                             size = 100,
                             probability = 1.0,
                             value_col_name = "value"):

        # import traceback
        # import pandas as pd
        # import numpy as np

#        expected_returns_df = pd.DataFrame()
        _l_exp_ret = []
        S=100

        try:
            if not (data_df.shape[0] > 0):
                raise ValueError("Invalid dataframe")

            _l_dates = list(data_df['Date'].unique())
            for date in _l_dates:
                ''' initialize vars'''
                sum_weighted_index=np.nan
                weighted_mc_returns=np.nan
                _max_weight_row=np.nan
                _top_assets_byDate_df = data_df.loc[data_df['Date'] == date]
                rand_arr = []
                ''' get random weights for the number of significant assets '''
                weights=np.random.dirichlet(np.ones(_top_assets_byDate_df.shape[0]),size=size)
#                _top_asset_arr = np.array(_top_assets_byDate_df[value_col_name])
#                weighted_return_arr = np.multiply(_top_asset_arr,weights)
                weighted_return_arr = np.multiply(np.array(_top_assets_byDate_df[value_col_name]),weights)
                ''' compute the market cap weighted sum '''
                sum_weighted_index = np.sum(weighted_return_arr, axis=1)
                _max_weight_row = np.argmax(np.array(sum_weighted_index), axis=0)
                if 'market_cap' in _top_assets_byDate_df.columns:
#                     weighted_mc_returns = np.sum(np.multiply(np.array(_top_assets_byDate_df['market_cap']),weights))
                    weighted_mc_returns = np.sum(np.multiply(np.array(_top_assets_byDate_df['market_cap']),
                                                             weights[_max_weight_row]))
                _l_exp_ret.append({
                                'date' : _top_assets_byDate_df['Date'].max(),
                                # 'date' : str(date.astype('datetime64[D]')),
                                'coins' : list(_top_assets_byDate_df['ID']),
                                'max_sum_row' : _max_weight_row,
                                value_col_name: list(_top_assets_byDate_df[value_col_name]),
                                #'weighted_sum' : sum_weighted_index,
                                'best_weights': list(weights[_max_weight_row]),
                                'weighted_'+value_col_name+'_sum' : sum_weighted_index[_max_weight_row],
                                'weighted_market_cap_returns': weighted_mc_returns,
                                })

        except Exception as err:
            _s_fn_id = "Class <RateOfReturns> Function <sum_weighted_returns>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return pd.DataFrame(_l_exp_ret)

    ''' Function
            name: maximize_weights
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    # @staticmethod
    def maximize_weights(self, sum_weighted_returns:list, value_col_name = "Value"):

        ''' initialize return list '''
        _l_exp_ret = []

        try:
            ''' validate list '''
            if not (len(sum_weighted_returns) > 0):
                raise ValueError('Invalid list weighted sum returns has %d values' %(len(sum_weighted_returns)))

            for row in sum_weighted_returns:
                print(row['max_sum_weights'], row[value_col_name])

        except Exception as err:
            _s_fn_id = "Class <RateOfReturns> Function <maximize_weights>"
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
    # @staticmethod
    def get_simple_returns(self, data_df, value_col_name = "Value"):

        _l_coin_ids = [col for col in data_df.columns if col != 'Date']
        simple_returns_df = data_df[_l_coin_ids].pct_change(periods=1)
        simple_returns_df["Date"] = data_df["Date"].astype('datetime64[ns]')

        return simple_returns_df

    ''' Function
            name: value_index
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    # @staticmethod
    def get_holding_period_returns(self, data_df, value_col_name = "Value"):

        _l_coin_ids = [col for col in data_df.columns if col != 'Date']
        _min_dt = data_df['Date'].min()
        _max_dt = data_df['Date'].max()

        curr_mcap = data_df[data_df['Date'] == _max_dt][_l_coin_ids]
        orig_mcap = data_df[data_df['Date'] == _min_dt][_l_coin_ids]

        return (curr_mcap.iloc[0].sub(orig_mcap.iloc[0])).div(orig_mcap.iloc[0])

    ''' Function
            name: get_logarithmic_return
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    # @staticmethod
    def get_logarithmic_returns(self, data_df : pd.DataFrame, value_col_name = "Value"):
        
        # import traceback
        # import pandas as pd
        # import numpy as np

        _log_return_df = pd.DataFrame()

        try:
            if not (data_df.shape[0] > 0):
                raise ValueError("Invalid dataframe no records found!")

            _l_coin_ids = data_df.ID.unique()
            for c_id in _l_coin_ids:
                coin_df = pd.DataFrame(data_df[data_df['ID']==c_id],columns = data_df.columns)
                coin_df[value_col_name+'_ror'] = np.log(coin_df[value_col_name].pct_change(periods=1)+1)
                _log_return_df = pd.concat([_log_return_df,coin_df])
                # print(_log_return_df['Date'].value_counts())

        except Exception as err:
            _s_fn_id = "Class <RateOfReturns> Function <get_logarithmic_return>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _log_return_df


    ''' name: get_coin_cov_metric
        description: applied only when the investments are layed over fixed period intervals.
        parameters:
            @name (str)
            @clean (dict)
        procedure: 
        return list
    '''
    # @staticmethod
    def get_coin_cov_cor_coef_matrix(self,
                            a_data_df: pd.DataFrame,     # first  dataframe, typically with the actual market cap values
                            b_data_df: pd.DataFrame,     # second dataframe, typically with the market cap moving average
                            suffix = ('_mean','_actual')     # suffix values for renaming the columns in the merge
                           ):

        # import traceback
        # import pandas as pd
        # import numpy as np

        _l_cov_dict = []    # return the dictionary of covariance

        try:
            if not (a_data_df.shape[0] > 0 and b_data_df.shape[0] > 0):
                raise ValueError("One more of the dataframes has no data: dataframe a has %d and b has %d rows"
                                 %(a_data_df.shape[0],b_data_df.shape[0]))

            ''' merge the two dataframes, with inner join, to match the rows ''' 
            _cov_df = pd.merge(a_data_df.dropna(axis=1, how='all', inplace=False),
                               b_data_df.dropna(axis=1, how='all', inplace=False),
                               how='inner', on=['Date'], suffixes=('_mean', '_actual'))
            ''' extract the coind id columns from both dataframes '''
            _l_coin_ids_a = [col for col in _cov_df if (col != 'Date' and '_actual' in col)]
            _l_coin_ids_b = [col for col in _cov_df if (col != 'Date' and '_mean' in col)]
            ''' prepare sets to find the intersection of the two lists for a common set of coin ids '''
            _set_coin_ids_a = set([coin.replace('_actual','') for coin in _l_coin_ids_a])
            ''' compute the covariance for each coin id '''
            _set_coin_ids_b = set([coin.replace('_mean','') for coin in _l_coin_ids_b])
            for coin in sorted(list(_set_coin_ids_b.intersection(_set_coin_ids_a)), reverse=False):
                _coin_df = _cov_df.dropna(axis=0, how='any',subset=[coin+'_mean',coin+'_actual'])
                _l_a_vals = _coin_df[coin+'_actual']
                _l_b_vals = _coin_df[coin+'_mean']
                _l_cov_dict.append({'id': coin,
                                    'cov': np.cov(np.array([_l_a_vals,_l_b_vals]))[0][1],
                                    'corcoef': np.corrcoef(np.array([_l_a_vals,_l_b_vals]))[0][1],
                                    'var': np.cov(np.array([_l_a_vals,_l_b_vals]))[0][0]
                                   })

        except Exception as err:
            _s_fn_id = "Class <RateOfReturns> Function <get_coin_cov_metric>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return _l_cov_dict

    # ''' Function [TBD] Phase II
    #         name: get_geometric_return
    #         description: applied only when the investments are layed over fixed period intervals.
    #         parameters:
    #                 @name (str)
    #                 @clean (dict)
    #         procedure: 
    #         return DataFrame
    # '''
    # # @staticmethod
    # def get_geometric_return(self, data_df : pd.DataFrame, value_col_name = "Value"):
        
    #     # import traceback
    #     # import pandas as pd
    #     # import numpy as np

    #     try:
    #         pass

    #     except Exception as err:
    #         _s_fn_id = "Class <RateOfReturns> Function <get_geometric_return>"
    #         print("[Error]"+_s_fn_id, err)
    #         print(traceback.format_exc())

    #     return _log_return_df