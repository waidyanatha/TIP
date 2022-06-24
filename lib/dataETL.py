#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

'''
    CLASS with essential data extract, load, and transform processes:
        1) read and save batches of data from coindex API
        2) load data from files into a dataframe and transform into a cross tab table
        3) 
'''
class ExtractLoadTransform():

    ''' Function
            name: __init__
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def __init__(self, dataPath : str="../data/market_cap/"):
        
        self.path = dataPath     # path = "../data/market_cap_2021-01-01_2022-06-01/"
 
        return None

    ''' Function
            name: get_file_list
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def get_file_list(self, path = None):

        # import OS module
        import os

        # Get the list of all files and directories        
        if path:
            self.path = path
#        dir_list = os.listdir(self.path)
        return os.listdir(self.path)

    ''' Function
            name: load_data
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def load_data(self,fileList):

        import pandas as pd

#        dir_list = self.get_file_list(path)

        columns = ["Date","ID","Symbol","market_cap"]
        data_df = pd.DataFrame([],columns=columns)
        for _s_file in fileList:
            if _s_file.endswith(".csv"):
                _s_rel_path = self.path+_s_file
                _tmp_df = pd.read_csv(_s_rel_path, index_col=False)
                data_df = pd.concat([data_df,_tmp_df[columns]])
        data_df.reset_index(drop=True)
        data_df['Date'] = data_df['Date'].astype('datetime64[ns]')
        data_df['market_cap'] = data_df['market_cap'].astype('float64')

        return data_df

    ''' Function
            name: transfrom_data
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def transfrom_data(self,data_df):

        import pandas as pd

        ''' Initialize variables '''
        _l_coin_ids = sorted(data_df['ID'].unique())
        _l_dates = sorted(data_df['Date'].unique())
        _l_columns = list(sorted(data_df['ID'].unique()))
        _l_columns.insert(0,"Date")

        market_df = pd.DataFrame([], columns=_l_columns)
        market_df["Date"] = _l_dates

        for _s_coin_id in _l_coin_ids:
            tmp_df = pd.DataFrame([])
            tmp_df = data_df.loc[data_df["ID"] == _s_coin_id]
            tmp_df = tmp_df.sort_values(by=['Date'])
            tmp_df = tmp_df.dropna(inplace=False)

            for _date in tmp_df["Date"]:
                _value = tmp_df.loc[tmp_df['Date'] == _date, "market_cap"].item()
                market_df.loc[market_df['Date']==_date, _s_coin_id] = _value

        ''' Cleanup rows and columns '''
        market_df=market_df.drop_duplicates()
        market_df = market_df.dropna(axis=1)

        ''' Set the dtypes '''
        market_df['Date'] = market_df['Date'].astype('datetime64[ns]')
        market_df.loc[:,market_df.columns !='Date'] = market_df.loc[:,market_df.columns !='Date'].astype('float64')

        return market_df