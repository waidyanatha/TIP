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
    CLASS with essential data extract, load, and transform processes:
        1) read and save batches of data from coindex API
        2) load data from files into a dataframe and transform into a cross tab table
        3) 
'''

class AssetMCapStore():

    ''' Function
            name: DataReader
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def __init__(self,
                dataSource : str = "coindesk",  # default = "coindesk" 
                # startDate = np.nan, # start time period of the extraction,
                # endDate = np.nan,   # ending time period of the extraction
                **s3,   # S3 bucket access, token, and location information
                ):
        
        self.data = pd.DataFrame()
        self.data_source = dataSource
        ''' if not specified set the end date to today '''
        if 'endDate' in s3.keys() and isinstance(s3['endtDate'],date):
            self.end_date = s3['endtDate']
        else:
            self.end_date = date.today()
        print(self.end_date)
        ''' if not specified set the start date to 3 months prior to end date '''
        if 'startDate' in s3.keys() and isinstance(s3['startDate'],date):
            self.start_date = s3['startDate']
        else:
            self.start_date = self.end_date - timedelta(days=3)
        print(self.start_date)
        ''' set the s3 bucket name '''
        if 'bucketName' in s3.keys():
            self.s3_bucket = s3['bucketName']
        else:
            self.s3_bucket = "crypto_assets"
        ''' set the s3 object group/folder name '''
        if 'objFolder' in s3.keys():
            self.s3_obj_folder = s3['objFolder']
        else:
            self.s3_obj_folder = "./market_cap/"
        ''' add a prefix to the object file name '''
        if 'objPrefix' in s3.keys():
            self.s3_object_prefix = s3['objPrefix']
        else:
            self.s3_object_prefix = "market_cap"
        if 'accessKey' in s3.keys():
            self.s3_access_key = s3['accessKey']
        else:
            self.s3_access_key = np.nan
        if 'secretKey' in s3.keys():
            self.s3_secret_key = s3['secretKey']
        else:
            self.s3_secret_key = np.nan

        ''' Initialize coin list '''
        self.coins_dict = {
            "btc" : "bitcoin",
            "eth" : "ethereum",
            # "bch" : "bitcoin_cash",
            "ltc" : "litecoin",
            "xrp" : "ripple",
            "sol" : "solana",
            "ada" : "cardano",
            "bnb" : "binancecoin"
        }

        return None

    ''' Function
            name: _init_s3
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: Get coin id, name, & symbol
            return DataFrame
    '''
    def request_historic_data(self,
                    url : str,  # e.g. "https://api.coingecko.com/api/v3/coins/"+str(coin_id)+"/history"
                    from_date,  # starting date of the data extraction
                    to_date,    # ending date of the data extraction
                    ):

        from requests import Request, Session
        from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
        import json
        import pandas as pd
        import traceback

        market_cap_df = pd.DataFrame()

        try:
            # url = "https://api.coingecko.com/api/v3/coins/list"

            headers = {
                'accepts': 'application/json',
            }

            session = Session()
            session.headers.update(headers)

            parameters = {
                'include_platform':'false'
            }
            response = session.get(url, params=parameters)
            coins = json.loads(response.text)

            market_cap_df = pd.DataFrame(coins)

        except Exception as err:
            _s_fn_id = "Class <RateOfReturns> Function <sum_weighted_returns>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return market_cap_df

    ''' Function
            name: _init_s3
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def data_to_s3object(self,
                    _s3_bucket_name : str,  # s3 bucket to store the data
                    _s3_object : str,         # s3 object: file name to write data
                    _market_cap_data : pd.DataFrame # the data from the request API
                        ):

        try:
            if _market_cap_data.shape[0] <=0:
                raise ValueError("Invalid dataframe with %d rows" %(_market_cap_data.shape[0]))
            if _s3_bucket_name:
                self.s3_bucket = _s3_bucket_name
            if not _s3_object:
                raise ValueError("Invalide file name %s to store the data" %(_s_fname))


        except Exception as err:
            _s_fn_id = "Class <RateOfReturns> Function <sum_weighted_returns>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return True
    ''' Function
            name: _init_s3
            parameters:
                    @name (str)
                    @clean (dict)
            procedure: 
            return DataFrame
    '''
    def _init_s3():

        import boto3

        AWS_ACCESS_KEY_ID = 'AKIA2N5RYRM4VSAMCF76'
        AWS_SECRET_ACCESS_KEY = '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'
        NEW_BUCKET_NAME = 'waidy-thin-three'

        try:
            # Retrieve the list of existing buckets
            # s3 = boto3.client('s3')
            s3 = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                # aws_session_token=SESSION_TOKEN
            )
            response = s3.list_buckets()

            # Output the bucket names
            print('Existing buckets:')
            for bucket in response['Buckets']:
                print(f'  {bucket["Name"]}')

            s3 = boto3.resource('s3')
            # if s3.Bucket('Hello') in s3.buckets.all():
            #     print("Hello in buckets list")
            # else:
            #     print("Hello not in")
            # print(s3.buckets.all())

            # bucket_name = AWS_ACCESS_KEY_ID.lower() + '-dump'
            # conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
            #         AWS_SECRET_ACCESS_KEY)

            # bucket_name="waidy-thin-three"
            # bucket = conn.create_bucket(bucket_name,
            #     location=boto.s3.connection.Location.DEFAULT)
            # bucket = boto.connect_s3.get_bucket('your bucket name')
            # print(bucket)

            # testfile = "/home/gnewy/Documents/Visa20200421.pdf"   #replace this with an actual filename
            # print('Uploading %s to Amazon S3 bucket %s' % \
            #    (testfile, bucket_name))

            # def percent_cb(complete, total):
            #     sys.stdout.write('.')
            #     sys.stdout.flush()


            # k = Key(bucket)
            # k.key = 'my test file'
            # k.set_contents_from_filename(testfile,
            #     cb=percent_cb, num_cb=10)

        except Exception as err:
            _s_fn_id = "Class <RateOfReturns> Function <sum_weighted_returns>"
            print("[Error]"+_s_fn_id, err)
            print(traceback.format_exc())

        return None