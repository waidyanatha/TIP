# StarXETP

## Introduction
The StarX aims to develop an ETP for the crypto currency asset class; namely, the StarXETP project. The code and data in this repository are relevant to the project; especially, for realizing the feasibility of developing the ETP as service.

## Insallation
1. Simply clone starxetp on to your local machine or a server ```git clone https://github.com/waidyanatha/starxetp.git```.
1. Run ```python3 -m pip install -r requirements.txt``` to ensure all dependencies are installed in your environment
1. Read the 0.Introduction.ipyn notebook for an quick understanding about the project and the features
1. Execute the notebooks to see the results and analyze the data

## Software Components
The poject has sevaral folcers containing the resources:
1. ___Data___ - comprising market capitolization (i.e., market-cap) data and evaluation data for selected crypto assets
   * _market_cap_2021-01-01_2022-06-01_ folder contains data for the selected tickers with each file containing two months of data
   * _*.csv_ files are redundant evaluation data from various outputs that will be removed in a later stage of the project
1. ___img___ - contains images for display in the notebooks and other relevant components
1. ___lib___ - libaries containing classes and methods
   * _clsDataETL_ - performs data load from local storage and transformations that enrich the market-cap data with mean, varance, moving average, moment, and selecting top N significant market cap assets.
   * _clsETPreturns_ - presents basic weighted sum, logarithmic, and simple returns
   * _clsIndex_ - offers functions for calculating sharpe, soratino, and adx performance measures
   * _clsS3MCapDataMart_ \[work in progress\] is for managing an AWS S3 bucet with necessary historic and latest market cap data
1. ___Notebooks___ - offers visual analytics of rate of returns, index performance, and trends of data under different transformations
   * _0.Introduction_ - offers an introduction of the project objectives and how to use the notebooks for performing the visual analytics
   * _1.etpDataDiscovery_ - redumentary mean and varian trend, risk, and simple returns
   * _2.etpLogReturns_ - considers the Logarithmic rate of returns as the basis to present analytical outputs
   * _3.etpPerformIndex_ - presents the adx, sharpe, and sortino performance indeces for the market-cap data
   * _4.etpBalancingPortfolio_ - \[work in progress\] will illustrate how the algorithm for selecting the top N assets and dynamially rebalancing the ETP portfolio to maintain a higher expected return
   * _coingekoDataExctract_ - executes the data extraction scripts for getting publicly available market-cap data

## Contanct
@waidyanatha for any queries regarding this code.