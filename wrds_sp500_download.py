import pandas as pd
import wrds 

# Fetch the list of S&P 500 tickers
def fetch_sp500_tickers():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    table = pd.read_html(url, header=0)[0] # Fetch table from Wikipedia page
    tickers = table.Symbol.tolist() # Convert table's Symbol column to list
    return tickers # Return list of tickers

# Fetch company data from WRDS for a given ticker
def fetch_data_wrds(wrds_username, ticker):
    db = wrds.Connection(wrds_username) # Connect to WRDS
        
    # Prepare the ticker list for the SQL query
    ticker_list = "','".join(tickers)

    # Define SQL query for fetching the data
    query = f"""
            select gvkey, tic, conm, datadate,  
            lt, at, lct, act, seq, dltt,dlc, ni, sale, cogs, oibdp, oiadp, invt, ppent, wcap, csho, prcc_f, re, rect, ap
            from comp.funda
            where indfmt='INDL' 
            and datafmt='STD'
            and popsrc='D'
            and consol='C'
            and datadate >= '01/01/2010' and tic in ('{ticker_list}')
            """
    comp = pd.DataFrame(db.raw_sql(query)) # Execute SQL query and convert the result to a DataFrame
    db.close() # Close the connection to WRDS

    comp['datadate'] = pd.to_datetime(comp['datadate']) # Convert datadate to datetime
    comp['year'] = comp['datadate'].dt.year # Extract year from datadate
    return comp # Return the DataFrame

#########################################################################

wrds_username = 'raunaq'
tickers = fetch_sp500_tickers()
comp = fetch_data_wrds(wrds_username, tickers)
comp.to_csv('sp500_data.csv') 