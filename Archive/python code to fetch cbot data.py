import yfinance as yf
import pandas as pd
from datetime import datetime

# Define the start and end dates
start_date = '2003-01-01'
end_date = '2024-07-21'
print(f"Start Date: {start_date}")
print(f"End Date: {end_date}")

# Commodity information
commodities = [
    {"Commodity": "Oil(Brent)", "Type": "Energy", "Ticker": "BZ=F", "Unit": "USD per Barrel"},
    {"Commodity": "Oil(WTI)", "Type": "Energy", "Ticker": "CL=F", "Unit": "USD per Barrel"},
    {"Commodity": "Natural Gas", "Type": "Energy", "Ticker": "NG=F", "Unit": "USD per MMBtu"},
    {"Commodity": "Gold", "Type": "Metal", "Ticker": "GC=F", "Unit": "USD per Troy Ounce"},
    {"Commodity": "Silver", "Type": "Metal", "Ticker": "SI=F", "Unit": "USD per Troy Ounce"},
    {"Commodity": "Cotton", "Type": "Agriculture", "Ticker": "CT=F", "Unit": "USc per lb."},
    {"Commodity": "Rice", "Type": "Agriculture", "Ticker": "ZR=F", "Unit": "USc per bushel"},
    {"Commodity": "Wheat", "Type": "Agriculture", "Ticker": "ZW=F", "Unit": "USc per bushel"},
    {"Commodity": "Soybean", "Type": "Agriculture", "Ticker": "ZS=F", "Unit": "USc per bushel"},
    {"Commodity": "Soybean Oil", "Type": "Agriculture", "Ticker": "ZL=F", "Unit": "USc per lb."},
    {"Commodity": "Corn", "Type": "Agriculture", "Ticker": "ZC=F", "Unit": "USc per lb."}
]

# Create an empty DataFrame to store the results
data = pd.DataFrame()

# Fetch data for each commodity
for commodity in commodities:
    ticker = commodity["Ticker"]
    df = yf.download(ticker, start=start_date, end=end_date)
    if not df.empty:
        df['Commodity'] = commodity["Commodity"]
        df['Unit'] = commodity["Unit"]
        df = df[['Commodity', 'Unit', 'Open', 'Close', 'High', 'Low']]
        data = pd.concat([data, df])

# Save to XLSX
try:
    data.to_excel('commodity_data.xlsx', index_label='Date')
    # Print success message
    print("Data successfully fetched and saved to 'commodity_data.xlsx'")
except Exception as e:
    print(f"An error occurred: {e}")

# Display the data
print(data)
