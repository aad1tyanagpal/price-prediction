    import yfinance as yf
    from datetime import datetime, timedelta
    import pandas as pd

    # Define the tickers for the currency pairs
    tickers = ['USDINR=X', 'MYRINR=X', 'EURINR=X']

    # Get yesterday's date
    yesterday = datetime.now() - timedelta(1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    # Initialize an empty DataFrame to store the results
    result_df = pd.DataFrame()

    # Fetch the historical data for each ticker
    for ticker in tickers:
        try:
            data = yf.download(ticker, start=yesterday_str, end=yesterday_str)
            if not data.empty:
                temp_df = data[['Open', 'Close', 'High', 'Low']].copy()
                temp_df['Date'] = yesterday_str
                temp_df['Currency'] = ticker.replace('=X', '')

                # Calculate the Average column
                temp_df['Average'] = temp_df[['Open', 'Close', 'High', 'Low']].mean(axis=1)

                result_df = pd.concat([result_df, temp_df])
            else:
                print(f"No data available for {ticker}")
        except Exception as e:
            print(f"Failed to download data for {ticker}: {e}")

    # Reorder and rename columns
    if not result_df.empty:
        result_df = result_df[['Date', 'Open', 'Close', 'High', 'Low', 'Average', 'Currency']]
        result_df.columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Average', 'Currency']

        # Save to CSV
        result_df.to_csv('exchange_rates.csv', index=False)
        print("Exchange rates saved to 'exchange_rates.csv'")
    else:
        print("No data available for the given date.")
