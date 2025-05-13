import pandas as pd
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import os
import psycopg2

# Database connection details
db_params = {
    'host': 'localhost',
    'port': 5433,
    'dbname': '2bt_database',
    'user': 'aad1tyanagpal',    # Replace with your actual username
    'password': 'aaditya99'  # Replace with your actual password
}

# Function to create a database connection using psycopg2
def create_connection():
    return psycopg2.connect(**db_params)

# Function to fetch the start date from a specified table and column
def fetch_start_date(table_name, column_name):
    print(f"Fetching start date from {table_name}.{column_name}...")
    query = f"SELECT MIN({column_name}) FROM {table_name} WHERE date_processed = 0"
    with create_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        start_date_str = cursor.fetchone()[0]
    if start_date_str is None:
        print(f"No dates to process in {table_name}.{column_name}.")
        exit()
    print(f"Start date fetched: {start_date_str}")
    return datetime.strptime(str(start_date_str), '%Y-%m-%d')

# Function to calculate the end date as today minus specified days
def calculate_end_date(days_ago=2):
    print(f"Calculating end date as today minus {days_ago} days...")
    end_date = datetime.today() - timedelta(days=days_ago)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"End date calculated: {end_date.strftime('%Y-%m-%d')}")
    return end_date

# Function to generate date ranges
def generate_date_ranges(start_date, end_date, period_days):
    print(f"Generating date ranges from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} with a period of {period_days} days...")
    date_ranges = []
    current_start_date = start_date
    while current_start_date <= end_date:
        current_end_date = current_start_date + timedelta(days=period_days - 1)
        if current_end_date > end_date:
            current_end_date = end_date
        date_ranges.append((current_start_date.strftime('%d-%m-%Y'), current_end_date.strftime('%d-%m-%Y')))
        current_start_date = current_end_date + timedelta(days=1)
    print(f"Generated {len(date_ranges)} date ranges.")
    return date_ranges

# Function to construct URL
def construct_url(commodity_code, state_alpha_code, start_date, end_date, commodity_head, state):
    URL1 = "https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity="
    URL3 = "&Tx_State="
    URL5 = "&Tx_District=0&Tx_Market=0&DateFrom="
    URL7 = "&DateTo="
    URL9 = "&Fr_Date="
    URL11 = "&To_Date="
    URL13 = "&Tx_Trend=2&Tx_CommodityHead="
    URL15 = "&Tx_StateHead="
    URL17 = "&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"

    # Convert dates to the required format
    start_date_formatted = datetime.strptime(start_date, '%d-%m-%Y').strftime('%d-%b-%Y')
    end_date_formatted = datetime.strptime(end_date, '%d-%m-%Y').strftime('%d-%b-%Y')

    url = (
        URL1 + commodity_code +
        URL3 + state_alpha_code +
        URL5 + start_date_formatted +
        URL7 + end_date_formatted +
        URL9 + start_date_formatted +
        URL11 + end_date_formatted +
        URL13 + commodity_head +
        URL15 + state +
        URL17
    )
    return url

# Function to fetch and parse data from URL
def fetch_data_from_url(url, commodity_name, commodity_code):
    print(f"Fetching data from URL: {url}")
    try:
        response = requests.get(url, timeout=10)  # Adding a timeout of 10 seconds
        response.raise_for_status()  # Check for request errors
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from URL: {url}\nException: {e}")
        return None

    # Parse HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table
    table = soup.find('table')
    if not table:
        print(f"No table found in URL: {url}")
        return None

    # Extract headers
    headers = [header.text.strip().replace(".", "") for header in table.find_all('th')]
    if not headers:
        headers = [header.text.strip().replace(".", "") for header in table.find_all('td')[:len(table.find_all('tr')[1].find_all('td'))]]

    # Extract rows, handling merged cells
    rows = []
    rowspan_cells = {}

    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        current_row = []
        col_index = 0

        while col_index < len(headers):
            if col_index in rowspan_cells and rowspan_cells[col_index]['span'] > 0:
                current_row.append(rowspan_cells[col_index]['value'])
                rowspan_cells[col_index]['span'] -= 1
            else:
                if cells:
                    cell = cells.pop(0)
                    cell_value = cell.text.strip()
                    current_row.append(cell_value)

                    if cell.has_attr('rowspan'):
                        rowspan_cells[col_index] = {
                            'value': cell_value,
                            'span': int(cell['rowspan']) - 1
                        }
                else:
                    current_row.append('')  # Handle missing cells

            col_index += 1

        # Add commodity name and code to the row
        current_row.append(commodity_name)
        current_row.append(commodity_code)

        rows.append(current_row)

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=headers + ['Commodity Name', 'Commodity Code'])
    return df

commodities = [
    {"Commodity": "Chanaa", "CommodityHead": "Bengal+Gram(Gram)(Whole)", "CommodityCode": "6"},
    {"Commodity": "Wheat", "CommodityHead": "Wheat", "CommodityCode": "1"},
    {"Commodity": "Taramira", "CommodityHead": "Taramira", "CommodityCode": "76"},
    {"Commodity": "Moong", "CommodityHead": "Green+Gram+(Moong)(Whole)", "CommodityCode": "9"},
    {"Commodity": "Mustard", "CommodityHead": "Mustard", "CommodityCode": "12"},
    {"Commodity": "Mustard Oil", "CommodityHead": "Mustard+Oil", "CommodityCode": "324"},
    {"Commodity": "Cotton", "CommodityHead": "Cotton", "CommodityCode": "15"},
    {"Commodity": "Guar", "CommodityHead": "Guar", "CommodityCode": "75"},
    {"Commodity": "Guar", "CommodityHead": "Guar+Seed(Cluster+Beans+Seed)", "CommodityCode": "413"},
    {"Commodity": "Jau", "CommodityHead": "Barley+(Jau)", "CommodityCode": "29"},
    {"Commodity": "Rice(Dhan)", "CommodityHead": "Paddy(Dhan)(Common)", "CommodityCode": "2"},
    {"Commodity": "Bajra", "CommodityHead": "Bajra(Pearl+Millet%2fCumbu)", "CommodityCode": "288"},
    {"Commodity": "Jwaar", "CommodityHead": "Jowar(Sorghum)", "CommodityCode": "5"},
    {"Commodity": "Corn", "CommodityHead": "Maize", "CommodityCode": "4"},
    {"Commodity": "Rice(Basmati)", "CommodityHead": "Paddy(Dhan)(Basmati)", "CommodityCode": "414"},
    {"Commodity": "Groundnut", "CommodityHead": "Groundnut", "CommodityCode": "10"},
    {"Commodity": "Till", "CommodityHead": "Sesamum(Sesame,Gingelly,Til)", "CommodityCode": "11"},
    {"Commodity": "Soyabean", "CommodityHead": "Soyabean", "CommodityCode": "13"},
    {"Commodity": "Dollar Chana", "CommodityHead": "Kabuli+Chana(Chickpeas-White)", "CommodityCode": "362"},
    {"Commodity": "Moath Dal", "CommodityHead": "Moath+Dal", "CommodityCode": "258"},
    {"Commodity": "Moath Dal", "CommodityHead": "Moath+Dal", "CommodityCode": "95"}
]

states = [
    {"StateName": "Andaman+and+Nicobar+Islands", "StateAlphaCode": "AN"},
    {"StateName": "Andhra+Pradesh", "StateAlphaCode": "AP"},
    {"StateName": "Arunachal+Pradesh", "StateAlphaCode": "AR"},
    {"StateName": "Assam", "StateAlphaCode": "AS"},
    {"StateName": "Bihar", "StateAlphaCode": "BR"},
    {"StateName": "Chandigarh", "StateAlphaCode": "CH"},
    {"StateName": "Chhattisgarh", "StateAlphaCode": "CG"},
    {"StateName": "Dadra+and+Nagar+Haveli", "StateAlphaCode": "DN"},
    {"StateName": "Daman+and+Diu", "StateAlphaCode": "DN"},
    {"StateName": "NCT+of+Delhi", "StateAlphaCode": "DL"},
    {"StateName": "Goa", "StateAlphaCode": "GO"},
    {"StateName": "Gujarat", "StateAlphaCode": "GJ"},
    {"StateName": "Haryana", "StateAlphaCode": "HR"},
    {"StateName": "Himachal+Pradesh", "StateAlphaCode": "HP"},
    {"StateName": "Jammu+and+Kashmir", "StateAlphaCode": "JK"},
    {"StateName": "Jharkhand", "StateAlphaCode": "JR"},
    {"StateName": "Karnataka", "StateAlphaCode": "KK"},
    {"StateName": "Kerala", "StateAlphaCode": "KL"},
    {"StateName": "Lakshadweep", "StateAlphaCode": "LD"},
    {"StateName": "Madhya+Pradesh", "StateAlphaCode": "MP"},
    {"StateName": "Maharashtra", "StateAlphaCode": "MH"},
    {"StateName": "Manipur", "StateAlphaCode": "MN"},
    {"StateName": "Meghalaya", "StateAlphaCode": "MG"},
    {"StateName": "Mizoram", "StateAlphaCode": "MZ"},
    {"StateName": "Nagaland", "StateAlphaCode": "NG"},
    {"StateName": "Odisha", "StateAlphaCode": "OR"},
    {"StateName": "Puducherry", "StateAlphaCode": "PC"},
    {"StateName": "Punjab", "StateAlphaCode": "PB"},
    {"StateName": "Rajasthan", "StateAlphaCode": "RJ"},
    {"StateName": "Sikkim", "StateAlphaCode": "SK"},
    {"StateName": "Tamil+Nadu", "StateAlphaCode": "TN"},
    {"StateName": "Telangana", "StateAlphaCode": "TS"},
    {"StateName": "Tripura", "StateAlphaCode": "TR"},
    {"StateName": "Uttar+Pradesh", "StateAlphaCode": "UP"},
    {"StateName": "Uttarakhand", "StateAlphaCode": "UC"},
    {"StateName": "West+Bengal", "StateAlphaCode": "WB"}
]

# ========== Main Script Execution Starts Here ==========

# Section 1: Fetch Start Date and End Date
print("\n========== Section 1: Fetching Start and End Dates ==========")
start_time = time.time()
start_date = fetch_start_date('mandi_data.mandi_date_log', 'date')
end_date = calculate_end_date(days_ago=2)
end_time = time.time()
print(f"Section 1 Completed in {end_time - start_time:.2f} seconds.")

# Section 2: Generate Date Ranges
print("\n========== Section 2: Generating Date Ranges ==========")
start_time = time.time()
date_ranges = generate_date_ranges(start_date, end_date, 100)
end_time = time.time()
print(f"Section 2 Completed in {end_time - start_time:.2f} seconds.")

# Section 3: Construct URLs
print("\n========== Section 3: Constructing URLs ==========")
start_time = time.time()
urls = []
for state_info in states:
    for commodity in commodities:
        for date_range in date_ranges:
            url = construct_url(
                commodity["CommodityCode"],
                state_info["StateAlphaCode"],
                date_range[0],
                date_range[1],
                commodity["CommodityHead"],
                state_info["StateName"].replace(' ', '+')
            )
            urls.append((url, commodity["Commodity"], commodity["CommodityCode"]))
end_time = time.time()
print(f"Constructed {len(urls)} URLs.")
print(f"Section 3 Completed in {end_time - start_time:.2f} seconds.")

# Section 4: Fetch and Process Data from URLs
print("\n========== Section 4: Fetching and Processing Data ==========")
start_time = time.time()
all_data = pd.DataFrame()
failed_urls = []  # To track failed URLs

for url, commodity_name, commodity_code in tqdm(urls, desc="Processing URLs"):
    df = fetch_data_from_url(url, commodity_name, commodity_code)
    if df is not None:
        all_data = pd.concat([all_data, df], ignore_index=True)
    else:
        failed_urls.append(url)
    time.sleep(1)  # Adding a delay to prevent overwhelming the server

end_time = time.time()
print(f"Section 4 Completed in {end_time - start_time:.2f} seconds.")
if failed_urls:
    print(f"Failed to fetch data from {len(failed_urls)} URLs.")

# Section 5: Data Cleaning
print("\n========== Section 5: Data Cleaning ==========")
start_time = time.time()
# Drop rows where 'State Name' column has value "-"
if 'State Name' in all_data.columns:
    all_data = all_data[all_data['State Name'] != "-"]

# Print the actual column names to check
print("Original columns:", all_data.columns.tolist())

# Rename the columns, including 'Group' to 'family'
all_data.rename(columns={
    'State Name': 'state_name',
    'District Name': 'district_name',
    'Market Name': 'market_name',
    'Variety': 'variety',
    'Group': 'family',  # Renaming 'Group' to 'family'
    'Arrivals (Tonnes)': 'arrivals_tonnes',
    'Min Price (Rs/Quintal)': 'min_price_rs_quintal',
    'Max Price (Rs/Quintal)': 'max_price_rs_quintal',
    'Modal Price (Rs/Quintal)': 'modal_price_rs_quintal',
    'Reported Date': 'reported_date',
    'Commodity Name': 'jins_name',
    'Commodity Code': 'jins_code'
}, inplace=True)

# If there are any remaining columns with spaces or special characters, clean them
def clean_column_names(columns):
    new_columns = []
    for col in columns:
        col_clean = col.strip().lower()
        col_clean = col_clean.replace(' ', '_')
        col_clean = col_clean.replace('(', '')
        col_clean = col_clean.replace(')', '')
        col_clean = col_clean.replace('.', '')
        col_clean = col_clean.replace('/', '_')
        col_clean = col_clean.replace('-', '_')
        col_clean = col_clean.replace(',', '')
        new_columns.append(col_clean)
    return new_columns

all_data.columns = clean_column_names(all_data.columns)

# Print the cleaned column names
print("Cleaned columns:", all_data.columns.tolist())

# Remove commas in specified columns
for col in ['arrivals_tonnes', 'min_price_rs_quintal', 'max_price_rs_quintal', 'modal_price_rs_quintal', 'reported_date']:
    if col in all_data.columns:
        all_data[col] = all_data[col].str.replace(',', '', regex=False)

end_time = time.time()
print(f"Section 5 Completed in {end_time - start_time:.2f} seconds.")

# Section 5.1: Save the pandas dataframe in a csv file
print("\n========== Section 5.1: Saving DataFrame to CSV File ==========")
start_time = time.time()

current_datetime = datetime.now()
filename = f"data_for_interim_raw_table_{current_datetime.strftime('%Y%m%d')}_{current_datetime.strftime('%H%M%S')}.csv"
filepath = r"D:\\2BT\\Python and codes\\Results\\agmarknet_data_from_code"  # Replace with your desired path

full_path = os.path.join(filepath, filename)

# Ensure the directory exists
os.makedirs(filepath, exist_ok=True)

# Save the DataFrame to CSV
all_data.to_csv(full_path, index=False, encoding='utf-8')
print(f"DataFrame saved to {full_path}")

end_time = time.time()
print(f"DataFrame saved in {end_time - start_time:.2f} seconds.")

# Section 6: Database Operations
print("\n========== Section 6: Database Operations ==========")
start_time = time.time()

# Section 6: Database Operations
print("\n========== Section 6: Database Operations ==========")
start_time = time.time()

# Truncate the table mandi_data.interim_raw
print("Truncating table mandi_data.interim_raw...")
with create_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE mandi_data.interim_raw")
    conn.commit()
print("mandi_data.interim_raw Table truncated.")

# Truncate the table mandi_data.interim_mdb
print("Truncating table mandi_data.interim_mdb...") 
with create_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE mandi_data.interim_mdb")
    conn.commit()
print("mandi_data.interim_mdb Table truncated.")

# Load the data from CSV file into mandi_data.interim_raw
print("Loading data from CSV file into database...")

with create_connection() as conn:
    cursor = conn.cursor()
    with open(full_path, 'r', encoding='utf-8') as f:
        columns = all_data.columns.tolist()
        quoted_columns = ', '.join(f'"{col}"' for col in columns)
        copy_sql = f"COPY mandi_data.interim_raw ({quoted_columns}) FROM STDIN WITH CSV HEADER DELIMITER ',' NULL ''"
        cursor.copy_expert(copy_sql, f)
    conn.commit()
print("Data loaded into database.")

# Update mandi_data.mandi_date_log, set date_processed to 1 for dates between start_date and end_date
print("Updating mandi_date_log...")
with create_connection() as conn:
    cursor = conn.cursor()
    update_query = """
        UPDATE mandi_data.mandi_date_log
        SET date_processed = 1
        WHERE date BETWEEN %s AND %s
    """
    params = (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    cursor.execute(update_query, params)
    conn.commit()
print("mandi_date_log updated.")

end_time = time.time()
print(f"Section 6 Completed in {end_time - start_time:.2f} seconds.")

# Section 7: Data Processing and Insertion into interim_mdb
print("\n========== Section 7: Data Processing and Insertion into interim_mdb ==========")
start_time = time.time()

# Compute start_date - 1 and start_date - 100
start_date_minus_1 = (start_date - timedelta(days=1)).strftime('%Y-%m-%d')
start_date_minus_100 = (start_date - timedelta(days=100)).strftime('%Y-%m-%d')

# Define the SQL query with actual date values
sql_query = f"""
WITH combined_raw_data AS (
    SELECT
        CAST(reported_date AS DATE) AS reported_date,
        state_name,
        district_name,
        market_name,
        variety,
        family,
        jins_name,
        jins_code,
        arrivals_tonnes,
        min_price_rs_quintal,
        max_price_rs_quintal,
        modal_price_rs_quintal
    FROM mandi_data.interim_raw
    UNION ALL
    SELECT
        reported_date,
        state_name,
        district_name,
        market_name,
        variety,
        family,
        jins_name,
        jins_code,
        arrivals_tonnes,
        min_price_rs_quintal,
        max_price_rs_quintal,
        modal_price_rs_quintal
    FROM mandi_data.mandi_mdb
    WHERE reported_date BETWEEN '{start_date_minus_100}' AND '{start_date_minus_1}'
),
price_data AS (
    SELECT 
        crd.reported_date, 
        crd.state_name, 
        crd.district_name, 
        crd.market_name, 
        crd.variety, 
        crd.family, 
        crd.jins_name, 
        crd.jins_code, 
        mg.geo_symbol AS geo_symbol, 
        mjs.jins_symbol, 
        mg.geo_symbol || mjs.jins_symbol AS symbol,
        TO_CHAR(crd.reported_date, 'YYYYMMDD') || mg.geo_symbol || mjs.jins_symbol AS uid,
        crd.arrivals_tonnes,  
        crd.min_price_rs_quintal,
        crd.max_price_rs_quintal,
        crd.modal_price_rs_quintal
    FROM 
        combined_raw_data crd
    JOIN 
        mandi_data.mandi_geosymbol mg 
    ON 
        crd.state_name = mg.state_name 
        AND crd.district_name = mg.district_name 
        AND crd.market_name = mg.market_name
    JOIN 
        mandi_data.mandi_jinssymbol mjs 
    ON 
        crd.jins_name = mjs.jins_name
),
final_raw_price_data AS (
    SELECT 
        pd.*
    FROM price_data pd
)
-- Insert into interim_mdb table
INSERT INTO mandi_data.interim_mdb (
    reported_date, 
    state_name, 
    district_name, 
    market_name, 
    variety, 
    family, 
    jins_name, 
    jins_code, 
    geo_symbol, 
    jins_symbol, 
    symbol, 
    uid, 
    arrivals_tonnes,  
    min_raw,
    max_raw,
    modal_raw,
    min_price_rs_quintal, 
    max_price_rs_quintal, 
    modal_price_rs_quintal
)
SELECT DISTINCT
    frpd.reported_date, 
    frpd.state_name, 
    frpd.district_name, 
    frpd.market_name, 
    frpd.variety, 
    frpd.family, 
    frpd.jins_name, 
    frpd.jins_code,
    frpd.geo_symbol, 
    frpd.jins_symbol, 
    frpd.symbol, 
    frpd.uid,
    frpd.arrivals_tonnes,
    frpd.min_price_rs_quintal AS min_raw,
    frpd.max_price_rs_quintal AS max_raw,
    frpd.modal_price_rs_quintal AS modal_raw,
    frpd.min_price_rs_quintal,
    frpd.max_price_rs_quintal,
    frpd.modal_price_rs_quintal
FROM final_raw_price_data frpd;
"""

# Execute the SQL query using psycopg2
print("Processing data and inserting into interim_mdb...")
with create_connection() as conn:
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        conn.commit()
        print("Data inserted into mandi_data.interim_mdb.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

end_time = time.time()
print(f"Section 7 Completed in {end_time - start_time:.2f} seconds.")

# Section 8: Appending Data to Master Tables
print("\n========== Section 8: Appending Data to Master Tables ==========")
start_time = time.time()

# Define the date range for filtering
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Append data from interim_raw to mandi_raw_master for the specified date range
print("Appending data from interim_raw to mandi_raw_master...")
with create_connection() as conn:
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            INSERT INTO mandi_data.mandi_raw_master
            SELECT *
            FROM mandi_data.interim_raw
            WHERE reported_date BETWEEN '{start_date_str}' AND '{end_date_str}'
        """)
        conn.commit()
        print("Data appended to mandi_raw_master.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

# Append data from interim_mdb to mandi_mdb for the specified date range
print("Appending data from interim_mdb to mandi_mdb...")
with create_connection() as conn:
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            INSERT INTO mandi_data.mandi_mdb
            SELECT *
            FROM mandi_data.interim_mdb
            WHERE reported_date BETWEEN '{start_date_str}' AND '{end_date_str}'
        """)
        conn.commit()
        print("Data appended to mandi_mdb.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

end_time = time.time()
print(f"Section 8 Completed in {end_time - start_time:.2f} seconds.")

print("\n========== All Sections Completed Successfully ==========")
