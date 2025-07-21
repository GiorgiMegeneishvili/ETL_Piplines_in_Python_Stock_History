from utils.logger import get_logger
from utils.config import create_engine
import yfinance as yf
import pandas as pd
from sqlalchemy import text

logger = get_logger(__name__)

def get_company_data(stock1):
    try:
        stock = yf.Ticker(stock1)
        hist = stock.history(start="2024-01-01", end="2024-12-31")

        # Reset index to move the Date from index to a column
        hist.reset_index(inplace=True)
        return hist
    except Exception as e:
        logger.error(f"Error : {e}")


def transfrom_data(hist, Company_ID):
    try:
        # Add an ID column starting from 1
        hist['ID'] = range(1, len(hist) + 1)

        # Add a fixed Company_ID (e.g., 1 for Apple)
        hist['Company_ID'] = Company_ID

        # Reorder columns to make Company_ID the second column
        columns = ['ID', 'Company_ID'] + [col for col in hist.columns if
                                          col not in ['ID', 'Company_ID', 'timestamp', 'Timestamp']]
        hist = hist[columns]

        # Convert to DataFrame (optional, already a DataFrame)
        df = pd.DataFrame(hist)

        # Ensure no timestamp columns remain
        df = df.loc[:, ~df.columns.str.lower().isin(['timestamp'])]

        df['Date'] = pd.to_datetime(df['Date']).dt.date

        return df
    except Exception as e:
        logger.error(f"Error : {e}")

def load_company_data_to_sql_server(df, table_name):
    engine = create_engine
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Table '{table_name}' loaded successfully.")
    except Exception as e:
        logger.error(f"Error : {e}")

def load_data_to_sql_server(df):
    engine = create_engine
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS stock_history;"))  # Drop old table that may have timestamp
    try:
        # Drop any timezone info that might cause dtype issues
        if df['Date'].dtype.name == 'datetime64[ns, US/Eastern]':
            df['Date'] = df['Date'].dt.tz_localize(None)

        df.to_sql('stock_history', engine, if_exists='append', index=False)
        logger.info("Data has been loaded successfully.")
    except Exception as e:
        logger.error(f"Error : {e}")



if __name__ == '__main__':


    # Your list of tickers
    tickers = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NFLX", "NVDA", "ADBE", "INTC",
        "CSCO", "ORCL", "CRM", "IBM", "QCOM", "AMD", "PYPL", "TXN", "AVGO", "SAP",
        "SHOP", "UBER", "ZM", "TWLO", "SNOW", "PLTR",  "ROKU", "SPOT", "BABA",
        "JD", "PDD", "TSM", "ASML", "MU", "DELL", "HPQ", "F", "GM", "BA"
    ]

    # Company names corresponding to each ticker
    company_names = [
        "Apple Inc.", "Microsoft Corp.", "Alphabet Inc.", "Amazon.com Inc.", "Meta Platforms Inc.",
        "Tesla Inc.", "Netflix Inc.", "NVIDIA Corp.", "Adobe Inc.", "Intel Corp.",
        "Cisco Systems Inc.", "Oracle Corp.", "Salesforce Inc.", "IBM Corp.", "Qualcomm Inc.",
        "Advanced Micro Devices Inc.", "PayPal Holdings Inc.", "Texas Instruments Inc.", "Broadcom Inc.", "SAP SE",
        "Shopify Inc.", "Uber Technologies Inc.", "Zoom Video Communications Inc.", "Twilio Inc.", "Snowflake Inc.",
        "Palantir Technologies Inc.",  "Roku Inc.", "Spotify Technology SA", "Alibaba Group Holding Ltd.",
        "JD.com Inc.", "PDD Holdings Inc.", "Taiwan Semiconductor Manufacturing Co.", "ASML Holding NV",
        "Micron Technology Inc.",
        "Dell Technologies Inc.", "HP Inc.", "Ford Motor Co.", "General Motors Co.", "Boeing Co."
    ]

    # Create DataFrame with ID, Ticker, and Company Name
    df = pd.DataFrame({
        'Company_ID': range(1, len(tickers) + 1),
        'Ticker': tickers,
        'Company': company_names
    })
    load_company_data_to_sql_server(df,'Company')
    # print(df)

    for _, row in df.iterrows():
        Company_ID, ticker = row['Company_ID'], row['Ticker']
        logger.info(f"Processing {Company_ID} - {ticker}")
        try:
            hist = get_company_data(ticker)
            df = transfrom_data(hist, Company_ID)
            load_data_to_sql_server(df)
        except Exception as e:
            logger.error(f"Failed to process {ticker}: {e}")
