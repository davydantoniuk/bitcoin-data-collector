import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('news_evaluation/summarize_news.db')
cursor = conn.cursor()

# Enable foreign key support
cursor.execute('PRAGMA foreign_keys = ON;')

# Create the link table with foreign keys
cursor.execute('''
CREATE TABLE IF NOT EXISTS news_prices (
    news_id INTEGER,
    price_id INTEGER,
    FOREIGN KEY(news_id) REFERENCES news(id),
    FOREIGN KEY(price_id) REFERENCES prices_3m(id)
)
''')

# Load news and prices data into DataFrames
news_df = pd.read_sql_query('SELECT id, time FROM news', conn)
prices_df = pd.read_sql_query('SELECT id, close_time FROM prices_3m', conn) # prices_3m - downloaded from Binance API


# Use merge_asof to find the nearest future close_time for each news item
merged_df = pd.merge_asof(
    news_df.sort_values('time'),
    prices_df.sort_values('close_time'),
    left_on='time',
    right_on='close_time',
    direction='forward'
)

# Prepare the link table DataFrame with foreign keys
link_table_df = merged_df[['id_x', 'id_y']]
link_table_df.columns = ['news_id', 'price_id']

# Insert the link data into the database
link_table_df.to_sql('news_prices', conn, if_exists='append', index=False)

# Commit changes and close the connection
conn.commit()
conn.close()