import json
import re
import sqlite3
from time import sleep
import requests
from urllib.parse import urlparse
import os
from datetime import datetime
import html

def remove_html_tags(text):
    text = re.sub(r'<script.*?>.*?</script>', '', text, flags=re.DOTALL)
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    return html.unescape(text)

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir, os.pardir))
db_path = os.path.join(parent_dir, "data.db")
data_path = os.path.join(script_dir, "coindesk_com_data/tg_coindesk.json")

# Load URLs from post_urls.json
with open(data_path, 'r') as file:
    urls = json.load(file)

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        content TEXT
    )
''')

counter = 0

for url in urls.get('urls', []):
    try:
        # Extract slug from URL
        parsed_url = urlparse(url)
        slug = parsed_url.path.strip('/').split('/')[-1]
        
        # Fetch JSON data from API
        api_url = f'https://api.news.bitcoin.com/wp-json/bcn/v1/post?slug={slug}'
        sleep(0.5)
        response = requests.get(api_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        })
        response.raise_for_status()
        data = response.json()
        
        # Extract and clean content
        content_html = data.get('content', '')
        content_text = remove_html_tags(content_html)
        date = data.get('date', '')
        date_obj = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        unix_timestamp = int(date_obj.timestamp())
        
        # Insert into database
        cursor.execute('''
            INSERT OR IGNORE INTO messages (source, message, timestamp, negative_reactions, positive_reactions, neutral_reactions) VALUES (?, ?, ?, ?, ?, ?)
        ''', ("bitcoin.com", content_text, unix_timestamp, 0, 0, 0))
        
        counter += 1
        if counter % 10 == 0:
            conn.commit()
            print(f'on {counter} committed to database / {len(urls)}')
        
    except Exception as e:
        print(f'Error processing URL {url}: {e}')

# Final commit
conn.commit()
conn.close()