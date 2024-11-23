import json
import re
import sqlite3
from time import sleep
import requests
from urllib.parse import urlparse
import os
from datetime import datetime
import html
import urllib
import unidecode


def extract_text(text):
    fullText = re.search(
        r'fullText="(.*?);[a-zA-Z]{2}.audio=', text, re.DOTALL)
    dateISOFull = re.search(r'dateISOFull="(.*?)"',
                            text, re.DOTALL).group(0)[13:-1]

    content = unidecode.unidecode(re.sub(re.compile(
        '<.*?>'), '', html.unescape(fullText.group(0)[10:-11].encode().decode('unicode_escape'))))

    return {
        "timestamp": int(datetime.strptime(dateISOFull, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()),
        "text": content
    }


script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir, os.pardir))
db_path = os.path.join(parent_dir, "data/temp2.db")
data_path = os.path.join(
    parent_dir, "data/news_data/cointelegraph_com_data/urls.json")

# Load URLs from post_urls.json
with open(data_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
urls = list(data)
# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message TEXT,
    timestamp INTEGER,
    source TEXT
)
''')

counter = 0

urls = urls[12500:]
for url in urls:
    try:
        # Extract slug from URL
        parsed_url = urlparse(url)
        slug = parsed_url.path.strip('/').split('/')[-1]
        # Fetch JSON data from API
        req = urllib.request.Request(url)
        req.add_header(
            'User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0')
        req.add_header(
            'Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8')
        req.add_header('Accept-Language', 'en-US,en;q=0.5')

        response = urllib.request.urlopen(req).read().decode('utf-8')

        content_text = extract_text(response)
        # Insert into database
        cursor.execute('''
            INSERT OR IGNORE INTO messages (message, timestamp, source) VALUES (?, ?, ?)
        ''', (content_text["text"], content_text["timestamp"], url))

        counter += 1
        if counter % 10 == 0:
            conn.commit()
            print(f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                      }]: on {counter} committed to database / {len(urls)}')

    except Exception as e:
        print(f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                  }]: Error processing URL\n{url} {e}')

# Final commit
conn.commit()
conn.close()
