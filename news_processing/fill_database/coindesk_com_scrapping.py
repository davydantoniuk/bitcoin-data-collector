import json
import re
import sqlite3
from time import sleep
import requests
from urllib.parse import urlparse
import os
from datetime import datetime
import html

def extract_text(text):
    metadata_match = re.search(r'<script id="fusion-metadata" type="application/javascript">(.*?)</script>', text, re.DOTALL)
    if metadata_match:
        metadata = metadata_match.group(1)
    global_content_match = re.search(r'Fusion\.globalContent\s*=\s*(\{.*?\});', metadata, re.DOTALL)
    if global_content_match:
        fusion_global_content = json.loads(global_content_match.group(1))
    text = ""
    for content in fusion_global_content.get("content", {}).get('content_elements', []):
        if content.get('type') == 'text':
            text += content.get('content', '')
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)

    return {
        "timestamp": int(datetime.strptime(fusion_global_content.get('content', {}).get('created_date', ''), '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()),
        "text": html.unescape(text)
    }

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir, os.pardir))
db_path = os.path.join(parent_dir, "temp.db")
data_path = os.path.join(script_dir, "coindesk_com_data/tg_coindesk.json")

# Load URLs from post_urls.json
with open(data_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
messages = data.get('messages', [])
urls = [
    entity.get("text")
    for message in messages
    for entity in message.get("text_entities", [])
    if entity.get("type") == "link"
]
urls = urls + [
    entity.get("href")
    for message in messages
    for entity in message.get("text_entities", [])
    if entity.get("type") == "text_link"
]

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message TEXT,
    timestamp INTEGER,
    source TEXT,
    negative_reactions INTEGER,
    positive_reactions INTEGER,
    neutral_reactions INTEGER
)
''')

counter = 0

urls = list(filter(lambda x: all(keyword not in x for keyword in ["podcast", "layer2", "static", "com/business", "com/policy", "com/tv", "com/learn"]), urls[10055:]))
for url in urls[350:]:
    try:
        # Extract slug from URL
        parsed_url = urlparse(url)
        slug = parsed_url.path.strip('/').split('/')[-1]
        # Fetch JSON data from API
        response = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        })
        response.raise_for_status()
        content_text = extract_text(response.text)
        # Insert into database
        cursor.execute('''
            INSERT OR IGNORE INTO messages (source, message, timestamp, negative_reactions, positive_reactions, neutral_reactions) VALUES (?, ?, ?, ?, ?, ?)
        ''', ("coindesk.com", content_text["timestamp"], content_text["text"], 0, 0, 0))
        
        counter += 1
        if counter % 10 == 0:
            conn.commit()
            print(f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}]: on {counter} committed to database / {len(urls)}')
        
    except Exception as e:
        print(f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}]: Error processing URL\n{url}')

# Final commit
conn.commit()
conn.close()