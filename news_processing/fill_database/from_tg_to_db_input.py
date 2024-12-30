import os
import json
import sqlite3

# Define reaction categories
negative_emojis = {"🤡", "🤬", "😡", "💩", "👎", "🤮", "😢"}
positive_emojis = {"👍", "❤️", "❤️‍🔥", "👌", "🍾", "🔥", "👏", "🆒", "💘", "😘"}

# Paths
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir, os.pardir))
db_path = os.path.join(parent_dir, "data.db")
data_dir = os.path.join(script_dir, "telegram_data")

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message TEXT,
    timestamp INTEGER,
    source TEXT,
    negative_reactions INTEGER,
    positive_reactions INTEGER,
    neutral_reactions INTEGER
)
""")
conn.commit()

# Process each JSON file in data directory
for filename in os.listdir(data_dir):
    if filename.endswith(".json"):
        source = os.path.splitext(filename)[0]
        file_path = os.path.join(data_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            for msg in data.get("messages", []):
                # Concatenate plain text
                plain_text = ''.join([entity['text'] for entity in msg.get('text_entities', []) if entity['type'] == 'plain'])
                if len(plain_text) < 50:
                    continue
                timestamp = int(msg.get('date_unixtime', 0))
                reactions = msg.get('reactions', [])
                neg = pos = neu = 0
                for reaction in reactions:
                    emoji = reaction.get('emoji')
                    count = reaction.get('count', 0)
                    if emoji in negative_emojis:
                        neg += count
                    elif emoji in positive_emojis:
                        pos += count
                    else:
                        neu += count
                # Insert into database
                cursor.execute("""
                INSERT INTO messages (message, timestamp, source, negative_reactions, positive_reactions, neutral_reactions)
                VALUES (?, ?, ?, ?, ?, ?)
                """, (plain_text, timestamp, source, neg, pos, neu))
        conn.commit()

conn.close()