import sqlite3
from transformers import pipeline

# Load sentiment analysis models
cryptobert_pipe = pipeline("text-classification", model="ElKulako/cryptobert")
distilroberta_pipe = pipeline("text-classification", model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")
finbert_pipe = pipeline("text-classification", model="ProsusAI/finbert")
finbert_tone_pipe = pipeline("text-classification", model="yiyanghkust/finbert-tone")

# Connect to SQLite database
conn = sqlite3.connect("summarize_news.db")
cursor = conn.cursor()

# Create new tables for each model
cursor.execute("""
    CREATE TABLE IF NOT EXISTS cryptobert (
        id INTEGER PRIMARY KEY,
        positive REAL,
        neutral REAL,
        negative REAL
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS distilroberta_ffnsa (
        id INTEGER PRIMARY KEY,
        positive REAL,
        neutral REAL,
        negative REAL
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS finbert (
        id INTEGER PRIMARY KEY,
        positive REAL,
        neutral REAL,
        negative REAL
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS finbert_tone (
        id INTEGER PRIMARY KEY,
        positive REAL,
        neutral REAL,
        negative REAL
    )
""")
conn.commit()

# Fetch all news content from the messages table
cursor.execute("SELECT id, summarization FROM messages")
rows = cursor.fetchall()

# Process each news item and insert sentiment scores into the respective tables
for row in rows:
    id, content = row
    try:
        # Get predictions for each model
        cryptobert_preds = {res['label'].lower(): res['score'] for res in cryptobert_pipe(content, return_all_scores=True)[0]}
        distilroberta_preds = {res['label'].lower(): res['score'] for res in distilroberta_pipe(content, return_all_scores=True)[0]}
        finbert_preds = {res['label'].lower(): res['score'] for res in finbert_pipe(content, return_all_scores=True)[0]}
        finbert_tone_preds = {res['label'].lower(): res['score'] for res in finbert_tone_pipe(content, return_all_scores=True)[0]}

        # Insert scores into the respective tables
        cursor.execute("INSERT INTO cryptobert (id, positive, neutral, negative) VALUES (?, ?, ?, ?)", 
                       (id, cryptobert_preds.get('positive', 0), cryptobert_preds.get('neutral', 0), cryptobert_preds.get('negative', 0)))
        cursor.execute("INSERT INTO distilroberta_ffnsa (id, positive, neutral, negative) VALUES (?, ?, ?, ?)", 
                       (id, distilroberta_preds.get('positive', 0), distilroberta_preds.get('neutral', 0), distilroberta_preds.get('negative', 0)))
        cursor.execute("INSERT INTO finbert (id, positive, neutral, negative) VALUES (?, ?, ?, ?)", 
                       (id, finbert_preds.get('positive', 0), finbert_preds.get('neutral', 0), finbert_preds.get('negative', 0)))
        cursor.execute("INSERT INTO finbert_tone (id, positive, neutral, negative) VALUES (?, ?, ?, ?)", 
                       (id, finbert_tone_preds.get('positive', 0), finbert_tone_preds.get('neutral', 0), finbert_tone_preds.get('negative', 0)))

    except Exception as e:
        print(f"Error processing ID {id}: {e}")

# Commit changes and close the database connection
conn.commit()
conn.close()

print("Sentiment analysis completed and results saved to database.")