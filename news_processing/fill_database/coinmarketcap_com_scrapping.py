import requests
import json
import os
import re
import time

def fetch_all_article_urls(base_url, headers):
    page = 1
    size = 1000
    all_urls = []

    while True:
        url = f"{base_url}?page={page}&size={size}&sort=-publishedOn"
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            posts = data.get("data", [])            
            if not posts:
                break
            for post in posts:
                source_url = post.get("meta", {}).get("sourceUrl", "")
                all_urls.append(source_url)
            page += 1
            print(f"Fetched {len(all_urls)} URLs so far, current page: {1}")
            time.sleep(2)  # Add a 5-second interval between requests
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            break

    return all_urls

def save_urls(file_path, urls):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as file:
        json.dump({"urls": urls}, file, indent=4)

if __name__ == "__main__":
    base_url = "https://api.coinmarketcap.com/content/v3/news/cryptown"
    base_dir = os.path.dirname(__file__)
    file_path = os.path.join(base_dir, "coinmarketcap_data", "post_urls.json")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    urls = fetch_all_article_urls(base_url, headers)
    save_urls(file_path, urls)