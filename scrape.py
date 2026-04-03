import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import time
import os
from sqlalchemy import create_engine

DB_USER = "postgres"
DB_PASSWORD = os.environ.get("DB_PASSWORD") 

DB_HOST = "aws-0-eu-west-1.pooler.supabase.com" 
DB_PORT = "6543" 
DB_NAME = "postgres"

db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
engine = create_engine(db_url)

scrape_time = datetime.now().isoformat()

categories = {
    "Gaming": "https://www.alternate.be/Gaming-laptops",
    "Business": "https://www.alternate.be/Zakelijke-laptops"
}

headers = {"User-Agent": "Mozilla/5.0"}
all_rows = []

for cat_name, base_url in categories.items():
    for page in range(2):
        url = f"{base_url}?page={page}"
        try:
            response = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(response.text, "html.parser")
            containers = soup.select("a.card")
            
            for item in containers:
                title = item.select_one(".product-name").get_text(strip=True)
                price_raw = item.select_one(".price").get_text(strip=True)
                old_price_el = item.select_one(".line-through")
                old_price_raw = old_price_el.get_text(strip=True) if old_price_el else price_raw
                
                all_rows.append({
                    "title": title,
                    "price_raw": price_raw,
                    "old_price_raw": old_price_raw,
                    "category": cat_name,
                    "scraped_at": scrape_time
                })
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")

df = pd.DataFrame(all_rows)
clean = lambda p: float(p.replace("€", "").replace(".", "").replace(",", ".").strip())
df["price_eur"] = df["price_raw"].apply(clean)
df["old_price_eur"] = df["old_price_raw"].apply(clean)
df["discount_pct"] = (((df["old_price_eur"] - df["price_eur"]) / df["old_price_eur"]) * 100).round(2)

df_final = df[['title', 'price_eur', 'discount_pct', 'category', 'scraped_at']]

avg_discount = df_final["discount_pct"].mean()
if avg_discount > 5.0:
    print(f"⚠️ BUSINESS ALERT: Average market discount is high ({avg_discount:.2f}%)!")

try:
    df_final.to_sql('laptops', engine, if_exists='append', index=False)
    print(f"Successfully appended {len(df_final)} rows with timestamp {scrape_time}")
except Exception as e:
    print(f"Database Error: {e}")
