import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import time
import os
from sqlalchemy import create_engine

DB_PASSWORD = os.environ.get("DB_PASSWORD") 
DB_HOST = "db.hpjsjnyuojemrayhjuem.supabase.co"
db_url = f"postgresql://postgres:{DB_PASSWORD}@{DB_HOST}:5432/postgres?sslmode=require"
engine = create_engine(db_url)

scrape_time = datetime.now().isoformat()
categories = {
    "Gaming": "https://www.alternate.be/Gaming-laptops",
    "Business": "https://www.alternate.be/Zakelijke-laptops"
}

all_rows = []
for cat_name, url in categories.items():
    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(res.text, "html.parser")
    for item in soup.select("a.card"):
        title = item.select_one(".product-name").text.strip()
        price_raw = item.select_one(".price").text.strip()
        old_price_el = item.select_one(".line-through")
        old_price_raw = old_price_el.text.strip() if old_price_el else price_raw
        
        all_rows.append({
            "title": title, "price_raw": price_raw, "old_price_raw": old_price_raw,
            "category": cat_name, "scraped_at": scrape_time
        })
    time.sleep(1)

df = pd.DataFrame(all_rows)
clean = lambda p: float(p.replace("€", "").replace(".", "").replace(",", ".").strip())
df["price_eur"] = df["price_raw"].apply(clean)
df["old_price_eur"] = df["old_price_raw"].apply(clean)
df["discount_pct"] = (((df["old_price_eur"] - df["price_eur"]) / df["old_price_eur"]) * 100).round(2)

df_final = df[['title', 'price_eur', 'discount_pct', 'category', 'scraped_at']]
df_final.to_sql('laptops', engine, if_exists='append', index=False)
print("Data successfully pushed to Supabase!")
