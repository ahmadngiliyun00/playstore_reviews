# scrape.py
# Scraping Google Play Store reviews (mandiri) + labeling 3 kelas
# Output:
# - playstore_reviews_raw.csv
# - playstore_reviews_labeled.csv

from __future__ import annotations

import re
import sys
import time
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
from google_play_scraper import Sort, reviews


APP_ID = "com.netmarble.sololv"
LANGS = ["id", "en"]     # ambil Indonesia + Inggris
COUNTRY = "id"
TARGET_N_PER_LANG = 6000  # per bahasa (nanti dedup)
BATCH_SIZE = 200  # max 200 per request
SLEEP_SEC = 0.25  # jeda ringan agar tidak terlalu agresif

OUT_RAW = "playstore_reviews_raw.csv"
OUT_LABELED = "playstore_reviews_labeled.csv"


def clean_text(text: str) -> str:
    """Cleaning ringan saja (opsional): rapikan spasi dan karakter kontrol."""
    text = re.sub(r"\s+", " ", str(text)).strip()
    return text


def rating_to_label(r: int) -> str:
    """Mapping rating bintang -> 3 kelas."""
    if r <= 2:
        return "negatif"
    elif r == 3:
        return "netral"
    else:
        return "positif"


def fetch_reviews(app_id: str, target_n: int, lang: str) -> pd.DataFrame:
    all_reviews = []
    token = None

    while len(all_reviews) < target_n:
        r, token = reviews(
            app_id,
            lang=lang,
            country=COUNTRY,
            sort=Sort.NEWEST,
            count=BATCH_SIZE,
            continuation_token=token
        )
        if not r:
            break
        all_reviews.extend(r)
        if token is None:
            break
        time.sleep(SLEEP_SEC)

    df = pd.DataFrame(all_reviews)
    df = df[["content", "score", "at"]].dropna()
    df = df.rename(columns={"content": "text", "score": "rating", "at": "date"})
    df["text"] = df["text"].apply(clean_text)
    df = df[df["text"].str.len() > 0].drop_duplicates(subset=["text"]).reset_index(drop=True)
    df["rating"] = df["rating"].astype(int)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").astype(str)
    df["lang"] = lang
    return df


def main() -> int:
    print(f"[INFO] Mulai scraping: {APP_ID} | langs={LANGS} country={COUNTRY}")
    start = datetime.now()

    dfs = []
    for lang in LANGS:
        print(f"[INFO] Scrape lang={lang}")
        df_lang = fetch_reviews(APP_ID, TARGET_N_PER_LANG, lang)
        print(f"[INFO] lang={lang} total setelah dedup: {len(df_lang)}")
        dfs.append(df_lang)

    df_raw = pd.concat(dfs, ignore_index=True)
    df_raw = df_raw.drop_duplicates(subset=["text"]).reset_index(drop=True)

    print(f"\n[RESULT] Total gabungan setelah dedup: {len(df_raw)}")

    print(f"\n[RESULT] Total setelah dedup: {len(df_raw)}")
    df_raw.to_csv(OUT_RAW, index=False)
    print(f"[SAVED] {OUT_RAW}")

    # Labeling
    df_labeled = df_raw.copy()
    df_labeled["label"] = df_labeled["rating"].apply(rating_to_label)
    df_labeled = df_labeled[["text", "label"]]

    # Distribusi label
    dist = df_labeled["label"].value_counts()
    print("\n[INFO] Distribusi label:")
    print(dist)

    df_labeled.to_csv(OUT_LABELED, index=False)
    print(f"[SAVED] {OUT_LABELED}")

    end = datetime.now()
    print(f"\n[DONE] Durasi: {end - start}")

    # Minimal kriteria dataset
    if len(df_raw) < 3000:
        print(
            "\n[WARNING] Data < 3000 setelah dedup. "
            "Solusi cepat: naikkan TARGET_N_PER_LANG atau tambah APP_ID kedua.",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
