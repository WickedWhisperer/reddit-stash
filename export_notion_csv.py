#!/usr/bin/env python3
"""
Export a CSV suitable for importing into Notion (Title, Subreddit, Permalink, URL, Date, Excerpt)

Usage:
  python export_notion_csv.py --db reddit_archive.db --out notion_import.csv

"""
import sqlite3
import csv
import argparse
from datetime import datetime


def to_iso(ts):
    if not ts:
        return ''
    try:
        return datetime.utcfromtimestamp(int(ts)).isoformat() + 'Z'
    except:
        return str(ts)


def export(db_path, out_csv):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('SELECT id, title, subreddit, permalink, url, created_utc, body FROM posts ORDER BY created_utc DESC')
    rows = c.fetchall()
    with open(out_csv, 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh)
        w.writerow(['Title','Subreddit','Permalink','URL','Date','Excerpt'])
        for r in rows:
            id, title, subreddit, permalink, url, created_utc, body = r
            excerpt = (body or '')[:250].replace('\n', ' ')
            w.writerow([title, subreddit, permalink, url, to_iso(created_utc), excerpt])
    conn.close()
    print('Wrote', out_csv)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--db', default='reddit_archive.db')
    p.add_argument('--out', default='notion_import.csv')
    args = p.parse_args()
    export(args.db, args.out)
