#!/usr/bin/env python3
"""
json_to_sqlite.py

Walk `reddit/` archive produced by reddit-stash and build a SQLite DB with
useful columns and an FTS5 full text index on title + body.

Usage:
  python json_to_sqlite.py --input reddit --output reddit_archive.db

"""
import os
import json
import sqlite3
import argparse
from datetime import datetime


def iter_json_files(base_dir):
    for root, dirs, files in os.walk(base_dir):
        for f in files:
            if f.lower().endswith('.json'):
                yield os.path.join(root, f)


def normalize_post(obj):
    # obj = parsed JSON for a post/comment
    return {
        'id': obj.get('id') or obj.get('name'),
        'type': obj.get('kind') or obj.get('type') or 'post',
        'subreddit': obj.get('subreddit') or obj.get('subreddit_name_prefixed', '').replace('r/', '') or None,
        'title': obj.get('title') or '',
        'author': obj.get('author') or '',
        'body': obj.get('selftext') or obj.get('body') or '',
        'permalink': obj.get('permalink') or obj.get('url') or None,
        'url': obj.get('url') or None,
        'created_utc': obj.get('created_utc') or obj.get('created') or None,
        'raw': obj
    }


def ensure_schema(conn):
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id TEXT PRIMARY KEY,
        type TEXT,
        subreddit TEXT,
        title TEXT,
        author TEXT,
        body TEXT,
        permalink TEXT,
        url TEXT,
        created_utc INTEGER,
        scraped_utc INTEGER,
        raw_json TEXT
    );
    """)
    # FTS5 full text search table
    c.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5(title, body, content='posts', content_rowid='rowid');
    """)
    # Indexes
    c.execute('CREATE INDEX IF NOT EXISTS idx_subreddit ON posts(subreddit);')
    c.execute('CREATE INDEX IF NOT EXISTS idx_created ON posts(created_utc);')
    conn.commit()


def insert_post(conn, post):
    c = conn.cursor()
    now = int(datetime.utcnow().timestamp())
    raw_text = json.dumps(post['raw'], ensure_ascii=False)
    try:
        c.execute("""
        INSERT OR REPLACE INTO posts (id, type, subreddit, title, author, body, permalink, url, created_utc, scraped_utc, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            post['id'], post['type'], post['subreddit'], post['title'], post['author'], post['body'],
            post['permalink'], post['url'], post['created_utc'], now, raw_text
        ))
        # update FTS
        c.execute("INSERT INTO posts_fts(rowid, title, body) VALUES (last_insert_rowid(), ?, ?)", (post['title'], post['body']))
    except Exception as e:
        print('Insert failed for', post.get('id'), e)


def build_db(input_dir, output_db, vacuum=False):
    if os.path.exists(output_db):
        print('Opening existing DB', output_db)
    conn = sqlite3.connect(output_db)
    ensure_schema(conn)
    processed = 0
    for path in iter_json_files(input_dir):
        try:
            with open(path, 'r', encoding='utf-8') as fh:
                obj = json.load(fh)
        except Exception as e:
            # sometimes files may be partial or not JSON; skip
            print('Skipping', path, 'error', e)
            continue
        post = normalize_post(obj)
        if not post['id']:
            # some files might be metadata; skip
            continue
        insert_post(conn, post)
        processed += 1
        if processed % 500 == 0:
            conn.commit()
            print('Processed', processed)
    conn.commit()
    if vacuum:
        conn.execute('VACUUM')
    conn.close()
    print('Done. Processed', processed, 'items')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--input', default='reddit', help='Path to reddit/ archive folder')
    p.add_argument('--output', default='reddit_archive.db', help='Output sqlite db file')
    p.add_argument('--vacuum', action='store_true')
    args = p.parse_args()
    build_db(args.input, args.output, vacuum=args.vacuum)
