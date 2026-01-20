#!/usr/bin/env python3
"""
One-time job to deduplicate existing articles in the database
Supports three deduplication methods:
1. Content Hash (exact match)
2. URL Normalization (removes query parameters)
3. Title Similarity (fuzzy matching)
"""

import os
import sys
import json
import boto3
import psycopg2
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from difflib import SequenceMatcher

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))


def load_config():
    """Load database config from Secrets Manager"""
    secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
    response = secrets_client.get_secret_value(SecretId='ai-stock-war/database-config')
    return json.loads(response['SecretString'])


def normalize_url(url: str) -> str:
    """
    Normalize URL by removing query parameters and fragments

    Args:
        url: Original URL

    Returns:
        Normalized URL without query params
    """
    try:
        parsed = urlparse(url.lower().strip())
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            '',  # params
            '',  # query - REMOVED
            ''   # fragment - REMOVED
        ))
        return normalized
    except Exception:
        return url.lower().strip()


def calculate_title_similarity(title1: str, title2: str) -> float:
    """
    Calculate similarity between two titles

    Args:
        title1: First title
        title2: Second title

    Returns:
        Similarity score (0.0 - 1.0)
    """
    t1 = title1.lower().strip()
    t2 = title2.lower().strip()
    return SequenceMatcher(None, t1, t2).ratio()


def deduplicate_by_content_hash(cursor, conn):
    """Deduplicate by content_hash (original method)"""
    print("\n" + "="*80)
    print("METHOD 1: CONTENT HASH DEDUPLICATION")
    print("="*80)

    cursor.execute("""
        SELECT content_hash, COUNT(*) as count
        FROM news_articles
        WHERE content_hash IS NOT NULL
        AND (is_duplicate = FALSE OR is_duplicate IS NULL)
        GROUP BY content_hash
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
    """)

    duplicate_groups = cursor.fetchall()
    print(f"Found {len(duplicate_groups)} duplicate content_hash groups")

    total_marked = 0
    for content_hash, count in duplicate_groups:
        cursor.execute("""
            SELECT id, news_id, title, fetched_at
            FROM news_articles
            WHERE content_hash = %s
            AND (is_duplicate = FALSE OR is_duplicate IS NULL)
            ORDER BY fetched_at ASC
        """, (content_hash,))

        articles = cursor.fetchall()
        primary_id = articles[0][0]

        for article_id, news_id, title, fetched_at in articles[1:]:
            cursor.execute("""
                UPDATE news_articles
                SET is_duplicate = TRUE,
                    primary_article_id = %s
                WHERE id = %s
            """, (primary_id, article_id))
            total_marked += 1

        conn.commit()

    print(f"✓ Marked {total_marked} duplicates by content hash")
    return total_marked


def deduplicate_by_url(cursor, conn):
    """Deduplicate by normalized URL (removes query params)"""
    print("\n" + "="*80)
    print("METHOD 2: URL DEDUPLICATION (normalized)")
    print("="*80)

    # Get all non-duplicate articles
    cursor.execute("""
        SELECT id, news_id, url, title, fetched_at
        FROM news_articles
        WHERE (is_duplicate = FALSE OR is_duplicate IS NULL)
        ORDER BY fetched_at ASC
    """)

    articles = cursor.fetchall()
    print(f"Processing {len(articles)} articles...")

    url_groups = {}
    for article_id, news_id, url, title, fetched_at in articles:
        normalized = normalize_url(url)
        if normalized not in url_groups:
            url_groups[normalized] = []
        url_groups[normalized].append((article_id, news_id, url, title, fetched_at))

    # Find groups with duplicates
    duplicate_groups = {url: group for url, group in url_groups.items() if len(group) > 1}
    print(f"Found {len(duplicate_groups)} duplicate URL groups")

    total_marked = 0
    for normalized_url, group in duplicate_groups.items():
        # First article is primary
        primary_id = group[0][0]

        print(f"\n  Normalized URL: {normalized_url[:70]}...")
        print(f"    Primary: ID={primary_id}")

        for article_id, news_id, original_url, title, fetched_at in group[1:]:
            cursor.execute("""
                UPDATE news_articles
                SET is_duplicate = TRUE,
                    primary_article_id = %s
                WHERE id = %s
            """, (primary_id, article_id))
            total_marked += 1

            print(f"    Duplicate: ID={article_id}")
            print(f"               Original URL: {original_url[:70]}...")

        conn.commit()

    print(f"\n✓ Marked {total_marked} duplicates by URL")
    return total_marked


def deduplicate_by_title_similarity(cursor, conn, threshold=0.85):
    """Deduplicate by title similarity (fuzzy matching)"""
    print("\n" + "="*80)
    print(f"METHOD 3: TITLE SIMILARITY DEDUPLICATION (threshold={threshold})")
    print("="*80)

    # Get all non-duplicate articles
    cursor.execute("""
        SELECT id, news_id, title, fetched_at
        FROM news_articles
        WHERE (is_duplicate = FALSE OR is_duplicate IS NULL)
        ORDER BY fetched_at ASC
    """)

    articles = cursor.fetchall()
    print(f"Processing {len(articles)} articles...")

    seen = []
    duplicates_found = []
    total_marked = 0

    for article_id, news_id, title, fetched_at in articles:
        is_duplicate = False
        primary_id = None

        for seen_id, seen_title in seen:
            similarity = calculate_title_similarity(title, seen_title)
            if similarity >= threshold:
                is_duplicate = True
                primary_id = seen_id
                duplicates_found.append({
                    'duplicate_id': article_id,
                    'primary_id': seen_id,
                    'similarity': similarity,
                    'duplicate_title': title,
                    'primary_title': seen_title
                })
                break

        if is_duplicate:
            cursor.execute("""
                UPDATE news_articles
                SET is_duplicate = TRUE,
                    primary_article_id = %s
                WHERE id = %s
            """, (primary_id, article_id))
            total_marked += 1

            if total_marked % 10 == 0:
                conn.commit()
        else:
            seen.append((article_id, title))

    conn.commit()

    # Show sample duplicates
    if duplicates_found:
        print(f"\nSample title duplicates (first 5):")
        for dup in duplicates_found[:5]:
            print(f"\n  Duplicate ID {dup['duplicate_id']} (similarity: {dup['similarity']:.2%}):")
            print(f"    \"{dup['duplicate_title'][:70]}...\"")
            print(f"  Primary ID {dup['primary_id']}:")
            print(f"    \"{dup['primary_title'][:70]}...\"")

    print(f"\n✓ Marked {total_marked} duplicates by title similarity")
    return total_marked


def deduplicate_existing_data(methods=None, title_threshold=0.85):
    """
    Find and mark duplicate articles using multiple methods

    Args:
        methods: List of methods to use ['content_hash', 'url', 'title_similarity']
                 If None, uses all methods
        title_threshold: Threshold for title similarity (default 0.85)
    """
    if methods is None:
        methods = ['content_hash', 'url', 'title_similarity']

    config = load_config()

    # Connect to database
    conn = psycopg2.connect(
        host=config['db_host'],
        port=config['db_port'],
        database=config['db_name'],
        user=config['db_user'],
        password=config['db_password']
    )

    cursor = conn.cursor()

    print("\n" + "="*80)
    print("DEDUPLICATING EXISTING DATA")
    print("="*80)
    print(f"Methods to use: {', '.join(methods)}")
    print(f"Title similarity threshold: {title_threshold}")

    # Initial stats
    cursor.execute("""
        SELECT COUNT(*) FROM news_articles
        WHERE (is_duplicate = FALSE OR is_duplicate IS NULL)
    """)
    initial_count = cursor.fetchone()[0]
    print(f"\nInitial unique articles: {initial_count:,}")

    total_marked = 0

    # Execute selected methods
    if 'content_hash' in methods:
        marked = deduplicate_by_content_hash(cursor, conn)
        total_marked += marked

    if 'url' in methods:
        marked = deduplicate_by_url(cursor, conn)
        total_marked += marked

    if 'title_similarity' in methods:
        marked = deduplicate_by_title_similarity(cursor, conn, threshold=title_threshold)
        total_marked += marked

    # Final verification
    print("\n" + "="*80)
    print("FINAL RESULTS")
    print("="*80)

    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE NOT is_duplicate OR is_duplicate IS NULL) AS unique_count,
            COUNT(*) FILTER (WHERE is_duplicate) AS duplicate_count,
            COUNT(*) AS total_count
        FROM news_articles
    """)

    unique_count, duplicate_count, total_count = cursor.fetchone()

    print(f"\n  Total Articles:      {total_count:,}")
    print(f"  Unique Articles:     {unique_count:,} ({unique_count/total_count*100:.1f}%)")
    print(f"  Duplicate Articles:  {duplicate_count:,} ({duplicate_count/total_count*100:.1f}%)")
    print(f"\n  Total marked in this run: {total_marked:,}")
    print(f"  Reduction from initial:   {initial_count - unique_count:,}")

    # Show sample duplicates
    print(f"\n  Sample duplicate articles:")
    cursor.execute("""
        SELECT
            id,
            LEFT(title, 60) AS title,
            source,
            primary_article_id,
            fetched_at
        FROM news_articles
        WHERE is_duplicate = TRUE
        ORDER BY fetched_at DESC
        LIMIT 5
    """)

    duplicates = cursor.fetchall()
    for i, (article_id, title, source, primary_id, fetched) in enumerate(duplicates, 1):
        print(f"    {i}. [{source}] {title}...")
        print(f"       ID: {article_id}, Primary: {primary_id}, Fetched: {fetched}")

    print("\n" + "="*80)
    print("DEDUPLICATION COMPLETED SUCCESSFULLY")
    print("="*80 + "\n")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Deduplicate news articles using multiple methods'
    )
    parser.add_argument(
        '--methods',
        nargs='+',
        choices=['content_hash', 'url', 'title_similarity'],
        default=['content_hash', 'url', 'title_similarity'],
        help='Deduplication methods to use (default: all)'
    )
    parser.add_argument(
        '--title-threshold',
        type=float,
        default=0.85,
        help='Similarity threshold for title matching (default: 0.85)'
    )

    args = parser.parse_args()

    try:
        deduplicate_existing_data(
            methods=args.methods,
            title_threshold=args.title_threshold
        )
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
