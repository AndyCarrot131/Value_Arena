#!/usr/bin/env python3
"""
Test script to verify deduplication is working correctly
"""

import os
import sys
import json
import boto3
import psycopg2
import redis
from datetime import datetime, timedelta

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from deduplication import NewsDeduplicator, SimpleDuplicateDetector


def load_config():
    """Load database config from Secrets Manager"""
    secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
    response = secrets_client.get_secret_value(SecretId='ai-stock-war/database-config')
    return json.loads(response['SecretString'])


def test_deduplication_status():
    """Test and display deduplication statistics"""

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
    print("📊 NEWS DEDUPLICATION STATUS CHECK")
    print("="*80)

    # 1. Overall statistics
    print("\n1️⃣  Overall Statistics:")
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE NOT is_duplicate OR is_duplicate IS NULL) AS unique_count,
            COUNT(*) FILTER (WHERE is_duplicate) AS duplicate_count,
            COUNT(*) AS total_count,
            MIN(fetched_at) AS earliest_article,
            MAX(fetched_at) AS latest_article
        FROM news_articles
    """)

    row = cursor.fetchone()
    unique_count, duplicate_count, total_count, earliest, latest = row

    print(f"   Total Articles:      {total_count:,}")
    print(f"   Unique Articles:     {unique_count:,} ({unique_count/total_count*100:.1f}%)")
    print(f"   Duplicate Articles:  {duplicate_count:,} ({duplicate_count/total_count*100:.1f}%)")
    print(f"   Date Range:          {earliest} → {latest}")

    # 2. Recent articles (last 24 hours)
    print("\n2️⃣  Last 24 Hours Activity:")
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE NOT is_duplicate OR is_duplicate IS NULL) AS unique_count,
            COUNT(*) FILTER (WHERE is_duplicate) AS duplicate_count,
            COUNT(*) AS total_count
        FROM news_articles
        WHERE fetched_at >= NOW() - INTERVAL '24 hours'
    """)

    row = cursor.fetchone()
    recent_unique, recent_dup, recent_total = row

    if recent_total > 0:
        print(f"   New Articles (24h):  {recent_total:,}")
        print(f"   Unique:              {recent_unique:,} ({recent_unique/recent_total*100:.1f}%)")
        print(f"   Duplicates:          {recent_dup:,} ({recent_dup/recent_total*100:.1f}%)")
    else:
        print("   ⚠️  No articles fetched in last 24 hours")

    # 3. Check if content_hash is being generated
    print("\n3️⃣  Content Hash Status:")
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE content_hash IS NOT NULL) AS with_hash,
            COUNT(*) FILTER (WHERE content_hash IS NULL) AS without_hash,
            COUNT(*) AS total
        FROM news_articles
    """)

    row = cursor.fetchone()
    with_hash, without_hash, total = row

    print(f"   With content_hash:   {with_hash:,} ({with_hash/total*100:.1f}%)")
    print(f"   Without hash:        {without_hash:,} ({without_hash/total*100:.1f}%)")

    # 4. Check if Comprehend entities/phrases are being stored
    print("\n4️⃣  AWS Comprehend Analysis:")
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE comprehend_entities IS NOT NULL) AS with_entities,
            COUNT(*) FILTER (WHERE comprehend_key_phrases IS NOT NULL) AS with_phrases,
            COUNT(*) AS total
        FROM news_articles
    """)

    row = cursor.fetchone()
    with_entities, with_phrases, total = row

    print(f"   With entities:       {with_entities:,} ({with_entities/total*100:.1f}%)")
    print(f"   With key phrases:    {with_phrases:,} ({with_phrases/total*100:.1f}%)")

    if with_entities == 0 and with_phrases == 0:
        print("   ⚠️  No Comprehend data found - using SimpleDuplicateDetector")
    else:
        print("   ✅ Using NewsDeduplicator (AWS Comprehend)")

    # 5. Check Redis cache
    print("\n5️⃣  Redis Cache Status:")
    try:
        r = redis.StrictRedis(
            host=config['redis_host'],
            port=6379,
            ssl=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            decode_responses=True
        )

        # Count cached fingerprints
        fingerprint_keys = list(r.scan_iter("news:fingerprint:*", count=1000))
        url_keys = list(r.scan_iter("news:url:*", count=1000))
        cluster_keys = list(r.scan_iter("news:cluster:*", count=1000))

        print(f"   Cached fingerprints: {len(fingerprint_keys):,}")
        print(f"   Cached URLs:         {len(url_keys):,}")
        print(f"   News clusters:       {len(cluster_keys):,}")

        if len(fingerprint_keys) > 0 or len(url_keys) > 0:
            print("   ✅ Redis cache is active")
        else:
            print("   ⚠️  Redis cache is empty (may be using DB-only deduplication)")

    except Exception as e:
        print(f"   ❌ Redis connection failed: {e}")

    # 6. Sample duplicate articles
    print("\n6️⃣  Sample Duplicate Articles:")
    cursor.execute("""
        SELECT
            id,
            title,
            source,
            primary_article_id,
            fetched_at
        FROM news_articles
        WHERE is_duplicate = TRUE
        ORDER BY fetched_at DESC
        LIMIT 5
    """)

    duplicates = cursor.fetchall()
    if duplicates:
        for i, (id, title, source, primary_id, fetched) in enumerate(duplicates, 1):
            print(f"   {i}. [{source}] {title[:60]}...")
            print(f"      ID: {id}, Primary: {primary_id}, Fetched: {fetched}")
    else:
        print("   No duplicates found (or deduplication not working)")

    # 7. Recommendations
    print("\n7️⃣  Diagnostic Recommendations:")

    if duplicate_count == 0 and total_count > 100:
        print("   ⚠️  WARNING: No duplicates detected in large dataset")
        print("   → Check if RSS collector is calling deduplication")
        print("   → Verify Redis connection")
        print("   → Check AWS Comprehend permissions")
    elif recent_dup == 0 and recent_total > 10:
        print("   ⚠️  WARNING: No duplicates in last 24h")
        print("   → Deduplication may not be running")
    else:
        print("   ✅ Deduplication appears to be working")

    print("\n" + "="*80 + "\n")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    try:
        test_deduplication_status()
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
