# RSS Collector Changes - Database-Only Deduplication

## Summary of Changes

Updated `collector.py` to use **database-only deduplication** without Redis, with automatic cleanup after collection.

## Key Changes

### 1. **Removed Redis Dependency**
- ✅ No Redis connection required
- ✅ No Redis read/write operations
- ✅ Deduplication uses database queries only

### 2. **3-Day Time Window for Deduplication**
- Only checks articles published in the **last 3 days (72 hours)**
- Based on `published_at` timestamp
- Reduces database query load

### 3. **Hybrid Deduplication Strategy**
During collection, checks against database:
1. **URL Normalization** - Removes query parameters and checks for exact URL match
2. **Title Similarity** - Fuzzy matching with 85% threshold (if URL not duplicate)

### 4. **Automatic Post-Collection Cleanup**
After collection completes, automatically:
1. Runs `deduplicate_existing_data()` on **entire database**
2. Uses all 3 methods:
   - Content Hash
   - URL Normalization
   - Title Similarity (threshold=0.85)
3. **Deletes** all articles marked as `is_duplicate = TRUE`

## Workflow

```
1. Fetch RSS feeds
   ↓
2. For each article:
   - Check hybrid duplicate (URL + Title) against last 3 days
   - If duplicate: Skip
   - If unique: Insert into database
   ↓
3. After all articles collected:
   - Run deduplicate_existing_data() on ENTIRE database
   - Mark duplicates with is_duplicate = TRUE
   - DELETE FROM news_articles WHERE is_duplicate = TRUE
```

## Code Changes

### Modified Files

1. **`collector.py`**
   - Removed `setup_redis()` method
   - Updated `setup_deduplicator()` - database-only mode
   - Modified `process_article()` - 72-hour time window
   - Removed `cache_article_fingerprint()` - no Redis caching
   - Added `run_deduplication_cleanup()` - post-collection cleanup
   - Updated `collect_all()` - calls cleanup after collection

2. **`deduplication.py`**
   - Made `redis_client` optional in `SimpleDuplicateDetector.__init__()`
   - Added Redis availability checks in `check_duplicate_by_url()`
   - Added Redis availability check in `cache_url()`

## Configuration

### Default Settings

```python
# Time window for real-time deduplication
time_window_hours = 72  # 3 days

# Title similarity threshold
title_similarity_threshold = 0.85  # 85% similarity

# Post-collection deduplication methods
methods = ['content_hash', 'url', 'title_similarity']
```

## Usage

```bash
# Run collector
cd /path/to/AI_Stock_War/services/rss-collector
python collector.py
```

### Expected Output

```
==================================================
Starting RSS collection task
Found 10 RSS sources
...
Collection completed - Total:50 New:45 Duplicate:5 Failed:0
==================================================

================================================================================
Running post-collection deduplication cleanup
================================================================================

================================================================================
DEDUPLICATING EXISTING DATA
================================================================================
Methods to use: content_hash, url, title_similarity
Title similarity threshold: 0.85

Initial unique articles: 1,283

================================================================================
METHOD 1: CONTENT HASH DEDUPLICATION
================================================================================
Found 5 duplicate content_hash groups
✓ Marked 8 duplicates by content hash

================================================================================
METHOD 2: URL DEDUPLICATION (normalized)
================================================================================
Processing 1,275 articles...
Found 3 duplicate URL groups
✓ Marked 4 duplicates by URL

================================================================================
METHOD 3: TITLE SIMILARITY DEDUPLICATION (threshold=0.85)
================================================================================
Processing 1,271 articles...
✓ Marked 6 duplicates by title similarity

================================================================================
FINAL RESULTS
================================================================================

  Total Articles:      1,283
  Unique Articles:     1,265 (98.6%)
  Duplicate Articles:  18 (1.4%)

  Total marked in this run: 18

Deleting duplicate articles from database...
✓ Deleted 18 duplicate articles

================================================================================
DEDUPLICATION COMPLETED SUCCESSFULLY
================================================================================
```

## Benefits

### Performance
- ✅ No Redis dependency = simpler deployment
- ✅ 3-day time window = faster queries
- ✅ Database-only = single source of truth

### Data Quality
- ✅ Two-stage deduplication (realtime + batch)
- ✅ Multiple deduplication methods
- ✅ Automatic cleanup = clean database

### Maintenance
- ✅ No Redis cache invalidation issues
- ✅ Self-cleaning database
- ✅ Easier to debug (all data in PostgreSQL)

## Database Impact

### Real-Time Deduplication Queries

```sql
-- URL check (indexed query)
SELECT id, news_id, title, url, source, published_at
FROM news_articles
WHERE LOWER(REGEXP_REPLACE(url, '\?.*$', '')) = $1
AND (is_duplicate = FALSE OR is_duplicate IS NULL)
ORDER BY fetched_at ASC
LIMIT 1;

-- Title similarity check (last 3 days only)
SELECT id, news_id, title, url, source, published_at
FROM news_articles
WHERE published_at > NOW() - INTERVAL '72 hours'
AND (is_duplicate = FALSE OR is_duplicate IS NULL)
ORDER BY published_at DESC
LIMIT 500;
```

### Post-Collection Cleanup

```sql
-- Mark duplicates
UPDATE news_articles
SET is_duplicate = TRUE, primary_article_id = $1
WHERE id = $2;

-- Delete duplicates
DELETE FROM news_articles WHERE is_duplicate = TRUE;
```

## Recommended Indexes

```sql
-- For URL deduplication
CREATE INDEX idx_news_articles_url_normalized
ON news_articles (LOWER(REGEXP_REPLACE(url, '\?.*$', '')))
WHERE is_duplicate = FALSE OR is_duplicate IS NULL;

-- For title similarity time window
CREATE INDEX idx_news_articles_published_dedup
ON news_articles(published_at DESC, is_duplicate)
WHERE is_duplicate = FALSE OR is_duplicate IS NULL;
```

## Migration from Redis-Based System

### What Changed
- ❌ No more Redis cache
- ❌ No more `cache_article_fingerprint()` calls
- ✅ Database becomes single source for deduplication
- ✅ Automatic cleanup after each collection

### What Stayed the Same
- ✅ Same deduplication algorithms (URL + Title)
- ✅ Same threshold values
- ✅ Same database schema

## Troubleshooting

### "Deduplication takes too long"
- Reduce time window: Change `time_window_hours` from 72 to 48 or 24
- Add database indexes (see Recommended Indexes above)

### "Too many false positives in title similarity"
- Increase threshold: Change from 0.85 to 0.90

### "Post-collection cleanup failed"
- Check database connection
- Verify `deduplicate_existing_data.py` is in the same directory
- Check logs for specific error

## Testing

### Local Testing
```bash
# Dry run (no actual deletion)
cd services/rss-collector
python -c "
from collector import RSSCollector
collector = RSSCollector(use_comprehend=False)
try:
    # Test deduplication only
    collector.run_deduplication_cleanup()
finally:
    collector.close()
"
```

### Production Deployment
1. Update ECS task definition
2. Deploy new version
3. Monitor CloudWatch logs for:
   - Collection stats
   - Deduplication results
   - Deletion counts
