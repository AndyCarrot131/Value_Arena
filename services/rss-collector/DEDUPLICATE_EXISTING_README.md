# Deduplicate Existing Data - Usage Guide

## Overview

`deduplicate_existing_data.py` is a one-time job to deduplicate existing articles in the database using three different methods:

1. **Content Hash** - Exact content matching (original method)
2. **URL Normalization** - Removes query parameters and checks for URL duplicates
3. **Title Similarity** - Fuzzy matching to find similar titles (threshold: 0.85)

## Features

- ✅ Three deduplication algorithms in one script
- ✅ Sequential execution (Content Hash → URL → Title Similarity)
- ✅ Configurable methods and thresholds
- ✅ Progress reporting for each method
- ✅ Safe operation (only marks duplicates, doesn't delete)
- ✅ AWS Secrets Manager integration

## Usage

### 1. Run All Methods (Recommended)

```bash
python deduplicate_existing_data.py
```

This runs all three methods sequentially:
- Content Hash deduplication
- URL normalization deduplication
- Title similarity deduplication (threshold=0.85)

### 2. Run Specific Methods

```bash
# Only content hash
python deduplicate_existing_data.py --methods content_hash

# Only URL normalization
python deduplicate_existing_data.py --methods url

# Only title similarity
python deduplicate_existing_data.py --methods title_similarity

# Combination
python deduplicate_existing_data.py --methods content_hash url
```

### 3. Adjust Title Similarity Threshold

```bash
# More strict (90% similarity required)
python deduplicate_existing_data.py --title-threshold 0.90

# More aggressive (80% similarity)
python deduplicate_existing_data.py --title-threshold 0.80

# Custom threshold with specific methods
python deduplicate_existing_data.py --methods url title_similarity --title-threshold 0.87
```

## Command Line Options

```
--methods [METHOD ...]
    Choose deduplication methods to run
    Choices: content_hash, url, title_similarity
    Default: all methods

--title-threshold FLOAT
    Similarity threshold for title matching (0.0 - 1.0)
    Default: 0.85
```

## Example Output

```
================================================================================
DEDUPLICATING EXISTING DATA
================================================================================
Methods to use: content_hash, url, title_similarity
Title similarity threshold: 0.85

Initial unique articles: 1,283

================================================================================
METHOD 1: CONTENT HASH DEDUPLICATION
================================================================================
Found 15 duplicate content_hash groups
✓ Marked 28 duplicates by content hash

================================================================================
METHOD 2: URL DEDUPLICATION (normalized)
================================================================================
Processing 1,255 articles...
Found 8 duplicate URL groups

  Normalized URL: https://bloomberg.com/news/articles/2026-01-07/tech-stocks...
    Primary: ID=18325
    Duplicate: ID=18342
               Original URL: https://bloomberg.com/news/articles/2026-01-07/tech-stocks?utm_source=twitter...

✓ Marked 12 duplicates by URL

================================================================================
METHOD 3: TITLE SIMILARITY DEDUPLICATION (threshold=0.85)
================================================================================
Processing 1,243 articles...

Sample title duplicates (first 5):

  Duplicate ID 18456 (similarity: 87.5%):
    "Apple announces new iPhone 16 with advanced AI features"
  Primary ID 18123:
    "Apple announces iPhone 16 featuring advanced AI capabilities"

✓ Marked 23 duplicates by title similarity

================================================================================
FINAL RESULTS
================================================================================

  Total Articles:      1,730
  Unique Articles:     1,220 (70.5%)
  Duplicate Articles:  510 (29.5%)

  Total marked in this run: 63
  Reduction from initial:   63

  Sample duplicate articles:
    1. [Bloomberg Markets] Xi Is Testing Japan's Ties With Trump by Escalatin...
       ID: 18342, Primary: 18330, Fetched: 2026-01-07 12:46:18.862198
    2. [CNBC Top News] Warner Bros. Dismisses Latest Paramount Bid as...
       ID: 18328, Primary: 18319, Fetched: 2026-01-07 12:31:13.698516

================================================================================
DEDUPLICATION COMPLETED SUCCESSFULLY
================================================================================
```

## How It Works

### Sequential Processing

The script runs methods sequentially to maximize efficiency:

1. **Content Hash** - Fast, exact matching removes obvious duplicates
2. **URL Normalization** - Catches URL duplicates with different tracking params
3. **Title Similarity** - Slowest method, only runs on remaining unique articles

Each method only processes articles that haven't been marked as duplicates yet.

### Method Details

#### 1. Content Hash
- Fastest method
- Exact content matching using SHA256 hash
- Catches articles with identical content

#### 2. URL Normalization
- Removes query parameters (`?utm_source=...`)
- Removes fragments (`#section`)
- Example:
  ```
  https://example.com/article?utm_source=twitter&ref=home
  → https://example.com/article
  ```

#### 3. Title Similarity
- Uses `SequenceMatcher` for fuzzy matching
- Default threshold: 0.85 (85% similarity)
- Only compares with non-duplicate articles
- Example matches:
  - "Apple announces new iPhone 16" vs "Apple announces iPhone 16"
  - Similarity: 87.5% → Marked as duplicate

## Database Changes

The script only updates two fields:
```sql
UPDATE news_articles
SET is_duplicate = TRUE,
    primary_article_id = <primary_article_id>
WHERE id = <duplicate_id>
```

**No articles are deleted** - duplicates are only marked for filtering.

## Performance Considerations

### Execution Time

- **Content Hash**: Very fast (~1-2 seconds for 1000 articles)
- **URL Normalization**: Fast (~5-10 seconds for 1000 articles)
- **Title Similarity**: Slow (~30-60 seconds for 1000 articles)

For large datasets (>5000 articles), title similarity may take several minutes.

### Memory Usage

- Content Hash: Minimal
- URL Normalization: Loads all URLs into memory (~1-2MB per 1000 articles)
- Title Similarity: Loads all titles into memory (~2-3MB per 1000 articles)

## Safety Features

1. **Only marks, never deletes** - Original data is preserved
2. **Keeps earliest article** - Sorted by `fetched_at ASC`
3. **Skips already marked duplicates** - Won't re-process
4. **Transaction safety** - Commits after each group/batch
5. **Detailed logging** - Shows what's being marked

## When to Run

### Initial Setup
Run once after importing historical data:
```bash
python deduplicate_existing_data.py
```

### After Bulk Import
If you imported a large batch of articles:
```bash
python deduplicate_existing_data.py --methods url title_similarity
```

### Periodic Cleanup
Run monthly to catch any duplicates that slipped through:
```bash
python deduplicate_existing_data.py --methods title_similarity --title-threshold 0.87
```

## Verification

After running, verify the results:

```sql
-- Count duplicates by method
SELECT
  COUNT(*) FILTER (WHERE is_duplicate) AS duplicates,
  COUNT(*) FILTER (WHERE NOT is_duplicate) AS unique_articles,
  COUNT(*) AS total
FROM news_articles;

-- Sample duplicates
SELECT id, title, primary_article_id, fetched_at
FROM news_articles
WHERE is_duplicate = TRUE
ORDER BY fetched_at DESC
LIMIT 10;

-- Verify primary articles exist
SELECT COUNT(*)
FROM news_articles d
LEFT JOIN news_articles p ON d.primary_article_id = p.id
WHERE d.is_duplicate = TRUE
  AND p.id IS NULL;  -- Should return 0
```

## Troubleshooting

### "No module named boto3"
Install dependencies:
```bash
pip install boto3 psycopg2-binary
```

### "Access denied to Secrets Manager"
Ensure your AWS credentials have access to `ai-stock-war/database-config` secret.

### Title similarity takes too long
Reduce the number of articles or use a higher threshold:
```bash
python deduplicate_existing_data.py --methods content_hash url
```

### Too many false positives in title similarity
Increase the threshold:
```bash
python deduplicate_existing_data.py --title-threshold 0.90
```

## Integration with RSS Collector

The real-time deduplication in `collector.py` uses the same algorithms but processes articles as they're collected. This script is for cleaning up existing data.

For new articles, the `SimpleDuplicateDetector` in `deduplication.py` provides:
- Real-time URL checking (with Redis cache)
- Real-time title similarity checking
- Hybrid detection (URL → Title)

See [DEDUPLICATION_USAGE.md](./DEDUPLICATION_USAGE.md) for real-time deduplication details.
