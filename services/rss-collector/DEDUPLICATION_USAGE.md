# Deduplication Usage Guide

## Overview

The `SimpleDuplicateDetector` class now supports three deduplication strategies:

1. **URL Deduplication** - Removes query parameters and checks for exact URL match
2. **Title Similarity** - Uses fuzzy matching (SequenceMatcher) to find similar titles
3. **Hybrid** - Combines both methods (URL first, then Title Similarity)

## Updated Features

### 1. URL Normalization (Removes Query Params)

```python
from deduplication import SimpleDuplicateDetector

detector = SimpleDuplicateDetector(redis_client, db_conn)

# Example URLs that will be treated as duplicates:
url1 = "https://example.com/article?utm_source=twitter&id=123"
url2 = "https://example.com/article?utm_source=facebook&ref=home"

# Both normalize to: "https://example.com/article"
normalized = detector.normalize_url(url1)
# Returns: "https://example.com/article"
```

### 2. URL Duplicate Check (with normalization)

```python
# Check if URL is duplicate (automatically removes query params)
duplicate = detector.check_duplicate_by_url(
    url="https://example.com/article?utm_source=twitter",
    use_normalized=True  # Default: True
)

if duplicate:
    print(f"Duplicate found: {duplicate['news_id']}")
    print(f"Original URL: {duplicate['url']}")
```

### 3. Title Similarity Check

```python
# Check for similar titles within 24 hours
duplicate = detector.check_duplicate_by_title_similarity(
    title="Apple announces new iPhone 16",
    threshold=0.85,  # 85% similarity required
    time_window_hours=24
)

if duplicate:
    print(f"Similar title found: {duplicate['title']}")
    print(f"Similarity score: {duplicate['similarity_score']:.2%}")
```

### 4. Hybrid Deduplication (Recommended)

This method combines both URL and Title Similarity checks:

```python
# Stage 1: Check URL (normalized)
# Stage 2: Check Title Similarity (if URL not duplicate)
duplicate = detector.check_hybrid_duplicate(
    url="https://bloomberg.com/news/articles/2026-01-07/tech-stocks-rally?ref=twitter",
    title="Tech Stocks Rally on Strong Earnings",
    title_similarity_threshold=0.85,
    time_window_hours=24
)

if duplicate:
    print(f"Duplicate type: {duplicate['duplicate_type']}")  # 'url' or 'title_similarity'
    print(f"Original article: {duplicate['news_id']}")
```

## Integration with RSS Collector

Update your `collector.py` to use the hybrid method:

```python
from deduplication import SimpleDuplicateDetector

# Initialize with database connection for title similarity
detector = SimpleDuplicateDetector(redis_client, db_conn)

for article in articles:
    # Use hybrid deduplication
    duplicate = detector.check_hybrid_duplicate(
        url=article['url'],
        title=article['title'],
        title_similarity_threshold=0.85,
        time_window_hours=24
    )

    if duplicate:
        logger.info(
            f"Skipping duplicate article (type: {duplicate['duplicate_type']}): "
            f"{article['title']}"
        )
        continue

    # Process new article...
    save_article(article)

    # Cache for future checks
    detector.cache_url(
        url=article['url'],
        article_data={
            'news_id': article['news_id'],
            'title': article['title'],
            'source': article['source'],
            'published_at': article['published_at']
        },
        ttl=86400,  # 24 hours
        use_normalized=True  # Cache normalized URL
    )
```

## Configuration Options

### Title Similarity Threshold

- `0.85` (default) - Recommended for general use
- `0.90` - More strict, catches only very similar titles
- `0.75` - More aggressive, may catch false positives

### Time Window

- `24` hours (default) - Check duplicates within last 24 hours
- `48` hours - Longer window for slower news cycles
- `12` hours - Shorter window for high-frequency sources

## Performance Considerations

1. **URL Check**: Very fast (Redis cache + simple SQL query)
2. **Title Similarity**: Slower (requires loading recent articles and fuzzy matching)
   - Limited to 500 most recent articles
   - Only runs if URL check fails
   - Consider caching results if needed

3. **Hybrid Method**: Best balance of accuracy and performance
   - Fast URL check eliminates most duplicates
   - Title similarity only runs when necessary

## Database Requirements

The Title Similarity check requires a database connection and uses this query:

```sql
SELECT id, news_id, title, url, source, published_at
FROM news_articles
WHERE published_at > NOW() - INTERVAL '24 hours'
AND (is_duplicate = FALSE OR is_duplicate IS NULL)
ORDER BY published_at DESC
LIMIT 500
```

**Recommended Index:**
```sql
CREATE INDEX idx_news_articles_published_dedup
ON news_articles(published_at DESC, is_duplicate)
WHERE is_duplicate = FALSE OR is_duplicate IS NULL;
```

## Example Output

```
INFO - Duplicate URL found in Redis: abc123def456
INFO - Similar title found: xyz789 (similarity: 87.5%)
INFO - Skipping duplicate article (type: title_similarity): Apple announces new iPhone 16
```

## Migration from Old Method

If you're currently using the old `check_duplicate_by_url` method:

**Old:**
```python
if detector.check_duplicate_by_url(url):
    continue  # Skip duplicate
```

**New (backward compatible):**
```python
duplicate = detector.check_duplicate_by_url(url, use_normalized=True)
if duplicate:
    continue  # Skip duplicate
```

**Recommended (hybrid):**
```python
duplicate = detector.check_hybrid_duplicate(url, title)
if duplicate:
    logger.info(f"Duplicate type: {duplicate['duplicate_type']}")
    continue
```

## Testing

See [SQL/news_articles/analize.ipynb](../../SQL/news_articles/analize.ipynb) for deduplication algorithm comparison tests.
