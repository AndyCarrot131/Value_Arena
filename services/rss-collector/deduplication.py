import boto3
import hashlib
import json
import logging
import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from difflib import SequenceMatcher
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)


class NewsDeduplicator:
    """News deduplicator - Uses AWS Comprehend for semantic analysis"""

    def __init__(self, redis_client, db_conn=None):
        """
        Initialize deduplicator

        Args:
            redis_client: Redis client instance
            db_conn: PostgreSQL database connection (optional, for persistent deduplication)
        """
        self.redis = redis_client
        self.db_conn = db_conn
        self.comprehend = boto3.client('comprehend', region_name='us-east-1')
        logger.info("News deduplicator initialized")
    
    def generate_fingerprint(self, article: Dict) -> Tuple[str, List[str], List[str]]:
        """
        Generate news fingerprint using AWS Comprehend
        
        Args:
            article: News article dict
        
        Returns:
            (fingerprint, entities, key_phrases) tuple
        """
        try:
            # 组合标题和内容前500字符
            text = f"{article.get('title', '')} {article.get('content', '')[:500]}"
            
            if len(text) < 10:
                logger.warning("Text too short, cannot generate fingerprint")
                return self._generate_simple_hash(article), [], []
            
            # 提取实体（人名、地名、组织等）
            try:
                entities_response = self.comprehend.detect_entities(
                    Text=text[:5000],  # Comprehend limit
                    LanguageCode='en'
                )
                entities = [
                    e['Text'].lower() 
                    for e in entities_response['Entities'] 
                    if e['Score'] > 0.8 and e['Type'] in ['ORGANIZATION', 'PERSON', 'LOCATION', 'COMMERCIAL_ITEM']
                ]
            except Exception as e:
                logger.warning(f"Entity extraction failed: {e}")
                entities = []
            
            # 提取关键短语
            try:
                phrases_response = self.comprehend.detect_key_phrases(
                    Text=text[:5000],
                    LanguageCode='en'
                )
                key_phrases = [
                    p['Text'].lower() 
                    for p in phrases_response['KeyPhrases']
                    if p['Score'] > 0.8
                ]
            except Exception as e:
                logger.warning(f"Key phrase extraction failed: {e}")
                key_phrases = []
            
            # 生成指纹（基于实体和关键短语）
            fingerprint_data = {
                'entities': sorted(entities[:10]),  # 取前10个最重要的实体
                'key_phrases': sorted(key_phrases[:10])  # 取前10个关键短语
            }
            
            fingerprint = hashlib.sha256(
                json.dumps(fingerprint_data, sort_keys=True).encode()
            ).hexdigest()
            
            logger.debug(f"Generated fingerprint: {fingerprint[:16]}... (entities:{len(entities)}, phrases:{len(key_phrases)})")
            
            return fingerprint, entities, key_phrases
            
        except Exception as e:
            logger.error(f"Comprehend analysis failed: {e}")
            # Fallback to simple hash
            return self._generate_simple_hash(article), [], []
    
    def _generate_simple_hash(self, article: Dict) -> str:
        """Fallback: simple content hash"""
        content = f"{article.get('title', '')}:{article.get('url', '')}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def check_duplicate(self, fingerprint: str, similarity_threshold: float = 0.8) -> Optional[Dict]:
        """
        Check if news is duplicate

        Args:
            fingerprint: News fingerprint
            similarity_threshold: Similarity threshold (not used yet, reserved for future)

        Returns:
            If duplicate found, returns original article info; else None
        """
        # First, check Redis cache (fast)
        try:
            cached_key = f"news:fingerprint:{fingerprint}"
            cached = self.redis.get(cached_key)

            if cached:
                article_data = json.loads(cached)
                logger.info(f"Duplicate news found in Redis: {article_data.get('news_id')}")
                return article_data

        except Exception as e:
            logger.error(f"Redis query failed: {e}")

        # If not in Redis, check database (persistent)
        if self.db_conn:
            try:
                cursor = self.db_conn.cursor()
                cursor.execute("""
                    SELECT id, news_id, title, url, source, published_at
                    FROM news_articles
                    WHERE content_hash = %s
                    AND (is_duplicate = FALSE OR is_duplicate IS NULL)
                    ORDER BY fetched_at ASC
                    LIMIT 1
                """, (fingerprint,))

                result = cursor.fetchone()
                cursor.close()

                if result:
                    article_data = {
                        'id': result[0],
                        'news_id': result[1],
                        'title': result[2],
                        'url': result[3],
                        'source': result[4],
                        'published_at': result[5].isoformat() if result[5] else None
                    }
                    logger.info(f"Duplicate news found in database: {article_data.get('news_id')}")

                    # Cache to Redis for future lookups
                    try:
                        self.redis.setex(
                            cached_key,
                            86400,  # 24 hours
                            json.dumps(article_data)
                        )
                    except Exception as redis_err:
                        logger.warning(f"Failed to cache to Redis: {redis_err}")

                    return article_data

            except Exception as e:
                logger.error(f"Database query failed: {e}")

        return None
    
    def cache_article(self, fingerprint: str, article_data: Dict, ttl: int = 86400):
        """
        Cache news fingerprint
        
        Args:
            fingerprint: News fingerprint
            article_data: Article data
            ttl: Cache time (seconds), default 24 hours
        """
        try:
            cache_data = {
                'news_id': article_data.get('news_id'),
                'title': article_data.get('title'),
                'url': article_data.get('url'),
                'source': article_data.get('source'),
                'published_at': article_data.get('published_at').isoformat() if article_data.get('published_at') else None,
                'cached_at': datetime.utcnow().isoformat()
            }
            
            self.redis.setex(
                f"news:fingerprint:{fingerprint}",
                ttl,
                json.dumps(cache_data)
            )
            
            logger.debug(f"Cached news fingerprint: {fingerprint[:16]}... TTL={ttl}s")
            
        except Exception as e:
            logger.error(f"Redis cache failed: {e}")
    
    def add_to_cluster(self, fingerprint: str, article_id: int):
        """
        Add article to similar news cluster
        
        Args:
            fingerprint: News fingerprint
            article_id: Article ID
        """
        try:
            cluster_key = f"news:cluster:{fingerprint[:16]}"
            self.redis.sadd(cluster_key, article_id)
            self.redis.expire(cluster_key, 86400)  # 24小时过期
            
        except Exception as e:
            logger.error(f"Failed to add to cluster: {e}")
    
    def get_cluster_articles(self, fingerprint: str) -> List[int]:
        """
        Get all articles in similar news cluster
        
        Args:
            fingerprint: News fingerprint
        
        Returns:
            List of article IDs
        """
        try:
            cluster_key = f"news:cluster:{fingerprint[:16]}"
            article_ids = self.redis.smembers(cluster_key)
            return [int(aid) for aid in article_ids]
            
        except Exception as e:
            logger.error(f"Failed to get cluster: {e}")
            return []


class SimpleDuplicateDetector:
    """
    Simple duplicate detector (does not use Comprehend, cost-saving)
    Based on exact match of URL and title, with advanced deduplication options
    """

    def __init__(self, redis_client=None, db_conn=None):
        self.redis = redis_client
        self.db_conn = db_conn
        if redis_client:
            logger.info("Simple duplicate detector initialized (with Redis)")
        else:
            logger.info("Simple duplicate detector initialized (database-only mode)")

    def normalize_url(self, url: str) -> str:
        """
        Normalize URL by removing query parameters and fragments

        Args:
            url: Original URL

        Returns:
            Normalized URL without query params
        """
        try:
            # Parse URL
            parsed = urlparse(url.lower().strip())

            # Reconstruct without query params and fragments
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                '',  # params
                '',  # query - REMOVED
                ''   # fragment - REMOVED
            ))

            return normalized

        except Exception as e:
            logger.warning(f"URL normalization failed for {url}: {e}")
            return url.lower().strip()

    def check_duplicate_by_url(self, url: str, use_normalized: bool = True) -> Optional[Dict]:
        """
        Check duplicate by URL (with optional normalization)

        Args:
            url: Article URL
            use_normalized: If True, removes query params before checking

        Returns:
            If duplicate found, returns cached article info; else None
        """
        try:
            # Normalize URL if requested
            check_url = self.normalize_url(url) if use_normalized else url

            # Check Redis cache (if Redis is available)
            if self.redis:
                url_hash = hashlib.md5(check_url.encode()).hexdigest()
                cached_key = f"news:url:{url_hash}"
                cached = self.redis.get(cached_key)

                if cached:
                    try:
                        article_data = json.loads(cached)
                        logger.info(f"Duplicate URL found in Redis: {article_data.get('news_id')}")
                        return article_data
                    except json.JSONDecodeError:
                        # Old format (just article_id), query database
                        if self.db_conn:
                            return self._get_article_from_db(int(cached))
                        return {'news_id': cached}

            # Check database by URL (always check if db_conn exists)
            if self.db_conn:
                return self._check_url_in_db(check_url)

        except Exception as e:
            logger.error(f"URL duplicate check failed: {e}")

        return None

    def _check_url_in_db(self, normalized_url: str) -> Optional[Dict]:
        """Check if normalized URL exists in database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT id, news_id, title, url, source, published_at
                FROM news_articles
                WHERE LOWER(REGEXP_REPLACE(url, '\?.*$', '')) = %s
                AND (is_duplicate = FALSE OR is_duplicate IS NULL)
                ORDER BY fetched_at ASC
                LIMIT 1
            """, (normalized_url,))

            result = cursor.fetchone()
            cursor.close()

            if result:
                article_data = {
                    'id': result[0],
                    'news_id': result[1],
                    'title': result[2],
                    'url': result[3],
                    'source': result[4],
                    'published_at': result[5].isoformat() if result[5] else None
                }
                logger.info(f"Duplicate URL found in database: {article_data.get('news_id')}")
                return article_data

        except Exception as e:
            logger.error(f"Database URL check failed: {e}")

        return None

    def _get_article_from_db(self, article_id: int) -> Optional[Dict]:
        """Get article info from database by ID"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT id, news_id, title, url, source, published_at
                FROM news_articles
                WHERE id = %s
            """, (article_id,))

            result = cursor.fetchone()
            cursor.close()

            if result:
                return {
                    'id': result[0],
                    'news_id': result[1],
                    'title': result[2],
                    'url': result[3],
                    'source': result[4],
                    'published_at': result[5].isoformat() if result[5] else None
                }
        except Exception as e:
            logger.error(f"Database article fetch failed: {e}")

        return None

    def check_duplicate_by_title_similarity(
        self,
        title: str,
        threshold: float = 0.85,
        time_window_hours: int = 24
    ) -> Optional[Dict]:
        """
        Check for duplicate by title similarity using fuzzy matching

        Args:
            title: Article title
            threshold: Similarity threshold (0.0-1.0), default 0.85
            time_window_hours: Only check articles within this time window

        Returns:
            If similar title found, returns original article info; else None
        """
        if not self.db_conn:
            logger.warning("Title similarity check requires database connection")
            return None

        try:
            title_clean = title.lower().strip()

            # Get recent articles from database
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT id, news_id, title, url, source, published_at
                FROM news_articles
                WHERE published_at > NOW() - INTERVAL '%s hours'
                AND (is_duplicate = FALSE OR is_duplicate IS NULL)
                ORDER BY published_at DESC
                LIMIT 500
            """, (time_window_hours,))

            results = cursor.fetchall()
            cursor.close()

            # Check similarity with each article
            for row in results:
                existing_title = row[2].lower().strip()
                similarity = SequenceMatcher(None, title_clean, existing_title).ratio()

                if similarity >= threshold:
                    article_data = {
                        'id': row[0],
                        'news_id': row[1],
                        'title': row[2],
                        'url': row[3],
                        'source': row[4],
                        'published_at': row[5].isoformat() if row[5] else None,
                        'similarity_score': similarity
                    }
                    logger.info(
                        f"Similar title found: {article_data.get('news_id')} "
                        f"(similarity: {similarity:.2%})"
                    )
                    return article_data

        except Exception as e:
            logger.error(f"Title similarity check failed: {e}")

        return None

    def check_hybrid_duplicate(
        self,
        url: str,
        title: str,
        title_similarity_threshold: float = 0.85,
        time_window_hours: int = 24
    ) -> Optional[Dict]:
        """
        Hybrid duplicate detection: URL first, then Title Similarity

        Args:
            url: Article URL
            title: Article title
            title_similarity_threshold: Threshold for title matching
            time_window_hours: Time window for title similarity check

        Returns:
            If duplicate found, returns original article info with 'duplicate_type'; else None
        """
        # Stage 1: Check URL (normalized)
        url_duplicate = self.check_duplicate_by_url(url, use_normalized=True)
        if url_duplicate:
            url_duplicate['duplicate_type'] = 'url'
            return url_duplicate

        # Stage 2: Check Title Similarity
        title_duplicate = self.check_duplicate_by_title_similarity(
            title,
            threshold=title_similarity_threshold,
            time_window_hours=time_window_hours
        )
        if title_duplicate:
            title_duplicate['duplicate_type'] = 'title_similarity'
            return title_duplicate

        return None

    def cache_url(self, url: str, article_data: Dict, ttl: int = 86400, use_normalized: bool = True):
        """
        Cache URL (with optional normalization)

        Args:
            url: Article URL
            article_data: Article metadata to cache
            ttl: Time to live in seconds (default 24 hours)
            use_normalized: If True, caches normalized URL without query params
        """
        # Skip if Redis is not available
        if not self.redis:
            return

        try:
            check_url = self.normalize_url(url) if use_normalized else url
            url_hash = hashlib.md5(check_url.encode()).hexdigest()

            cache_data = {
                'news_id': article_data.get('news_id'),
                'title': article_data.get('title'),
                'url': url,  # Store original URL
                'source': article_data.get('source'),
                'published_at': article_data.get('published_at').isoformat()
                    if article_data.get('published_at') else None,
                'cached_at': datetime.utcnow().isoformat()
            }

            self.redis.setex(
                f"news:url:{url_hash}",
                ttl,
                json.dumps(cache_data)
            )

            logger.debug(f"Cached URL: {check_url[:60]}... TTL={ttl}s")

        except Exception as e:
            logger.error(f"URL cache failed: {e}")