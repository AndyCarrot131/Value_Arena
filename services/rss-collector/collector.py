import feedparser
import boto3
import psycopg2
import json
import hashlib
import logging
import subprocess
import sys
from datetime import datetime
from typing import List, Dict
import os

from deduplication import SimpleDuplicateDetector

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RSSCollector:
    def __init__(self):
        """Initialize RSS Collector with database-only deduplication"""
        self.load_config()
        self.setup_aws_clients()
        self.setup_database()
        self.setup_deduplicator()
    
    def load_config(self):
        """Load configuration from Secrets Manager"""
        try:
            secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
            response = secrets_client.get_secret_value(
                SecretId='ai-stock-war/database-config'
            )
            self.config = json.loads(response['SecretString'])
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def setup_aws_clients(self):
        """Initialize AWS clients"""
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.info("AWS clients initialized")
    
    def setup_database(self):
        """Connect to PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                host=self.config['db_host'],
                port=self.config['db_port'],
                database=self.config['db_name'],
                user=self.config['db_user'],
                password=self.config['db_password']
            )
            logger.info("Database connection successful")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def setup_deduplicator(self):
        """Initialize deduplicator (database-only, no Redis)"""
        # SimpleDuplicateDetector with database connection (no Redis)
        self.deduplicator = SimpleDuplicateDetector(redis_client=None, db_conn=self.db_conn)
        logger.info("Using database-only deduplication (URL + Title Similarity)")
    
    def get_rss_sources(self) -> List[Dict]:
        """从数据库获取RSS源列表"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT id, name, url, category, priority 
            FROM rss_sources 
            WHERE enabled = TRUE
            ORDER BY priority, id
        """)
        sources = []
        for row in cursor.fetchall():
            sources.append({
                'id': row[0],
                'name': row[1],
                'url': row[2],
                'category': row[3],
                'priority': row[4]
            })
        cursor.close()
        return sources
    
    def generate_news_id(self, url: str, title: str) -> str:
        """生成新闻唯一ID"""
        content = f"{url}:{title}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def fetch_feed(self, source: Dict) -> List[Dict]:
        """Fetch a single RSS feed"""
        try:
            logger.info(f"Start fetching: {source['name']} ({source['url']})")
            feed = feedparser.parse(source['url'])
            
            articles = []
            for entry in feed.entries[:20]:  # Limit to 20 entries per fetch
                news_id = self.generate_news_id(
                    entry.get('link', ''),
                    entry.get('title', '')
                )
                
                # Parse publish time
                published_at = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    published_at = datetime(*entry.published_parsed[:6])
                
                article = {
                    'news_id': news_id,
                    'title': entry.get('title', ''),
                    'content': entry.get('summary', ''),
                    'source': source['name'],
                    'url': entry.get('link', ''),
                    'published_at': published_at,
                    'category': source['category'],
                    'source_id': source['id']
                }
                articles.append(article)
            
            logger.info(f"Fetched {len(articles)} articles: {source['name']}")
            return articles
            
        except Exception as e:
            logger.error(f"Fetch failed {source['name']}: {e}")
            return []
    
    def process_article(self, article: Dict) -> Dict:
        """
        Process single article (deduplication check against database)
        Only checks articles from the last 3 days

        Returns:
            Result dict with is_duplicate, primary_article_id, etc.
        """
        result = {
            'is_duplicate': False,
            'primary_article_id': None,
            'fingerprint': None,
            'entities': [],
            'key_phrases': []
        }

        # Hybrid deduplication check (URL + Title Similarity)
        # Only checks against articles published in the last 3 days (72 hours)
        if isinstance(self.deduplicator, SimpleDuplicateDetector):
            duplicate = self.deduplicator.check_hybrid_duplicate(
                url=article['url'],
                title=article['title'],
                title_similarity_threshold=0.85,
                time_window_hours=72  # 3 days
            )

            if duplicate:
                result['is_duplicate'] = True
                result['primary_article_id'] = duplicate.get('id')
                duplicate_type = duplicate.get('duplicate_type', 'unknown')
                logger.info(
                    f"Duplicate ({duplicate_type}): {article['title'][:50]}... "
                    f"-> Original ID: {duplicate.get('id')}"
                )
                return result

        return result
    
    def save_to_database(self, article: Dict, dedup_result: Dict):
        """Save to PostgreSQL"""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO news_articles
                (news_id, title, content, source, url, published_at, category,
                 classification, is_duplicate, primary_article_id, content_hash,
                 comprehend_entities, comprehend_key_phrases)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (news_id) DO NOTHING
                RETURNING id
            """, (
                article['news_id'],
                article['title'],
                article['content'],
                article['source'],
                article['url'],
                article['published_at'],
                article['category'],
                'pending',  # 等待分类
                dedup_result['is_duplicate'],
                dedup_result.get('primary_article_id'),
                dedup_result.get('fingerprint'),
                json.dumps(dedup_result.get('entities', [])),
                json.dumps(dedup_result.get('key_phrases', []))
            ))
            
            inserted = cursor.fetchone()
            self.db_conn.commit()
            
            if inserted:
                article_id = inserted[0]
                logger.info(f"Saved successfully: {article_id} - {article['title'][:50]}...")
                return article_id
            else:
                logger.debug(f"Already exists: {article['news_id']}")
                return None
            
        except Exception as e:
            logger.error(f"Database save failed: {e}")
            self.db_conn.rollback()
            return None
        finally:
            cursor.close()
    
    def cache_article_fingerprint(self, article: Dict, dedup_result: Dict):
        """Cache article fingerprint (disabled - no Redis)"""
        # Redis caching disabled
        pass
    
    def save_to_s3(self, article: Dict):
        """Save raw data to S3"""
        try:
            key = f"raw/{datetime.now().strftime('%Y/%m/%d')}/{article['news_id']}.json"
            self.s3_client.put_object(
                Bucket=self.config['s3_rss_bucket'],
                Key=key,
                Body=json.dumps(article, default=str, ensure_ascii=False),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Failed to save to S3: {e}")
    
    def update_fetch_timestamp(self, source_id: int):
        """Update last fetch timestamp for RSS source"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            UPDATE rss_sources 
            SET last_fetched = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (source_id,))
        self.db_conn.commit()
        cursor.close()
    
    def run_deduplication_cleanup(self):
        """
        Run full database deduplication and delete duplicates
        Calls deduplicate_existing_data.py to scan entire DB and delete duplicates
        """
        logger.info("\n" + "=" * 80)
        logger.info("Running post-collection deduplication cleanup")
        logger.info("=" * 80)

        try:
            # Import and run the deduplication function
            from deduplicate_existing_data import deduplicate_existing_data, load_config

            # Run all deduplication methods
            deduplicate_existing_data(
                methods=['content_hash', 'url', 'title_similarity'],
                title_threshold=0.85
            )

            # Delete duplicates from database
            logger.info("\nDeleting duplicate articles from database...")
            cursor = self.db_conn.cursor()

            # Count before deletion
            cursor.execute("SELECT COUNT(*) FROM news_articles WHERE is_duplicate = TRUE")
            duplicate_count = cursor.fetchone()[0]

            if duplicate_count > 0:
                # Delete duplicates
                cursor.execute("DELETE FROM news_articles WHERE is_duplicate = TRUE")
                self.db_conn.commit()
                logger.info(f"✓ Deleted {duplicate_count} duplicate articles")
            else:
                logger.info("✓ No duplicates to delete")

            cursor.close()

        except Exception as e:
            logger.error(f"Deduplication cleanup failed: {e}")
            import traceback
            traceback.print_exc()

    def collect_all(self):
        """Perform a full collection run"""
        logger.info("=" * 50)
        logger.info("Starting RSS collection task")

        sources = self.get_rss_sources()
        logger.info(f"Found {len(sources)} RSS sources")

        stats = {
            'total': 0,
            'new': 0,
            'duplicate': 0,
            'failed': 0
        }

        for source in sources:
            articles = self.fetch_feed(source)

            for article in articles:
                stats['total'] += 1

                try:
                    # Deduplication check (against last 3 days in database)
                    dedup_result = self.process_article(article)

                    if dedup_result['is_duplicate']:
                        stats['duplicate'] += 1
                        continue

                    # Save to database (only non-duplicates)
                    article_id = self.save_to_database(article, dedup_result)

                    if article_id:
                        stats['new'] += 1
                        # Save to S3
                        self.save_to_s3(article)

                except Exception as e:
                    stats['failed'] += 1
                    logger.error(f"Processing failed: {e}")

            if articles:
                self.update_fetch_timestamp(source['id'])

        logger.info(f"Collection completed - Total:{stats['total']} New:{stats['new']} Duplicate:{stats['duplicate']} Failed:{stats['failed']}")
        logger.info("=" * 50)

        # Run full database deduplication and delete duplicates
        self.run_deduplication_cleanup()
    
    def close(self):
        """Close connections"""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()


if __name__ == "__main__":
    # Database-only mode (no Redis)
    collector = RSSCollector()
    try:
        collector.collect_all()
    finally:
        collector.close()