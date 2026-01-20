import sys
import logging
from unittest.mock import Mock, patch, MagicMock

logging.basicConfig(level=logging.INFO)

# Mock AWS services
mock_boto3 = MagicMock()
mock_secrets = MagicMock()
mock_secrets.get_secret_value.return_value = {
    'SecretString': '{"db_host": "localhost", "db_port": "5432", "db_name": "test_db", "db_user": "test", "db_password": "test", "redis_host": "localhost", "redis_port": "6379", "redis_ssl": false, "s3_rss_bucket": "test-bucket"}'
}
mock_boto3.client.return_value = mock_secrets

sys.modules['boto3'] = mock_boto3
sys.modules['psycopg2'] = MagicMock()
sys.modules['redis'] = MagicMock()

# Now import your module
from collector import RSSCollector

def test_basic_functionality():
    """Test basic collector functionality"""
    try:
        collector = RSSCollector(use_comprehend=False)
        print("✓ Collector initialization successful")
        
        # Test RSS parsing logic
        article = {
            'news_id': 'test123',
            'title': 'Apple reports record earnings',
            'content': 'Apple Inc. announced...',
            'url': 'https://example.com/news',
            'source': 'Test Source',
            'category': 'tech',
            'published_at': None
        }
        
        result = collector.process_article(article)
        print(f"✓ Article processing successful: {result}")
        
        print("\n✓ All tests passed!")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_basic_functionality()