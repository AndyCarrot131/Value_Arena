import boto3
import psycopg2
import json
import logging
from typing import List, Dict, Optional
from datetime import datetime
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline

logger = logging.getLogger(__name__)


class NewsClassifier:
    """
    News Classifier with FinBERT
    - Uses FinBERT for sentiment analysis
    - Matches news with relevant stocks
    - Classifies news into: direct, indirect, macroeconomic, irrelevant
    """

    def __init__(self):
        self.load_config()
        self.setup_database()
        self.setup_aws_clients()
        self.setup_finbert()
        self.load_stock_info()
    
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
    
    def setup_aws_clients(self):
        """Initialize AWS clients"""
        self.comprehend = boto3.client('comprehend', region_name='us-east-1')
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        logger.info("AWS clients initialized")

    def setup_finbert(self):
        """Initialize FinBERT model for sentiment analysis"""
        try:
            logger.info("Loading FinBERT model...")

            # Check if GPU is available
            self.device = 0 if torch.cuda.is_available() else -1
            device_name = "GPU" if self.device == 0 else "CPU"
            logger.info(f"Using device: {device_name}")

            # Load FinBERT model and tokenizer
            model_name = "ProsusAI/finbert"
            self.tokenizer = AutoTokenizer.from_pretrained(model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(model_name)

            # Create sentiment analysis pipeline
            self.sentiment_analyzer = pipeline(
                "sentiment-analysis",
                model=self.model,
                tokenizer=self.tokenizer,
                device=self.device
            )

            logger.info("FinBERT model loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load FinBERT model: {e}")
            raise
    
    def load_stock_info(self):
        """Load stock information from database"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT symbol, name, sector, industry 
            FROM stocks 
            WHERE enabled = TRUE
        """)
        
        self.stocks = {}
        for row in cursor.fetchall():
            symbol = row[0]
            self.stocks[symbol] = {
                'name': row[1],
                'sector': row[2],
                'industry': row[3],
                'keywords': [symbol, row[1].lower()]  # Stock symbol and company name
            }
        
        cursor.close()
        logger.info(f"Loaded {len(self.stocks)} stocks information")
    
    def get_pending_articles(self, limit: int = 100) -> List[Dict]:
        """Get pending articles for classification"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT id, news_id, title, content, source, url, 
                   published_at, category, comprehend_entities, comprehend_key_phrases
            FROM news_articles 
            WHERE classification = 'pending' 
              AND is_duplicate = FALSE
            ORDER BY published_at DESC
            LIMIT %s
        """, (limit,))
        
        articles = []
        for row in cursor.fetchall():
            articles.append({
                'id': row[0],
                'news_id': row[1],
                'title': row[2],
                'content': row[3],
                'source': row[4],
                'url': row[5],
                'published_at': row[6],
                'category': row[7],
                'entities': row[8] if row[8] else [],
                'key_phrases': row[9] if row[9] else []
            })
        
        cursor.close()
        logger.info(f"Found {len(articles)} pending articles for classification")
        return articles
    
    def match_stocks(self, text: str) -> List[str]:
        """
        Match stocks mentioned in the text

        Args:
            text: News content to analyze

        Returns:
            List of matched stock symbols
        """
        text_lower = text.lower()
        matched_stocks = []

        for symbol, info in self.stocks.items():
            # Check if stock symbol or company name is mentioned
            for keyword in info['keywords']:
                if keyword.lower() in text_lower:
                    matched_stocks.append(symbol)
                    break

        return matched_stocks

    def analyze_sentiment(self, text: str) -> Dict:
        """
        Analyze sentiment using FinBERT

        Args:
            text: Text to analyze (max 512 tokens)

        Returns:
            {
                'sentiment': 'positive'/'negative'/'neutral',
                'score': 0.95
            }
        """
        try:
            # FinBERT works best with first 512 tokens
            truncated_text = text[:512]

            result = self.sentiment_analyzer(truncated_text)[0]

            return {
                'sentiment': result['label'].lower(),
                'score': result['score']
            }
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            return {
                'sentiment': 'neutral',
                'score': 0.0
            }

    def classify_article(self, article: Dict) -> Dict:
        """
        Classify a single article using FinBERT

        Returns:
            {
                'classification': 'direct'/'indirect'/'macro'/'irrelevant',
                'related_stocks': ['AAPL', 'GOOGL'],
                'sentiment': 'positive'/'negative'/'neutral',
                'sentiment_score': 0.95,
                'confidence': 0.85
            }
        """
        text = f"{article['title']} {article['content']}"
        text_lower = text.lower()

        # Step 1: Match stocks in the content
        matched_stocks = self.match_stocks(text)

        # Step 2: Analyze sentiment only if stocks are matched
        sentiment_result = {'sentiment': 'neutral', 'score': 0.0}

        if matched_stocks:
            sentiment_result = self.analyze_sentiment(text[:512])

            return {
                'classification': 'direct',
                'related_stocks': matched_stocks,
                'sentiment': sentiment_result['sentiment'],
                'sentiment_score': sentiment_result['score'],
                'confidence': 0.9
            }

        # Step 3: Check for indirect relevance (industry/sector keywords)
        entities = article.get('entities', [])
        key_phrases = article.get('key_phrases', [])

        # Extract entities and key phrases if not available
        if not entities or not key_phrases:
            try:
                entities_response = self.comprehend.detect_entities(
                    Text=text[:5000],
                    LanguageCode='en'
                )
                entities = [e['Text'].lower() for e in entities_response['Entities']]

                phrases_response = self.comprehend.detect_key_phrases(
                    Text=text[:5000],
                    LanguageCode='en'
                )
                key_phrases = [p['Text'].lower() for p in phrases_response['KeyPhrases']]

            except Exception as e:
                logger.error(f"Comprehend analysis failed: {e}")
                entities = []
                key_phrases = []

        all_keywords = entities + key_phrases + [text_lower]

        # Check indirect relevance (same industry/sector)
        indirect_matches = []
        for symbol, info in self.stocks.items():
            sector_keywords = info['sector'].lower().split() if info['sector'] else []
            industry_keywords = info['industry'].lower().split() if info['industry'] else []

            for text_item in all_keywords:
                if any(kw in str(text_item) for kw in sector_keywords + industry_keywords):
                    indirect_matches.append(symbol)
                    break

        if indirect_matches:
            sentiment_result = self.analyze_sentiment(text[:512])

            return {
                'classification': 'indirect',
                'related_stocks': indirect_matches,
                'sentiment': sentiment_result['sentiment'],
                'sentiment_score': sentiment_result['score'],
                'confidence': 0.7
            }

        # Step 4: Check macroeconomic relevance
        macro_keywords = [
            'economy', 'gdp', 'inflation', 'federal reserve', 'interest rate',
            'unemployment', 'recession', 'economic growth', 'monetary policy',
            'fiscal policy', 'trade war', 'tariff', 'market', 'stock market'
        ]

        if any(kw in text_lower for kw in macro_keywords):
            return {
                'classification': 'macro',
                'related_stocks': [],
                'sentiment': 'neutral',
                'sentiment_score': 0.0,
                'confidence': 0.6
            }

        # Step 5: Irrelevant
        return {
            'classification': 'irrelevant',
            'related_stocks': [],
            'sentiment': 'neutral',
            'sentiment_score': 0.0,
            'confidence': 0.5
        }
    
    def update_classification(self, article_id: int, classification_result: Dict):
        """Update classification result in the database or delete if irrelevant"""
        cursor = self.db_conn.cursor()
        try:
            # Delete irrelevant articles from database
            if classification_result['classification'] == 'irrelevant':
                cursor.execute("""
                    DELETE FROM news_articles
                    WHERE id = %s
                """, (article_id,))
                logger.info(f"Deleted irrelevant article ID: {article_id}")
            else:
                # Check if sentiment columns exist, if not, skip sentiment update
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'news_articles'
                    AND column_name IN ('sentiment', 'sentiment_score')
                """)
                has_sentiment_columns = len(cursor.fetchall()) == 2

                if has_sentiment_columns:
                    cursor.execute("""
                        UPDATE news_articles
                        SET classification = %s,
                            related_stocks = %s,
                            sentiment = %s,
                            sentiment_score = %s
                        WHERE id = %s
                    """, (
                        classification_result['classification'],
                        classification_result['related_stocks'],
                        classification_result.get('sentiment', 'neutral'),
                        classification_result.get('sentiment_score', 0.0),
                        article_id
                    ))
                else:
                    # Fallback to old schema without sentiment
                    cursor.execute("""
                        UPDATE news_articles
                        SET classification = %s,
                            related_stocks = %s
                        WHERE id = %s
                    """, (
                        classification_result['classification'],
                        classification_result['related_stocks'],
                        article_id
                    ))

            self.db_conn.commit()

        except Exception as e:
            logger.error(f"Failed to update classification: {e}")
            self.db_conn.rollback()
        finally:
            cursor.close()
    
    def save_classified_to_s3(self, article: Dict, classification_result: Dict):
        """Save classified news to corresponding S3 directory"""
        try:
            classification = classification_result['classification']
            key = f"classified/{classification}/{datetime.now().strftime('%Y/%m/%d')}/{article['news_id']}.json"

            data = {
                'news_id': article['news_id'],
                'title': article['title'],
                'content': article['content'],
                'source': article['source'],
                'url': article['url'],
                'published_at': article['published_at'].isoformat() if article['published_at'] else None,
                'classification': classification,
                'related_stocks': classification_result.get('related_stocks', []),
                'sentiment': classification_result.get('sentiment', 'neutral'),
                'sentiment_score': classification_result.get('sentiment_score', 0.0),
                'confidence': classification_result.get('confidence', 0.0),
                'classified_at': datetime.utcnow().isoformat()
            }

            self.s3_client.put_object(
                Bucket=self.config['s3_rss_bucket'],
                Key=key,
                Body=json.dumps(data, ensure_ascii=False),
                ContentType='application/json'
            )

        except Exception as e:
            logger.error(f"Failed to save to S3: {e}")
    
    def classify_batch(self):
        """Batch classify news articles"""
        logger.info("=" * 50)
        logger.info("Starting news classification task")
        
        articles = self.get_pending_articles(limit=100)
        
        if not articles:
            logger.info("No pending articles for classification")
            return
        
        stats = {
            'direct': 0,
            'indirect': 0,
            'macro': 0,
            'irrelevant': 0
        }
        
        for article in articles:
            try:
                result = self.classify_article(article)
                classification = result['classification']
                
                # Update database
                self.update_classification(article['id'], result)

                # Save to S3
                self.save_classified_to_s3(article, result)
                
                stats[classification] += 1
                
                sentiment_info = f"[{result.get('sentiment', 'N/A')}:{result.get('sentiment_score', 0.0):.2f}]" if result.get('sentiment') else ""
                stocks_info = f"[{', '.join(result['related_stocks'][:3])}]" if result['related_stocks'] else ""
                logger.info(f"Classified: {classification} {sentiment_info} {stocks_info} - {article['title'][:50]}...")
                
            except Exception as e:
                logger.error(f"Classification failed: {e}")
        
        logger.info(f"Classification completed - Direct: {stats['direct']} Indirect: {stats['indirect']} Macro: {stats['macro']} Irrelevant: {stats['irrelevant']}")
        logger.info("=" * 50)
    
    def close(self):
        """Close connections"""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    classifier = NewsClassifier()
    try:
        classifier.classify_batch()
    finally:
        classifier.close()