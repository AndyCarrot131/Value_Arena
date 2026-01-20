"""
Data Collector Service
Collect news, earnings, stock prices, and related data
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from core import DatabaseManager, RedisClient, create_context_logger
from utils import get_et_now, ET_OFFSET

logger = create_context_logger()


class DataCollector:
    """Data collection service"""
    
    def __init__(self, db: DatabaseManager, redis_client: RedisClient):
        """
        Initialize the data collector

        Args:
            db: database manager
            redis_client: Redis client
        """
        self.db = db
        self.redis = redis_client

    def fetch_all(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute query and fetch all results

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of result dictionaries
        """
        try:
            results = self.db.execute_query(query, params, fetch=True)
            return results or []
        except Exception as e:
            logger.error(f"Failed to fetch all: {e}")
            return []

    def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        """
        Execute query and fetch one result

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Single result dictionary, or None if no results
        """
        try:
            results = self.db.execute_query(query, params, fetch=True)
            if results and len(results) > 0:
                return results[0]
            return None
        except Exception as e:
            logger.error(f"Failed to fetch one: {e}")
            return None

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """
        Execute a non-SELECT query (INSERT/UPDATE/DELETE)

        Args:
            query: SQL query string
            params: Query parameters
        """
        try:
            self.db.execute_update(query, params)
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def collect_news(
        self,
        hours: int = 1,
        symbols: Optional[List[str]] = None,
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Collect news data

        Args:
            hours: Collect news from last N hours
            symbols: Filter by stock symbols (optional)
            categories: Filter by news categories (optional)

        Returns:
            List of news articles [{'news_id': str, 'title': str, 'content': str, ...}, ...]
        """
        logger.info(f"Collecting news from last {hours} hour(s)")

        # Build query
        query = """
            SELECT
                news_id,
                title,
                content,
                source,
                url,
                published_at,
                category,
                related_stocks,
                classification
            FROM news_articles
            WHERE published_at > NOW() - INTERVAL '%s hours'
              AND is_duplicate = FALSE
              AND classification != 'irrelevant'
        """
        params = [hours]

        # Add stock symbol filter
        if symbols:
            query += " AND related_stocks && %s"
            params.append(symbols)

        # Add category filter
        if categories:
            query += " AND category = ANY(%s)"
            params.append(categories)
        
        query += " ORDER BY published_at DESC"
        
        try:
            results = self.db.execute_query(query, tuple(params))
            logger.info(f"Collected {len(results)} news articles")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to collect news: {e}")
            return []
    
    def collect_financial_reports(
        self,
        symbol: str,
        report_type: Optional[str] = None,
        limit: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Collect financial report data
        
        Args:
            symbol: stock symbol
            report_type: report type (10-K/10-Q, optional)
            limit: number of records to return
            
        Returns:
            List of financial reports [{'symbol': str, 'summary_en': str, ...}, ...]
        """
        logger.info(f"Collecting financial reports for {symbol}")
        
        query = """
            SELECT 
                symbol,
                report_type,
                fiscal_year,
                fiscal_quarter,
                filing_date,
                summary_en,
                extraction_status
            FROM financial_reports
            WHERE symbol = %s
              AND extraction_status = 'completed'
        """
        params = [symbol]
        
        if report_type:
            query += " AND report_type = %s"
            params.append(report_type)
        
        query += " ORDER BY filing_date DESC LIMIT %s"
        params.append(limit)
        
        try:
            results = self.db.execute_query(query, tuple(params))
            logger.info(f"Collected {len(results)} financial reports")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to collect financial reports: {e}")
            return []
    
    def collect_stock_prices(
        self,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """
        Collect real-time stock prices
        
        Args:
            symbols: list of stock symbols (optional, None means all)
        
        Returns:
            {symbol: price} dictionary
        """
        logger.info(f"Collecting stock prices for {len(symbols) if symbols else 'all'} symbols")
        
        try:
            if symbols:
                prices = {}
                for symbol in symbols:
                    price = self.redis.get_stock_price(symbol)
                    if price is not None:
                        prices[symbol] = price
                    else:
                        logger.warning(f"Price not found for {symbol}")
            else:
                prices = self.redis.get_all_stock_prices()
            
            logger.info(f"Collected prices for {len(prices)} stocks")
            return prices
        
        except Exception as e:
            logger.error(f"Failed to collect stock prices: {e}")
            return {}
    
    def get_stock_list(
        self,
        enabled_only: bool = True,
        stock_type: str = 'stock'
    ) -> List[Dict[str, Any]]:
        """
        Get stock list
        
        Args:
            enabled_only: return only enabled stocks
            stock_type: stock type (stock/index/etf)
        
        Returns:
            Stock list [{'symbol': str, 'name': str, 'sector': str, ...}, ...]
        """
        logger.info("Fetching stock list")
        
        query = """
            SELECT 
                symbol,
                name,
                sector,
                industry,
                type,
                enabled
            FROM stocks
            WHERE type = %s
        """
        params = [stock_type]
        
        if enabled_only:
            query += " AND enabled = TRUE"
        
        query += " ORDER BY symbol"
        
        try:
            results = self.db.execute_query(query, tuple(params))
            logger.info(f"Found {len(results)} stocks")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to get stock list: {e}")
            return []
    
    def get_positions(self, agent_id: str) -> List[Dict[str, Any]]:
        """
        Get current positions for the AI
        
        Args:
            agent_id: AI ID
            
        Returns:
            Positions list [{'symbol': str, 'quantity': int, 'position_type': str, ...}, ...]
        """
        logger.info(f"Fetching positions for {agent_id}")
        
        query = """
            SELECT 
                symbol,
                quantity,
                average_cost,
                current_value,
                unrealized_pnl,
                position_type,
                first_buy_date,
                updated_at
            FROM positions
            WHERE agent_id = %s
              AND quantity > 0
            ORDER BY symbol
        """
        
        try:
            results = self.db.execute_query(query, (agent_id,))
            logger.info(f"Found {len(results)} positions")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return []
    
    def get_recent_transactions(
        self,
        agent_id: str,
        days: int = 7,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get recent transactions
        
        Args:
            agent_id: AI ID
            days: last N days
            limit: number of records to return
            
        Returns:
            List of transactions
        """
        logger.info(f"Fetching recent transactions for {agent_id}")
        
        query = """
            SELECT 
                symbol,
                action,
                quantity,
                price,
                total_amount,
                reason,
                position_type,
                executed_at
            FROM transactions
            WHERE agent_id = %s
              AND executed_at > NOW() - INTERVAL '%s days'
            ORDER BY executed_at DESC
            LIMIT %s
        """
        
        try:
            results = self.db.execute_query(query, (agent_id, days, limit))
            logger.info(f"Found {len(results)} transactions")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to get transactions: {e}")
            return []
    
    def get_daily_reviews(
        self,
        agent_id: str,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Get historical daily summaries
        
        Args:
            agent_id: AI ID
            days: last N days
        
        Returns:
            List of daily summaries
        """
        logger.info(f"Fetching daily reviews for {agent_id}")
        
        query = """
            SELECT 
                review_date,
                portfolio_value,
                daily_pnl,
                total_pnl,
                transactions_count,
                review_content
            FROM daily_reviews
            WHERE agent_id = %s
              AND review_date > CURRENT_DATE - INTERVAL '%s days'
            ORDER BY review_date DESC
        """
        
        try:
            results = self.db.execute_query(query, (agent_id, days))
            logger.info(f"Found {len(results)} daily reviews")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to get daily reviews: {e}")
            return []
    
    def get_hourly_news_analysis(
        self,
        agent_id: str,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get hourly news analysis

        Args:
            agent_id: AI ID
            hours: last N hours

        Returns:
            List of news analyses
        """
        logger.info(f"Fetching hourly news analysis for {agent_id}")

        query = """
            SELECT
                news_id,
                analysis,
                sentiment,
                mentioned_stocks,
                impact_prediction,
                confidence_score,
                created_at
            FROM hourly_news_analysis
            WHERE agent_id = %s
              AND created_at > NOW() - INTERVAL '%s hours'
            ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_query(query, (agent_id, hours))
            logger.info(f"Found {len(results)} news analysis records")
            return results or []

        except Exception as e:
            logger.error(f"Failed to get hourly news analysis: {e}")
            return []

    def collect_unanalyzed_news(
        self,
        agent_id: str,
        hours: float = 4.0
    ) -> List[Dict[str, Any]]:
        """
        Collect unanalyzed news (for the hourly news analysis workflow)

        Filter criteria:
        1. Within the last N hours (based on fetched_at)
        2. The current agent has not analyzed it (not in hourly_news_analysis table)
        3. classification is not 'irrelevant'
        4. Not a duplicate news item

        Args:
            agent_id: AI ID
            hours: time window (hours), default 4.0 to allow retries for failed analyses

        Returns:
            List of unanalyzed news
        """
        logger.info(f"Collecting unanalyzed news for {agent_id} from last {hours} hours")

        query = """
            SELECT
                n.news_id,
                n.title,
                n.content,
                n.source,
                n.url,
                n.published_at,
                n.fetched_at,
                n.category,
                n.related_stocks,
                n.classification,
                n.sentiment,
                n.sentiment_score
            FROM news_articles n
            WHERE n.fetched_at > NOW() - INTERVAL '%s hours'
              AND n.is_duplicate = FALSE
              AND n.classification != 'irrelevant'
              AND NOT EXISTS (
                  SELECT 1
                  FROM hourly_news_analysis h
                  WHERE h.news_id = n.news_id
                    AND h.agent_id = %s
              )
            ORDER BY n.fetched_at DESC
        """

        try:
            results = self.db.execute_query(query, (hours, agent_id))
            logger.info(f"Collected {len(results)} unanalyzed news articles")
            return results or []

        except Exception as e:
            logger.error(f"Failed to collect unanalyzed news: {e}")
            return []
    
    def calculate_portfolio_value(
        self,
        agent_id: str
    ) -> Dict[str, float]:
        """
        Calculate current portfolio value
        
        Args:
            agent_id: AI ID
            
        Returns:
            {'total_value': float, 'cash': float, 'stocks': float}
        """
        logger.info(f"Calculating portfolio value for {agent_id}")
        
        try:
            # Get positions
            positions = self.get_positions(agent_id)
            
            # Get wallet
            wallet_query = """
                SELECT cash_balance, long_term_cash, short_term_cash
                FROM wallets
                WHERE agent_id = %s
            """
            wallet_result = self.db.execute_query(wallet_query, (agent_id,))
            
            if not wallet_result:
                logger.warning(f"Wallet not found for {agent_id}")
                return {'total_value': 0.0, 'cash': 0.0, 'stocks': 0.0}
            
            wallet = wallet_result[0]
            cash_balance = float(wallet['cash_balance'])
            
            # Calculate position value
            stocks_value = sum(float(p['current_value']) for p in positions)
            
            return {
                'total_value': cash_balance + stocks_value,
                'cash': cash_balance,
                'stocks': stocks_value
            }
        
        except Exception as e:
            logger.error(f"Failed to calculate portfolio value: {e}")
            return {'total_value': 0.0, 'cash': 0.0, 'stocks': 0.0}

    def get_price_changes_48h(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Query DynamoDB for 48h price history (hourly data points)

        User Requirement: Use ALL available hourly data points from DynamoDB
        - Data collection frequency: Hourly (48 data points expected)
        - Calculate accurate high/low and change percentage

        Args:
            symbols: List of stock symbols to query

        Returns:
            Dict[symbol, {
                'current_price': float,
                'price_48h_ago': float,
                'change_pct': float,
                'high_48h': float,
                'low_48h': float,
                'avg_volume_48h': int,
                'data_points': int,
                'volatility': float
            }]
        """
        logger.info(f"Fetching 48h price changes for {len(symbols)} symbols")

        try:
            import boto3
            from boto3.dynamodb.conditions import Key
            from decimal import Decimal

            # Initialize DynamoDB client
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            table = dynamodb.Table('StockPrices')

            # Use ET timezone for timestamp calculations
            now = get_et_now()
            timestamp_48h_ago = int((now - timedelta(hours=48)).timestamp())
            timestamp_now = int(now.timestamp())

            results = {}

            for symbol in symbols:
                try:
                    # Query ALL hourly data from last 48 hours
                    response = table.query(
                        KeyConditionExpression=Key('symbol').eq(symbol) &
                                             Key('timestamp').between(timestamp_48h_ago, timestamp_now),
                        ScanIndexForward=True  # Ascending order
                    )

                    items = response.get('Items', [])

                    if len(items) >= 2:
                        # StockPriceFetcher stores data with 'price' field, not OHLC
                        # Convert Decimal to float for calculations
                        prices = [float(item['price']) for item in items]
                        first_price = prices[0]
                        latest_price = prices[-1]

                        # Calculate high/low over ALL 48h data points
                        high_48h = max(prices)
                        low_48h = min(prices)

                        # Calculate percentage change
                        change_pct = ((latest_price - first_price) / first_price) * 100 if first_price > 0 else 0.0

                        results[symbol] = {
                            'current_price': latest_price,
                            'price_48h_ago': first_price,
                            'change_pct': round(change_pct, 2),
                            'high_48h': high_48h,
                            'low_48h': low_48h,
                            'data_points': len(items),
                            'volatility': round(((high_48h - low_48h) / low_48h) * 100, 2) if low_48h > 0 else 0.0
                        }
                    else:
                        # Insufficient data
                        logger.warning(f"Insufficient price data for {symbol} (only {len(items)} points)")
                        results[symbol] = {
                            'current_price': float(items[-1]['price']) if items else None,
                            'change_pct': 0.0,
                            'data_points': len(items),
                            'insufficient_data': True
                        }

                except Exception as e:
                    logger.error(f"Failed to fetch price data for {symbol}: {e}")
                    results[symbol] = {'error': str(e)}

            avg_points = sum(r.get('data_points', 0) for r in results.values()) / len(results) if results else 0
            logger.info(f"Fetched 48h price data for {len(results)} symbols "
                       f"(avg {avg_points:.1f} points/symbol)")

            return results

        except Exception as e:
            logger.error(f"Failed to initialize DynamoDB client: {e}")
            return {}

    def get_recent_earnings_reports(
        self,
        symbols: Optional[List[str]] = None,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Get recently published earnings reports (within N days)

        Args:
            symbols: Optional list to filter specific symbols
            days: Lookback period (default 7 days)

        Returns:
            List of financial reports with summaries
        """
        logger.info(f"Fetching recent earnings reports (last {days} days)")

        query = """
            SELECT symbol, report_type, fiscal_year, fiscal_quarter,
                   filing_date, summary_en, extraction_status
            FROM financial_reports
            WHERE extraction_status = 'completed'
              AND filing_date >= CURRENT_DATE - INTERVAL '%s days'
        """
        params = [days]

        if symbols:
            query += " AND symbol = ANY(%s)"
            params.append(symbols)

        query += " ORDER BY filing_date DESC"

        try:
            results = self.db.execute_query(query, tuple(params))
            logger.info(f"Found {len(results)} recent earnings reports")
            return results or []

        except Exception as e:
            logger.error(f"Failed to get recent earnings reports: {e}")
            return []

    def get_latest_earnings_reports(
        self,
        symbols: Optional[List[str]] = None,
        limit_per_symbol: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Get the LATEST earnings report for each symbol (regardless of date)

        User Requirement: Show most recent earnings even if it's months old

        Args:
            symbols: Optional list to filter specific symbols
            limit_per_symbol: How many reports per symbol (default 1)

        Returns:
            List of financial reports (one per symbol)
        """
        logger.info("Fetching latest earnings reports for all symbols")

        # Use DISTINCT ON to get latest report per symbol
        query = """
            SELECT DISTINCT ON (symbol)
                   symbol, report_type, fiscal_year, fiscal_quarter,
                   filing_date, summary_en, extraction_status
            FROM financial_reports
            WHERE extraction_status = 'completed'
        """

        if symbols:
            query += " AND symbol = ANY(%s)"
            params = (symbols,)
        else:
            params = ()

        query += " ORDER BY symbol, filing_date DESC"

        try:
            results = self.db.execute_query(query, params if params else None)
            logger.info(f"Found {len(results)} latest earnings reports")
            return results or []

        except Exception as e:
            logger.error(f"Failed to get latest earnings reports: {e}")
            return []

    def get_market_indices(self) -> List[str]:
        """
        Get list of market index symbols and major ETFs for market analysis

        Note: Market indices use Yahoo Finance format with ^ prefix (e.g. ^GSPC, ^VIX)
        ETFs use standard symbols (e.g. VOO, GLD)

        Returns:
            List of index/ETF symbols from database
        """
        logger.info("Fetching market indices and ETFs")

        query = """
            SELECT symbol, type, name
            FROM stocks
            WHERE type IN ('market_index', 'etf')
              AND enabled = TRUE
            ORDER BY
                CASE
                    WHEN type = 'market_index' THEN 0
                    WHEN type = 'etf' THEN 1
                END,
                symbol
        """

        try:
            results = self.db.execute_query(query, None)

            if results:
                symbols = [r['symbol'] for r in results]
                logger.info(f"Found {len(symbols)} market indices/ETFs from database: {symbols}")
                return symbols
            else:
                # Fallback: Use major market indices (Yahoo Finance format with ^) + major ETFs
                logger.warning("No indices found in database, using hardcoded fallback")
                fallback_symbols = ['^GSPC', '^VIX', 'VOO', 'GLD']  # S&P 500, VIX, Vanguard S&P 500 ETF, Gold ETF
                logger.info(f"Using fallback symbols: {fallback_symbols}")
                return fallback_symbols

        except Exception as e:
            logger.error(f"Failed to get market indices: {e}")
            # Fallback to major market indicators
            fallback_symbols = ['^GSPC', '^VIX', 'VOO', 'GLD']
            logger.info(f"Using fallback symbols due to error: {fallback_symbols}")
            return fallback_symbols
