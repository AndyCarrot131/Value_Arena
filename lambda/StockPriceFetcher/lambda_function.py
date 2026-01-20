import json
import boto3
import redis
import yfinance as yf
import psycopg2
import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any

# --- 初始化日志 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class StockPriceFetcher:
    def __init__(self, test_mode: bool = False):
        """
        Initialize StockPriceFetcher

        Args:
            test_mode: If True, will not write to any database (DynamoDB or PostgreSQL)
        """
        self.test_mode = test_mode
        self.config = {}
        self.stocks = {} # 格式: {'AAPL': 'Apple Inc'}

        if self.test_mode:
            logger.info("=" * 80)
            logger.info("RUNNING IN TEST MODE - No database writes will occur")
            logger.info("=" * 80)

        # 1. 加载配置 (从 Secrets Manager 获取所有配置)
        self.load_config()

        # 从配置中提取变量，消除 Hardcode
        self.redis_endpoint = self.config.get('redis_host')
        self.redis_port = int(self.config.get('redis_port', 6379))
        self.dynamodb_table = self.config.get('dynamodb_tables', {}).get('stock_prices', 'StockPrices')

        # 2. 连接数据库 (使用配置)
        self.setup_database()
        # 3. 加载股票列表
        self.load_stock_symbols()

        # 初始化持久化存储客户端
        # 增加了我们之前调试成功的 SSL 和 Timeout 配置
        self.r = redis.StrictRedis(
            host=self.redis_endpoint,
            port=self.redis_port,
            db=0,
            decode_responses=True,
            ssl=self.config.get('redis_ssl', True), # 从 Secret 获取 SSL 开关
            socket_connect_timeout=5,
            socket_timeout=5
        )

        # 初始化 DynamoDB
        self.db = boto3.resource('dynamodb', region_name=self.config.get('dynamodb_region', 'us-east-1'))
        self.table = self.db.Table(self.dynamodb_table)

    def load_config(self):
        """从 Secrets Manager 获取数据库凭据"""
        try:
            secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
            response = secrets_client.get_secret_value(SecretId='ai-stock-war/database-config')
            self.config = json.loads(response['SecretString'])
            logger.info("Configuration loaded successfully from Secrets Manager")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def setup_database(self):
        """连接 PostgreSQL (RDS)"""
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

    def load_stock_symbols(self):
        """动态加载启用的股票清单（包括股票、ETF和市场指数）"""
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT symbol, name
                    FROM stocks
                    WHERE enabled = TRUE AND type IN ('stock', 'etf', 'market_index')
                """)
                self.stocks = {row[0]: row[1] for row in cursor.fetchall()}
            logger.info(f"Loaded {len(self.stocks)} stocks/ETFs/indices from DB")
        except Exception as e:
            logger.error(f"Failed to load stocks from DB: {e}")
            raise

    def get_stock_price_from_redis(self, symbol: str) -> Optional[Decimal]:
        """
        Get current stock price from Redis

        Args:
            symbol: Stock ticker symbol

        Returns:
            Current stock price as Decimal, or None if not available
        """
        try:
            price_str = self.r.get(f"stock:price:{symbol}")
            if price_str:
                return Decimal(str(price_str))
        except Exception as e:
            logger.warning(f"Failed to get price for {symbol} from Redis: {e}")

        return None

    def calculate_agent_portfolio(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Calculate total portfolio value for an agent

        Args:
            agent_id: Agent identifier

        Returns:
            Dictionary with portfolio breakdown, or None if no data
        """
        try:
            with self.db_conn.cursor() as cursor:
                # Get wallet info
                cursor.execute("""
                    SELECT
                        cash_balance,
                        long_term_cash,
                        short_term_cash,
                        reserved_cash,
                        total_invested
                    FROM wallets
                    WHERE agent_id = %s
                """, (agent_id,))

                wallet = cursor.fetchone()

                if not wallet:
                    logger.warning(f"No wallet found for {agent_id}")
                    return None

                # Get all positions
                cursor.execute("""
                    SELECT
                        symbol,
                        quantity,
                        average_cost,
                        current_value,
                        position_type
                    FROM positions
                    WHERE agent_id = %s AND quantity > 0
                """, (agent_id,))

                positions = cursor.fetchall()

                # Calculate position values with current prices
                total_long_term_value = Decimal('0')
                total_short_term_value = Decimal('0')
                position_details = []

                for pos in positions:
                    symbol = pos[0]
                    quantity = pos[1]
                    avg_cost = Decimal(str(pos[2]))
                    position_type = pos[4]

                    # Get current price from Redis
                    current_price = self.get_stock_price_from_redis(symbol)

                    if current_price is None:
                        # Fallback to average cost
                        current_price = avg_cost
                        logger.debug(f"Using avg_cost ${avg_cost} for {symbol} (Redis unavailable)")

                    # Calculate current value
                    current_value = current_price * quantity
                    unrealized_pnl = current_value - (avg_cost * quantity)

                    position_details.append({
                        'symbol': symbol,
                        'quantity': quantity,
                        'avg_cost': float(avg_cost),
                        'current_price': float(current_price),
                        'current_value': float(current_value),
                        'unrealized_pnl': float(unrealized_pnl),
                        'position_type': position_type
                    })

                    # Add to totals
                    if position_type == 'LONG_TERM':
                        total_long_term_value += current_value
                    else:
                        total_short_term_value += current_value

                # Calculate total portfolio value
                cash_balance = Decimal(str(wallet[0]))
                long_term_cash = Decimal(str(wallet[1]))
                short_term_cash = Decimal(str(wallet[2]))
                reserved_cash = Decimal(str(wallet[3]))

                total_portfolio_value = cash_balance + total_long_term_value + total_short_term_value

                return {
                    'agent_id': agent_id,
                    'cash': {
                        'total': float(cash_balance),
                        'long_term': float(long_term_cash),
                        'short_term': float(short_term_cash),
                        'reserved': float(reserved_cash)
                    },
                    'investments': {
                        'long_term_value': float(total_long_term_value),
                        'short_term_value': float(total_short_term_value),
                        'total_value': float(total_long_term_value + total_short_term_value)
                    },
                    'portfolio_value': float(total_portfolio_value),
                    'positions': position_details,
                    'positions_count': len(position_details)
                }

        except Exception as e:
            logger.error(f"Failed to calculate portfolio for {agent_id}: {e}")
            return None

    def store_portfolio_snapshot(self, portfolio_data: Dict[str, Any]):
        """
        Store portfolio snapshot to database

        Args:
            portfolio_data: Portfolio data dictionary
        """
        if self.test_mode:
            logger.info(f"[TEST MODE] Would store snapshot for {portfolio_data['agent_id']}: "
                       f"${portfolio_data['portfolio_value']:,.2f}")
            return

        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO portfolio_snapshots (
                        agent_id,
                        snapshot_time,
                        cash_balance,
                        long_term_cash,
                        short_term_cash,
                        reserved_cash,
                        long_term_investments,
                        short_term_investments,
                        total_portfolio_value,
                        positions_detail,
                        positions_count
                    ) VALUES (
                        %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    portfolio_data['agent_id'],
                    portfolio_data['cash']['total'],
                    portfolio_data['cash']['long_term'],
                    portfolio_data['cash']['short_term'],
                    portfolio_data['cash']['reserved'],
                    portfolio_data['investments']['long_term_value'],
                    portfolio_data['investments']['short_term_value'],
                    portfolio_data['portfolio_value'],
                    json.dumps(portfolio_data['positions']),
                    portfolio_data['positions_count']
                ))

            self.db_conn.commit()
            logger.info(
                f"Stored snapshot for {portfolio_data['agent_id']}: "
                f"${portfolio_data['portfolio_value']:,.2f}"
            )

        except Exception as e:
            logger.error(f"Failed to store snapshot: {e}")
            raise

    def update_positions_current_value(self, agent_id: str, positions: List[Dict[str, Any]]):
        """
        Update current_value in positions table

        Args:
            agent_id: Agent identifier
            positions: List of position details with current values
        """
        if self.test_mode:
            logger.info(f"[TEST MODE] Would update {len(positions)} position values for {agent_id}")
            return

        try:
            with self.db_conn.cursor() as cursor:
                for pos in positions:
                    cursor.execute("""
                        UPDATE positions
                        SET
                            current_value = %s,
                            unrealized_pnl = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE agent_id = %s AND symbol = %s
                    """, (pos['current_value'], pos['unrealized_pnl'], agent_id, pos['symbol']))

            self.db_conn.commit()
            logger.debug(f"Updated {len(positions)} position values for {agent_id}")

        except Exception as e:
            logger.error(f"Failed to update positions: {e}")
            raise

    def update_agent_current_capital(self, agent_id: str, portfolio_value: float):
        """
        Update ai_agents.current_capital with total portfolio value

        Args:
            agent_id: Agent identifier
            portfolio_value: Total portfolio value (cash + investments)
        """
        if self.test_mode:
            logger.info(f"[TEST MODE] Would update {agent_id} current_capital to ${portfolio_value:,.2f}")
            return

        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE ai_agents
                    SET current_capital = %s
                    WHERE agent_id = %s
                """, (portfolio_value, agent_id))

            self.db_conn.commit()
            logger.info(f"Updated {agent_id} current_capital to ${portfolio_value:,.2f}")

        except Exception as e:
            logger.error(f"Failed to update current_capital for {agent_id}: {e}")
            raise

    def process_portfolio_snapshots(self):
        """
        Process portfolio snapshots for all active agents
        """
        logger.info("=" * 80)
        logger.info("Processing portfolio snapshots for all active agents")
        logger.info("=" * 80)

        try:
            # Get all active agents
            with self.db_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT agent_id, name
                    FROM ai_agents
                    WHERE enabled = true
                """)
                agents = cursor.fetchall()

            if not agents:
                logger.warning("No active agents found")
                return

            logger.info(f"Found {len(agents)} active agents")

            success_count = 0
            error_count = 0

            for agent in agents:
                agent_id = agent[0]
                agent_name = agent[1]

                try:
                    # Calculate portfolio
                    portfolio_data = self.calculate_agent_portfolio(agent_id)

                    if portfolio_data is None:
                        logger.warning(f"Skipping {agent_id}: no wallet data")
                        error_count += 1
                        continue

                    # Store snapshot
                    self.store_portfolio_snapshot(portfolio_data)

                    # Update positions table with current values
                    self.update_positions_current_value(
                        agent_id,
                        portfolio_data['positions']
                    )

                    # Update ai_agents.current_capital
                    self.update_agent_current_capital(
                        agent_id,
                        portfolio_data['portfolio_value']
                    )

                    success_count += 1

                    logger.info(
                        f"✓ {agent_id} ({agent_name}): ${portfolio_data['portfolio_value']:,.2f} "
                        f"({portfolio_data['positions_count']} positions)"
                    )

                except Exception as e:
                    logger.error(f"Failed to process {agent_id}: {e}", exc_info=True)
                    error_count += 1

            logger.info(
                f"Portfolio snapshot complete: {success_count} succeeded, {error_count} failed"
            )

        except Exception as e:
            logger.error(f"Portfolio snapshot processing failed: {e}", exc_info=True)

    def run(self):
        import yfinance as yf
        # 必须设置缓存，解决只读文件系统问题
        yf.set_tz_cache_location("/tmp")

        stats = {"success": 0, "errors": 0}

        # Step 1: Fetch and update stock prices
        logger.info("=" * 80)
        logger.info("Step 1: Fetching and updating stock prices")
        logger.info("=" * 80)

        for db_symbol, name in self.stocks.items():
            try:
                # 转换符号 (例如 BRK.B -> BRK-B)
                api_symbol = db_symbol.replace('.', '-')
                logger.info(f"==> Processing {api_symbol}...")

                ticker = yf.Ticker(api_symbol)
                price = None

                # 尝试使用 fast_info 获取最新价
                try:
                    price = ticker.fast_info.get('last_price')
                except:
                    pass

                # 如果 fast_info 失败或返回空值，使用 history 作为 fallback
                if not price or price <= 0:
                    logger.info(f"fast_info failed for {db_symbol}, trying history fallback...")
                    hist = ticker.history(period='1d')
                    if not hist.empty:
                        price = float(hist['Close'].iloc[-1])
                        logger.info(f"Retrieved price from history for {db_symbol}: ${price}")

                if price and price > 0:
                    timestamp = int(datetime.now().timestamp())

                    # Write to Redis
                    if not self.test_mode:
                        self.r.set(f"stock:price:{db_symbol}", price)
                    else:
                        logger.info(f"[TEST MODE] Would write to Redis: stock:price:{db_symbol} = {price}")

                    # Write to DynamoDB
                    if not self.test_mode:
                        self.table.put_item(
                            Item={
                                'symbol': db_symbol,
                                'timestamp': timestamp,
                                'price': Decimal(str(round(price, 2))),
                                'name': name,
                                'created_at': datetime.now().isoformat()
                            }
                        )
                        logger.info(f"Successfully updated {db_symbol} at ${price}")
                    else:
                        logger.info(f"[TEST MODE] Would write to DynamoDB: {db_symbol} at ${price}")

                    stats["success"] += 1
                else:
                    logger.warning(f"No price data for {db_symbol}")
                    stats["errors"] += 1

            except Exception as e:
                logger.error(f"Error updating {db_symbol}: {str(e)}")
                stats["errors"] += 1

        # Step 2: Process portfolio snapshots
        logger.info("")
        logger.info("=" * 80)
        logger.info("Step 2: Processing portfolio snapshots")
        logger.info("=" * 80)
        self.process_portfolio_snapshots()

        return stats

# --- Lambda 入口 ---
def lambda_handler(event, context):
    """
    Lambda handler - supports test_mode via event parameter

    Event parameters:
        test_mode (bool): If True, no database writes will occur

    Example:
        # Normal mode
        lambda_handler({}, context)

        # Test mode
        lambda_handler({'test_mode': True}, context)
    """
    # Check for test_mode in event
    test_mode = event.get('test_mode', False)

    fetcher = StockPriceFetcher(test_mode=test_mode)
    result = fetcher.run()

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }