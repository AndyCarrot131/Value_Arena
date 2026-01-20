"""
Fetch Data Lambda Function

This Lambda function fetches data from PostgreSQL (RDS) and DynamoDB,
then exports to S3 bucket for frontend consumption.

Data exported:
- ai_agents (agent_id, name, current_capital)
- ai_token_records (all token usage records)
- news_merged (news_articles merged with hourly_news_analysis)
- stock_price (daily last price + today's all prices per stock)
- portfolio_snapshots (daily last snapshot + today's all snapshots per agent)
- stocks, transactions, positions, wallets, daily_reviews, stock_summaries

S3 Bucket: ai-stock-frontend-131/data/
"""

import json
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date, timedelta
from decimal import Decimal
from collections import defaultdict
from typing import Dict, List, Any, Optional


# Configuration
S3_BUCKET = "ai-stock-frontend-131"
S3_PREFIX = "data/"
DYNAMODB_TABLE = "StockPrices"
REGION = "us-east-1"

# Global config loaded from Secrets Manager
config = {}


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder for Decimal and datetime types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def load_config():
    """Load configuration from AWS Secrets Manager (same as StockPriceFetcher)"""
    global config
    try:
        secrets_client = boto3.client('secretsmanager', region_name=REGION)
        response = secrets_client.get_secret_value(SecretId='ai-stock-war/database-config')
        config = json.loads(response['SecretString'])
        print("Configuration loaded successfully from Secrets Manager")
    except Exception as e:
        print(f"Failed to load configuration: {e}")
        raise


def get_db_connection():
    """Create PostgreSQL database connection using config from Secrets Manager"""
    return psycopg2.connect(
        host=config['db_host'],
        port=config.get('db_port', 5432),
        database=config['db_name'],
        user=config['db_user'],
        password=config['db_password'],
        cursor_factory=RealDictCursor
    )


def upload_to_s3(data: Any, filename: str):
    """Upload JSON data to S3"""
    s3_client = boto3.client("s3", region_name=REGION)

    json_data = json.dumps(data, cls=DecimalEncoder, ensure_ascii=False, indent=2)

    key = f"{S3_PREFIX}{filename}"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json"
    )

    print(f"Uploaded {filename} to s3://{S3_BUCKET}/{key}")
    return key


def fetch_ai_agents(cursor) -> List[Dict]:
    """Fetch ai_agents table (agent_id, name, current_capital)"""
    cursor.execute("""
        SELECT agent_id, name, current_capital
        FROM ai_agents
        WHERE enabled = true
        ORDER BY agent_id
    """)
    return cursor.fetchall()


def fetch_ai_token_records(cursor) -> List[Dict]:
    """Fetch all ai_token_records"""
    cursor.execute("""
        SELECT
            record_date, agent_id, service,
            token_in, token_out, created_at, updated_at
        FROM ai_token_records
        ORDER BY record_date DESC, agent_id, service
    """)
    return cursor.fetchall()


def fetch_news_with_analysis(cursor, limit: int = 200) -> List[Dict]:
    """
    Fetch latest news_articles that are mentioned in hourly_news_analysis,
    then merge with analysis data.

    1. Select latest 200 news_ids from hourly_news_analysis
    2. Fetch corresponding news_articles
    3. Merge with all analysis from hourly_news_analysis
    """
    # Step 1: Get latest 200 news_ids that have analysis
    cursor.execute("""
        SELECT DISTINCT news_id
        FROM hourly_news_analysis
        ORDER BY news_id DESC
        LIMIT %s
    """, (limit,))
    news_ids = [row['news_id'] for row in cursor.fetchall()]

    if not news_ids:
        return []

    # Step 2: Fetch news_articles for these news_ids
    cursor.execute("""
        SELECT * FROM news_articles
        WHERE news_id = ANY(%s)
        ORDER BY published_at DESC
    """, (news_ids,))
    news_articles = {row['news_id']: dict(row) for row in cursor.fetchall()}

    # Step 3: Fetch all analysis for these news_ids
    cursor.execute("""
        SELECT * FROM hourly_news_analysis
        WHERE news_id = ANY(%s)
        ORDER BY created_at DESC
    """, (news_ids,))

    # Build analysis mapping: news_id -> {agent_id -> analysis}
    analysis_by_news = defaultdict(dict)
    for row in cursor.fetchall():
        news_id = row['news_id']
        agent_id = row['agent_id']
        analysis_by_news[news_id][agent_id] = {
            'analysis': row.get('analysis', ''),
            'sentiment': row.get('sentiment'),
            'mentioned_stocks': row.get('mentioned_stocks', []),
            'impact_prediction': row.get('impact_prediction'),
            'confidence_score': row.get('confidence_score'),
            'created_at': row.get('created_at')
        }

    # Step 4: Merge news_articles with analysis
    merged_news = []
    for news_id in news_ids:
        if news_id in news_articles:
            article = news_articles[news_id]
            article['AI_agents'] = analysis_by_news.get(news_id, {})
            merged_news.append(article)

    print(f"Merged {len(merged_news)} news articles with analysis")
    return merged_news


def fetch_stock_prices_from_dynamodb(symbols: List[str]) -> Dict[str, List[Dict]]:
    """
    Fetch stock prices from DynamoDB.
    For each stock:
    - Get the last price of each day (for historical data)
    - Get all prices for today

    Returns: {symbol: [{timestamp, price}, ...]}
    """
    dynamodb = boto3.client("dynamodb", region_name=REGION)

    today = datetime.now().date()
    today_start_ts = int(datetime.combine(today, datetime.min.time()).timestamp())

    all_prices = {}

    for symbol in symbols:
        try:
            # Query all prices for this stock (most recent first)
            response = dynamodb.query(
                TableName=DYNAMODB_TABLE,
                KeyConditionExpression="symbol = :symbol",
                ExpressionAttributeValues={":symbol": {"S": symbol}},
                ScanIndexForward=False,
                Limit=1000
            )

            items = response.get("Items", [])

            # Group by date
            prices_by_date = defaultdict(list)
            today_prices = []

            for item in items:
                try:
                    timestamp = int(item["timestamp"]["N"])
                    price = float(item["price"]["N"])

                    item_date = datetime.fromtimestamp(timestamp).date()

                    if item_date == today:
                        today_prices.append({"timestamp": timestamp, "price": price})
                    else:
                        prices_by_date[item_date].append({"timestamp": timestamp, "price": price})
                except (KeyError, ValueError) as e:
                    continue

            # Get last price of each historical day
            historical_prices = []
            for dt, prices in sorted(prices_by_date.items(), reverse=True):
                # Get the latest price of that day (highest timestamp)
                last_price = max(prices, key=lambda x: x["timestamp"])
                historical_prices.append(last_price)

            # Combine: historical (sorted by time) + today's all prices
            # Sort historical by timestamp ascending, then add today's prices
            historical_prices.sort(key=lambda x: x["timestamp"])
            today_prices.sort(key=lambda x: x["timestamp"])

            all_prices[symbol] = historical_prices + today_prices

            print(f"  {symbol}: {len(historical_prices)} historical + {len(today_prices)} today")

        except Exception as e:
            print(f"  {symbol}: ERROR - {e}")
            all_prices[symbol] = []

    return all_prices


def fetch_portfolio_snapshots(cursor) -> List[Dict]:
    """
    Fetch portfolio_snapshots.
    For each agent:
    - Get the last snapshot of each day (for historical data)
    - Get all snapshots for today
    """
    today = date.today()

    # Fetch all snapshots
    cursor.execute("""
        SELECT * FROM portfolio_snapshots
        ORDER BY agent_id, snapshot_time DESC
    """)

    all_snapshots = cursor.fetchall()

    # Group by agent
    snapshots_by_agent = defaultdict(list)
    for snapshot in all_snapshots:
        agent_id = snapshot['agent_id']
        snapshots_by_agent[agent_id].append(dict(snapshot))

    result = []

    for agent_id, snapshots in snapshots_by_agent.items():
        # Group by date
        snapshots_by_date = defaultdict(list)
        today_snapshots = []

        for snapshot in snapshots:
            snapshot_time = snapshot['snapshot_time']
            if isinstance(snapshot_time, str):
                snapshot_time = datetime.fromisoformat(snapshot_time)

            snapshot_date = snapshot_time.date()

            if snapshot_date == today:
                today_snapshots.append(snapshot)
            else:
                snapshots_by_date[snapshot_date].append(snapshot)

        # Get last snapshot of each historical day
        historical_snapshots = []
        for dt, day_snapshots in sorted(snapshots_by_date.items(), reverse=True):
            # Get the latest snapshot of that day
            last_snapshot = max(day_snapshots, key=lambda x: x['snapshot_time'] if isinstance(x['snapshot_time'], datetime) else datetime.fromisoformat(x['snapshot_time']))
            historical_snapshots.append(last_snapshot)

        # Sort historical by time ascending, then add today's snapshots
        historical_snapshots.sort(key=lambda x: x['snapshot_time'] if isinstance(x['snapshot_time'], datetime) else datetime.fromisoformat(x['snapshot_time']))
        today_snapshots.sort(key=lambda x: x['snapshot_time'] if isinstance(x['snapshot_time'], datetime) else datetime.fromisoformat(x['snapshot_time']))

        agent_snapshots = historical_snapshots + today_snapshots
        result.extend(agent_snapshots)

        print(f"  {agent_id}: {len(historical_snapshots)} historical + {len(today_snapshots)} today")

    return result


def fetch_simple_table(cursor, table_name: str) -> List[Dict]:
    """Fetch all data from a simple table"""
    cursor.execute(f"SELECT * FROM {table_name}")
    return [dict(row) for row in cursor.fetchall()]


def lambda_handler(event, context):
    """Main Lambda handler"""
    print("=" * 60)
    print("Fetch Data Lambda - Starting")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 60)

    # Load config from Secrets Manager
    load_config()

    results = {}

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Fetch ai_agents
        print("\n[1/10] Fetching ai_agents...")
        ai_agents = fetch_ai_agents(cursor)
        upload_to_s3(ai_agents, "ai_agents.json")
        results["ai_agents"] = len(ai_agents)

        # 2. Fetch ai_token_records
        print("\n[2/10] Fetching ai_token_records...")
        token_records = fetch_ai_token_records(cursor)
        upload_to_s3(token_records, "ai_token_records.json")
        results["ai_token_records"] = len(token_records)

        # 3. Fetch news_merged (news_articles + hourly_news_analysis)
        print("\n[3/10] Fetching and merging news with analysis...")
        news_merged = fetch_news_with_analysis(cursor, limit=200)
        upload_to_s3(news_merged, "news_merged.json")
        results["news_merged"] = len(news_merged)

        # 4. Fetch stocks (needed for stock_price)
        print("\n[4/10] Fetching stocks...")
        stocks = fetch_simple_table(cursor, "stocks")
        upload_to_s3(stocks, "stocks.json")
        results["stocks"] = len(stocks)

        # 5. Fetch stock_prices from DynamoDB
        print("\n[5/10] Fetching stock prices from DynamoDB...")
        enabled_symbols = [s['symbol'] for s in stocks if s.get('enabled', True)]
        stock_prices = fetch_stock_prices_from_dynamodb(enabled_symbols)
        upload_to_s3(stock_prices, "stock_price.json")
        results["stock_price"] = sum(len(prices) for prices in stock_prices.values())

        # 6. Fetch portfolio_snapshots
        print("\n[6/10] Fetching portfolio_snapshots...")
        portfolio_snapshots = fetch_portfolio_snapshots(cursor)
        upload_to_s3(portfolio_snapshots, "portfolio_snapshots.json")
        results["portfolio_snapshots"] = len(portfolio_snapshots)

        # 7. Fetch transactions
        print("\n[7/10] Fetching transactions...")
        transactions = fetch_simple_table(cursor, "transactions")
        upload_to_s3(transactions, "transactions.json")
        results["transactions"] = len(transactions)

        # 8. Fetch positions
        print("\n[8/10] Fetching positions...")
        positions = fetch_simple_table(cursor, "positions")
        upload_to_s3(positions, "positions.json")
        results["positions"] = len(positions)

        # 9. Fetch wallets
        print("\n[9/10] Fetching wallets...")
        wallets = fetch_simple_table(cursor, "wallets")
        upload_to_s3(wallets, "wallets.json")
        results["wallets"] = len(wallets)

        # 10. Fetch daily_reviews and stock_summaries
        print("\n[10/10] Fetching daily_reviews and stock_summaries...")
        daily_reviews = fetch_simple_table(cursor, "daily_reviews")
        upload_to_s3(daily_reviews, "daily_reviews.json")
        results["daily_reviews"] = len(daily_reviews)

        stock_summaries = fetch_simple_table(cursor, "stock_summaries")
        upload_to_s3(stock_summaries, "stock_summaries.json")
        results["stock_summaries"] = len(stock_summaries)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    print("\n" + "=" * 60)
    print("Fetch Data Lambda - Completed Successfully")
    print("=" * 60)
    print(f"Results: {json.dumps(results, indent=2)}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Data export completed successfully",
            "results": results,
            "timestamp": datetime.now().isoformat()
        })
    }


if __name__ == "__main__":
    # For local testing
    result = lambda_handler({}, None)
    print(result)
