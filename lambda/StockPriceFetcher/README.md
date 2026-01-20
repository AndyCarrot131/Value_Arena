# StockPriceFetcher Lambda Function

## Overview

This Lambda function performs two main tasks:
1. **Stock Price Fetching**: Fetches current stock prices from Yahoo Finance and stores them in Redis and DynamoDB
2. **Portfolio Snapshot Processing**: Calculates and stores portfolio snapshots for all active AI agents

## Features

### Stock Price Fetching
- Fetches real-time stock prices using Yahoo Finance API
- Stores prices in Redis for fast access (key format: `stock:price:{symbol}`)
- Archives historical prices in DynamoDB `StockPrices` table
- Supports stocks, ETFs, and market indices

### Portfolio Snapshot Processing
- Calculates total portfolio value for each active AI agent
- Stores snapshot data in PostgreSQL `portfolio_snapshots` table
- Updates position values in `positions` table
- Updates current capital in `ai_agents` table

### Test Mode Support
- Supports `test_mode` parameter to run without writing to any database
- Useful for testing and validation
- All write operations are logged but not executed in test mode

## Database Operations

### DynamoDB
- **Table**: `StockPrices`
- **Operation**: `put_item` - Store stock price with timestamp
- **Test Mode**: Skipped

### Redis
- **Key Format**: `stock:price:{symbol}`
- **Operation**: `set` - Store current stock price
- **Test Mode**: Skipped

### PostgreSQL

#### Writes
1. **portfolio_snapshots** - Insert new snapshot record
   - `agent_id`, `snapshot_time`, cash breakdown, investment values, positions detail

2. **positions** - Update current values
   - `current_value`, `unrealized_pnl`, `updated_at`

3. **ai_agents** - Update current capital
   - `current_capital` = cash_balance + total_investment_value

#### Reads
- `stocks` - Load enabled stocks/ETFs/indices
- `ai_agents` - Get all active agents
- `wallets` - Get cash balances
- `positions` - Get current positions

## Usage

### Normal Mode (Production)
```python
# Lambda event
{}

# Or explicitly
{'test_mode': False}
```

### Test Mode (No Database Writes)
```python
# Lambda event
{'test_mode': True}
```

## Execution Flow

```
1. Fetch Stock Prices
   ├── Load enabled stocks from PostgreSQL
   ├── Fetch prices from Yahoo Finance
   ├── Write to Redis (stock:price:{symbol})
   └── Write to DynamoDB (StockPrices table)

2. Process Portfolio Snapshots
   ├── Load active agents from PostgreSQL
   ├── For each agent:
   │   ├── Get wallet data (cash balances)
   │   ├── Get positions data
   │   ├── Fetch current prices from Redis
   │   ├── Calculate portfolio value
   │   ├── INSERT portfolio_snapshots
   │   ├── UPDATE positions (current_value, unrealized_pnl)
   │   └── UPDATE ai_agents (current_capital)
   └── Log summary statistics
```

## Data Flow

```
Yahoo Finance API
    ↓
Lambda (Fetch Prices)
    ↓
    ├─→ Redis (stock:price:{symbol})
    └─→ DynamoDB (StockPrices table)
         ↓
Lambda (Portfolio Snapshot)
    ↓
    ├─→ Read from Redis (current prices)
    ├─→ Read from PostgreSQL (agents, wallets, positions)
    └─→ Write to PostgreSQL (portfolio_snapshots, positions, ai_agents)
```

## Test Mode Behavior

When `test_mode=True`:
- ✅ **Allowed**: Read operations from all databases
- ✅ **Allowed**: API calls to Yahoo Finance
- ❌ **Blocked**: Write to Redis
- ❌ **Blocked**: Write to DynamoDB
- ❌ **Blocked**: Write to PostgreSQL (INSERT/UPDATE)
- 📝 **Logged**: All blocked operations are logged with `[TEST MODE]` prefix

## Example Logs

### Normal Mode
```
Step 1: Fetching and updating stock prices
==> Processing AAPL...
Successfully updated AAPL at $150.25

Step 2: Processing portfolio snapshots
Found 3 active agents
Stored snapshot for agent_deepseek: $125,430.50
Updated 5 position values for agent_deepseek
Updated agent_deepseek current_capital to $125,430.50
✓ agent_deepseek (DeepSeek Agent): $125,430.50 (5 positions)
Portfolio snapshot complete: 3 succeeded, 0 failed
```

### Test Mode
```
RUNNING IN TEST MODE - No database writes will occur

Step 1: Fetching and updating stock prices
==> Processing AAPL...
[TEST MODE] Would write to Redis: stock:price:AAPL = 150.25
[TEST MODE] Would write to DynamoDB: AAPL at $150.25

Step 2: Processing portfolio snapshots
Found 3 active agents
[TEST MODE] Would store snapshot for agent_deepseek: $125,430.50
[TEST MODE] Would update 5 position values for agent_deepseek
[TEST MODE] Would update agent_deepseek current_capital to $125,430.50
```

## Configuration

Configuration is loaded from AWS Secrets Manager:
- **Secret Name**: `ai-stock-war/database-config`
- **Required Fields**:
  - `db_host`, `db_port`, `db_name`, `db_user`, `db_password` (PostgreSQL)
  - `redis_host`, `redis_port`, `redis_ssl` (Redis)
  - `dynamodb_region`, `dynamodb_tables.stock_prices` (DynamoDB)

## Deployment

This Lambda function should be scheduled to run:
- **Frequency**: Hourly (during market hours)
- **Trigger**: EventBridge (CloudWatch Events)
- **Timeout**: 5 minutes
- **Memory**: 512 MB

## Error Handling

- Individual stock fetch failures are logged but don't stop the entire process
- Portfolio calculation failures for individual agents are logged and counted
- Final summary shows success/error counts for both operations

## Dependencies

- `boto3` - AWS SDK (DynamoDB, Secrets Manager)
- `redis` - Redis client
- `yfinance` - Yahoo Finance API
- `psycopg2` - PostgreSQL driver

## Notes

- Stock symbols are automatically converted (e.g., `BRK.B` → `BRK-B`) for Yahoo Finance API
- yfinance cache is set to `/tmp` to work with Lambda's read-only filesystem
- Portfolio value = cash_balance + sum(all positions current_value)
- Current capital update ensures `ai_agents` table always reflects latest portfolio value
