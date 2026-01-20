# AI Stock War - Database Architecture Documentation

> **Schema Reference:** Based on `SQL/DB_schema.txt`
> **Last Updated:** 2026-01-11

---

## Table of Contents

1. [Overview](#overview)
2. [Database Tables Summary](#database-tables-summary)
3. [File-by-File Database Operations](#file-by-file-database-operations)
4. [Critical Patterns & Best Practices](#critical-patterns--best-practices)

---

## Overview

This document provides a comprehensive mapping of all PostgreSQL database operations in the AI Stock War system, organized by source file. It details every READ, WRITE, UPDATE, and DELETE operation performed on each table.

**Total Tables:** 22
**Core Service Files:** 10
**Database Engine:** PostgreSQL with JSONB support

---

## Database Tables Summary

### Core Information Tables (4)

| Table | Primary Key | Description | Key Fields |
|-------|-------------|-------------|------------|
| **stocks** | id (serial) | Stock/ETF trading pool | symbol (UNIQUE), name, sector, industry, type, enabled |
| **rss_sources** | id (serial) | RSS news feed configuration | name, url, category, priority, enabled, last_fetched |
| **news_articles** | id (serial) | Collected news articles | news_id (UNIQUE), title, content, source, published_at, related_stocks[], sentiment |
| **financial_reports** | id (serial) | SEC financial reports metadata | symbol, report_type, fiscal_year, pdf_s3_key, txt_s3_key |

### AI Agent Management (5)

| Table | Primary Key | Description | Key Fields |
|-------|-------------|-------------|------------|
| **ai_agents** | id (serial) | AI trading agent configuration | agent_id (UNIQUE), name, model, api_url, initial_capital, enabled |
| **ai_state** | agent_id (PK) | AI persistent state | portfolio_summary (JSONB), investment_thesis, weekly_trade_quota (JSONB), state_version |
| **ai_learning_logs** | id (serial) | Learning phase analysis logs | agent_id, news_id, analysis, sentiment, learning_day |
| **key_events** | id (serial) | Important trading events | agent_id, event_type, symbol, context (JSONB), estimated_tokens |
| **memory_archives** | id (serial) | Compressed historical memory | agent_id, archive_type, period_start, compressed_summary |

### Trading & Portfolio (5)

| Table | Primary Key | Description | Key Fields |
|-------|-------------|-------------|------------|
| **positions** | id (serial) | Current holdings | agent_id, symbol (UNIQUE), quantity, average_cost, position_type, first_buy_date |
| **transactions** | id (serial) | Transaction history | agent_id, symbol, action, quantity, price, position_type, decision_id, market_context (JSONB) |
| **wallets** | agent_id (PK) | Agent cash management | cash_balance, long_term_cash, short_term_cash, reserved_cash, total_invested |
| **portfolio_snapshots** | id (serial) | Periodic portfolio snapshots (2-hour) | agent_id, snapshot_time, cash_balance, positions_detail (JSONB), total_portfolio_value |
| **decision_quality** | id (serial) | Decision quality evaluation | decision_id, agent_id, predicted_outcome, actual_outcome, quality_weight |

### Analysis & Reporting (5)

| Table | Primary Key | Description | Key Fields |
|-------|-------------|-------------|------------|
| **hourly_news_analysis** | id (serial) | Hourly trading phase analysis | agent_id, news_id, analysis, sentiment, mentioned_stocks[], confidence_score |
| **daily_reviews** | id (serial) | Daily P&L reviews | review_date, agent_id (UNIQUE), portfolio_value, daily_pnl, total_pnl, review_content |
| **compliance_violations** | id (serial) | Trading rule violations | agent_id, violation_type, attempted_action (JSONB), severity, notes |

### System (3)

| Table | Primary Key | Description | Key Fields |
|-------|-------------|-------------|------------|
| **system_config** | id (serial) | System-wide configuration | config_key (UNIQUE), config_value, description |
| **news_similarity** | id (serial) | News article similarity tracking | article_1_id, article_2_id, similarity_score, detection_method |

---

## File-by-File Database Operations

### 1. services/memory_manager.py

**Purpose:** Manages AI agent persistent state, key events, and wallet operations

#### READ Operations (3)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 37-51 | ai_state | agent_id, portfolio_summary, investment_thesis, market_view, weekly_trade_quota, long_term_allocation, cash_ratio, state_version, estimated_tokens, last_updated | WHERE agent_id = %s | `load_ai_state()` |
| 219-232 | key_events | event_type, symbol, event_date, description, context, impact, estimated_tokens, created_at | WHERE agent_id = %s ORDER BY created_at DESC LIMIT %s | `get_key_events()` |
| 349-361 | wallets | cash_balance, long_term_cash, short_term_cash, reserved_cash, total_invested, total_withdrawn, last_transaction_at, updated_at | WHERE agent_id = %s | `get_wallet()` |

#### WRITE Operations (2)

| Line | Table | Fields | Special Logic | Method |
|------|-------|--------|---------------|--------|
| 94-108 | ai_state | agent_id, portfolio_summary, investment_thesis, market_view, weekly_trade_quota, long_term_allocation, cash_ratio, state_version, estimated_tokens | ON CONFLICT (agent_id) DO NOTHING | `_initialize_ai_state()` |
| 273-283 | key_events | agent_id, event_type, symbol, event_date, description, context (JSONB), impact, estimated_tokens | Triggers `_cleanup_old_events()` | `append_key_event()` |

#### UPDATE Operations (2)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 179-183 | ai_state | Dynamic fields, state_version (incremented), last_updated | WHERE agent_id = %s AND state_version = %s | `update_ai_state()` (Optimistic Locking) |
| 399-407 | wallets | cash_balance, long_term_cash, short_term_cash, last_transaction_at, updated_at | WHERE agent_id = %s | `update_wallet()` |

#### DELETE Operations (1)

| Line | Table | Filter | Purpose | Method |
|------|-------|--------|---------|--------|
| 318-327 | key_events | WHERE agent_id = %s AND id NOT IN (top 20 by created_at) | Retain only 20 most recent events | `_cleanup_old_events()` |

**Key Patterns:**
- **Optimistic Locking:** Uses `state_version` field to prevent concurrent update conflicts
- **JSONB Usage:** portfolio_summary, weekly_trade_quota, context stored as JSONB
- **Auto-cleanup:** Automatically prunes old key_events to maintain performance

---

### 2. services/portfolio_executor.py

**Purpose:** Executes BUY/SELL trades, manages positions and transactions

#### READ Operations (3)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 103-107 | positions | quantity, average_cost, position_type, first_buy_date | WHERE agent_id = %s AND symbol = %s | `_execute_buy()` |
| 198-202 | positions | quantity, average_cost, position_type | WHERE agent_id = %s AND symbol = %s | `_execute_sell()` |
| 355-357 | positions | symbol, quantity, average_cost | WHERE agent_id = %s | `update_position_values()` |

#### WRITE Operations (2)

| Line | Table | Fields | Notes | Method |
|------|-------|--------|-------|--------|
| 139-148 | positions | agent_id, symbol, quantity, average_cost, position_type, first_buy_date | Only when new position | `_execute_buy()` |
| 292-304 | transactions | agent_id, symbol, action, quantity, price, total_amount, reason, position_type, decision_id, market_context (JSONB) | All trades logged | `_record_transaction()` |

#### UPDATE Operations (7)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 126-133 | positions | quantity, average_cost, updated_at | WHERE agent_id = %s AND symbol = %s | `_execute_buy()` (Add to existing) |
| 152-161 | wallets | cash_balance, long_term_cash, total_invested, last_transaction_at, updated_at | WHERE agent_id = %s | `_execute_buy()` (LONG_TERM) |
| 164-173 | wallets | cash_balance, short_term_cash, total_invested, last_transaction_at, updated_at | WHERE agent_id = %s | `_execute_buy()` (SHORT_TERM) |
| 222-228 | positions | quantity, updated_at | WHERE agent_id = %s AND symbol = %s | `_execute_sell()` (Partial) |
| 238-248 | wallets | cash_balance, long_term_cash, total_withdrawn, last_transaction_at, updated_at | WHERE agent_id = %s | `_execute_sell()` (LONG_TERM) |
| 250-260 | wallets | cash_balance, short_term_cash, total_withdrawn, last_transaction_at, updated_at | WHERE agent_id = %s | `_execute_sell()` (SHORT_TERM) |
| 326-336 | ai_state | monthly_trade_quota (JSONB), last_updated | WHERE agent_id = %s | `_update_trade_quota()` (JSONB manipulation) |
| 383-389 | positions | current_value, unrealized_pnl, updated_at | WHERE agent_id = %s AND symbol = %s | `update_position_values()` |

#### DELETE Operations (1)

| Line | Table | Filter | Condition | Method |
|------|-------|--------|-----------|--------|
| 232-235 | positions | WHERE agent_id = %s AND symbol = %s | When quantity reaches 0 after SELL | `_execute_sell()` |

**Key Patterns:**
- **Atomic Transactions:** All BUY/SELL operations use database cursor for atomicity
- **Dual Account System:** Separate tracking for LONG_TERM vs SHORT_TERM cash
- **Average Cost Calculation:** Weighted average when adding to positions
- **Trade Quota Tracking:** Updates monthly_trade_quota JSONB field using `jsonb_set()`

---

### 3. services/decision_validator.py

**Purpose:** Validates trading decisions against compliance rules

#### READ Operations (6)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 109-112 | stocks | symbol, type | WHERE symbol = %s AND enabled = TRUE AND type IN ('stock', 'etf') | `_validate_stock_pool()` |
| 137-140 | ai_state | monthly_trade_quota | WHERE agent_id = %s | `_validate_trade_quota()` |
| 187-190 | wallets | long_term_cash, short_term_cash | WHERE agent_id = %s | `_validate_wallet_balance()` |
| 239-242 | wallets | cash_balance, long_term_cash, short_term_cash | WHERE agent_id = %s | `_validate_account_allocation()` |
| 295-298 | positions | position_type, first_buy_date | WHERE agent_id = %s AND symbol = %s | `_validate_wash_trade()` (30-day rule) |
| 401-413 | compliance_violations | violation_type, attempted_action, detection_method, severity, notes, detected_at | WHERE agent_id = %s AND detected_at > NOW() - INTERVAL '%s days' ORDER BY detected_at DESC | `get_recent_violations()` |

#### WRITE Operations (1)

| Line | Table | Fields | Skip Condition | Method |
|------|-------|--------|----------------|--------|
| 359-380 | compliance_violations | agent_id, violation_type, attempted_action (JSONB), detection_method, severity, notes | Skipped in test_mode | `_log_violation()` |

**Key Validation Rules:**
- **Stock Pool:** Only enabled stocks/ETFs allowed
- **Trade Quota:** Monthly trade limit enforcement (JSONB field check)
- **Wallet Balance:** Sufficient funds in correct account (LONG_TERM/SHORT_TERM)
- **Account Allocation:** 70/30 ratio enforcement
- **Wash Trade:** 30-day holding period for LONG_TERM positions
- **Test Mode:** Violations NOT logged to DB in test mode

---

### 4. services/ai_orchestrator.py

**Purpose:** Orchestrates AI agent API calls and statistics

#### READ Operations (4)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 73-91 | ai_agents | agent_id, name, model, api_url, api_key_env, strategy, enabled | WHERE enabled = TRUE ORDER BY (CASE agent_id ...) | `get_enabled_agents()` |
| 206-210 | ai_agents | agent_id, name, model, api_url, api_key_env, enabled | WHERE agent_id = %s | `call_single_agent()` |
| 350-360 | transactions | COUNT(*), COUNT(CASE action = 'BUY'), COUNT(CASE action = 'SELL'), SUM(total_amount) grouped by action | WHERE agent_id = %s | `get_agent_statistics()` |
| 371-373 | wallets | cash_balance | WHERE agent_id = %s | `get_agent_statistics()` |

**Key Patterns:**
- **Agent Ordering:** Custom ORDER BY for consistent execution sequence
- **Statistics Aggregation:** COUNT and SUM aggregates for transaction analysis

---

### 5. workflows/daily_summary.py

**Purpose:** Generates daily performance reviews and P&L summaries

#### READ Operations (2)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 609-611 | ai_agents | initial_capital | WHERE agent_id = %s | `_save_summary()` |
| 663-670 | portfolio_snapshots | total_portfolio_value | WHERE agent_id = %s AND DATE(snapshot_time) = CURRENT_DATE - INTERVAL '1 day' ORDER BY snapshot_time DESC LIMIT 1 | `_get_portfolio_value_yesterday()` |

#### WRITE Operations (1 - UPSERT)

| Line | Table | Fields | Special Logic | Method |
|------|-------|--------|---------------|--------|
| 615-631 | daily_reviews | review_date, agent_id, portfolio_value, daily_pnl, total_pnl, transactions_count, review_content | ON CONFLICT (review_date, agent_id) DO UPDATE SET ... | `_save_summary()` |

**Key Patterns:**
- **UPSERT Logic:** Uses `ON CONFLICT ... DO UPDATE` to overwrite existing daily review
- **Yesterday's Value:** Queries portfolio_snapshots for previous day comparison
- **P&L Calculation:** total_pnl = (portfolio_value - initial_capital)

---

### 6. workflows/trading_decision.py

**Purpose:** Main trading decision workflow, processes AI decisions

#### READ Operations (3)

| Line | Table | Fields | Method | Notes |
|------|-------|--------|--------|-------|
| 278-282 | daily_reviews | review_content | Via `data_collector.get_daily_reviews(agent_id, days=1)` | Gets yesterday's summary |
| 830-833 | transactions | COUNT(*) | WHERE agent_id = %s AND symbol = %s AND action = 'BUY' | Check if first BUY |
| 846-850 | positions | quantity | WHERE agent_id = %s AND symbol = %s | Check position for SELL |

**Key Patterns:**
- **Context Building:** Reads yesterday's review for AI context
- **Event Recording:** Checks transaction history to determine if event is significant

---

### 7. workflows/daily_learning.py

**Purpose:** Learning phase workflow, analyzes news without trading

#### READ Operations (1)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 109-113 | ai_learning_logs | COALESCE(MAX(learning_day), 0) + 1 | WHERE agent_id = %s | `_get_learning_day()` |

#### WRITE Operations (2)

| Line | Table | Fields | Method |
|------|-------|--------|--------|
| 347-356 | ai_learning_logs | agent_id, news_id, analysis, sentiment, predicted_impact, confidence_score, learning_day | `_save_analysis()` |
| 407-416 | hourly_news_analysis | agent_id, news_id, analysis, sentiment, mentioned_stocks (array), impact_prediction, confidence_score | `_save_to_hourly_analysis()` |

#### UPDATE Operations (1)

| Line | Table | Fields | Method | Notes |
|------|-------|--------|--------|-------|
| 380-385 | ai_state | market_view, estimated_tokens | Via `memory_manager.update_ai_state()` | Updates market view from learning |

**Key Patterns:**
- **Learning Day Tracking:** Auto-increments learning_day for sequential tracking
- **Dual Logging:** Saves to both ai_learning_logs and hourly_news_analysis tables

---

### 8. workflows/hourly_news_analysis.py

**Purpose:** Hourly news analysis during trading phase

#### WRITE Operations (1)

| Line | Table | Fields | Method |
|------|-------|--------|--------|
| 425-434 | hourly_news_analysis | agent_id, news_id, analysis, sentiment, mentioned_stocks (array), impact_prediction, confidence_score | `_save_analysis()` |

**Key Patterns:**
- **Array Field:** mentioned_stocks stored as PostgreSQL TEXT[] array type
- **Sentiment Enum:** POSITIVE/NEGATIVE/NEUTRAL/MIXED

---

### 9. workflows/portfolio_snapshot_workflow.py

**Purpose:** Captures portfolio snapshots every 2 hours

#### READ Operations (3)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 64-72 | wallets | cash_balance, long_term_cash, short_term_cash, reserved_cash, total_invested | WHERE agent_id = %s | `calculate_agent_portfolio()` |
| 81-89 | positions | symbol, quantity, average_cost, current_value, position_type | WHERE agent_id = %s AND quantity > 0 | `calculate_agent_portfolio()` |
| 235-239 | ai_agents | agent_id, name | WHERE enabled = true | `run()` |

#### WRITE Operations (1)

| Line | Table | Fields | Method |
|------|-------|--------|--------|
| 165-180 | portfolio_snapshots | agent_id, snapshot_time (CURRENT_TIMESTAMP), cash_balance, long_term_cash, short_term_cash, reserved_cash, long_term_investments, short_term_investments, total_portfolio_value, positions_detail (JSONB), positions_count | `store_snapshot()` |

#### UPDATE Operations (1)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 211-217 | positions | current_value, unrealized_pnl, updated_at | WHERE agent_id = %s AND symbol = %s | `update_positions_current_value()` |

**Key Patterns:**
- **JSONB Position Detail:** Stores full position array as JSONB for historical tracking
- **Scheduled Execution:** Runs every 2 hours via cron schedule
- **Total Value Calculation:** cash_balance + sum(positions.current_value)

---

### 10. workflows/weekly_report.py

**Purpose:** Generates weekly performance reports (Friday 23:00)

#### READ Operations (3)

| Line | Table | Fields | Method | Notes |
|------|-------|--------|--------|-------|
| 140-144 | daily_reviews | review_content | Via `data_collector.get_daily_reviews(agent_id, days=7)` | Last 7 days |
| 144-146 | transactions | All fields | Via `data_collector.get_recent_transactions(agent_id, days=7)` | Last 7 days |
| 147-150 | positions, wallets | Multiple | Via data_collector and memory_manager | Current state |

#### WRITE Operations (1)

| Line | Table | Fields | Method |
|------|-------|--------|--------|
| 474-488 | weekly_reports | agent_id, report_week, starting_capital, ending_capital, weekly_return, total_trades, winning_trades, losing_trades, best_trade (JSONB), worst_trade (JSONB), self_critique, next_week_plan | `_save_report()` |

**Key Patterns:**
- **ISO Week Format:** report_week uses Monday date (e.g., 2025-01-06 for Week 1)
- **JSONB Trade Details:** best_trade and worst_trade stored as JSON objects
- **AI Self-Reflection:** self_critique and next_week_plan text fields

---

### 11. main.py

**Purpose:** Main service entry point, agent coordination

#### READ Operations (1)

| Line | Table | Fields | Filter | Method |
|------|-------|--------|--------|--------|
| 278-279 | ai_agents | agent_id | WHERE enabled = TRUE ORDER BY agent_id | `get_enabled_agents()` |

**Key Patterns:**
- **Agent Discovery:** Loads all enabled agents at startup

---

## Critical Patterns & Best Practices

### 1. Transaction Management

**Atomic Operations:**
```python
# Example from portfolio_executor.py
with get_db_connection() as conn:
    with conn.cursor() as cur:
        # 1. Update position
        cur.execute("UPDATE positions SET ...")
        # 2. Update wallet
        cur.execute("UPDATE wallets SET ...")
        # 3. Record transaction
        cur.execute("INSERT INTO transactions ...")
        conn.commit()  # All or nothing
```

**Critical Files:**
- `services/portfolio_executor.py` - All BUY/SELL operations
- `workflows/portfolio_snapshot_workflow.py` - Multi-table reads

---

### 2. JSONB Field Usage

**Tables with JSONB:**
| Table | Field | Content |
|-------|-------|---------|
| ai_state | portfolio_summary | Portfolio positions summary |
| ai_state | weekly_trade_quota | `{"used": 2, "limit": 5, "week": "2025-W01"}` |
| key_events | context | Event context data |
| transactions | market_context | Market conditions at trade time |
| portfolio_snapshots | positions_detail | Array of position details |
| weekly_reports | best_trade, worst_trade | Trade details |
| compliance_violations | attempted_action | Blocked decision details |

**JSONB Operations:**
```sql
-- Update nested JSONB field (portfolio_executor.py:326-336)
UPDATE ai_state
SET monthly_trade_quota = jsonb_set(
    monthly_trade_quota,
    '{used}',
    to_jsonb((monthly_trade_quota->>'used')::int + 1)
)
WHERE agent_id = %s
```

---

### 3. Optimistic Locking

**Implementation (ai_state table):**
```python
# memory_manager.py:179-183
def update_ai_state(agent_id, updates, current_version=None):
    sql = "UPDATE ai_state SET "
    sql += ", ".join([f"{k} = %s" for k in updates.keys()])
    sql += ", state_version = state_version + 1"
    sql += ", last_updated = CURRENT_TIMESTAMP"
    sql += " WHERE agent_id = %s"
    if current_version:
        sql += " AND state_version = %s"  # Optimistic lock check
```

**Why It Matters:**
- Prevents lost updates in concurrent AI agent operations
- Version mismatch triggers retry logic

---

### 4. Array Field Types

**PostgreSQL Arrays:**
| Table | Field | Type | Example |
|-------|-------|------|---------|
| news_articles | related_stocks | TEXT[] | `{'AAPL', 'MSFT'}` |
| hourly_news_analysis | mentioned_stocks | TEXT[] | `{'TSLA', 'NVDA'}` |
| transactions | news_references | TEXT[] | `{'news_123', 'news_456'}` |

**Query Example:**
```sql
-- Insert array field (hourly_news_analysis.py:425-434)
INSERT INTO hourly_news_analysis (mentioned_stocks, ...)
VALUES (%s, ...)
-- Pass Python list: ['AAPL', 'MSFT']
```

---

### 5. Test Mode Pattern

**Conditional Writes:**
```python
# decision_validator.py:359-380
def _log_violation(self, agent_id, decision, violation_type, reason):
    if self.test_mode:
        logger.info(f"[TEST MODE] Skipping violation log: {violation_type}")
        return

    # Only write to DB in production
    cur.execute("""
        INSERT INTO compliance_violations (...)
        VALUES (...)
    """)
```

**Files with Test Mode:**
- `services/decision_validator.py`
- `workflows/daily_learning.py`
- `workflows/daily_summary.py`

---

### 6. Upsert Pattern

**ON CONFLICT Usage:**
```sql
-- daily_summary.py:615-631
INSERT INTO daily_reviews (
    review_date, agent_id, portfolio_value, daily_pnl, ...
) VALUES (
    %s, %s, %s, %s, ...
)
ON CONFLICT (review_date, agent_id)
DO UPDATE SET
    portfolio_value = EXCLUDED.portfolio_value,
    daily_pnl = EXCLUDED.daily_pnl,
    ...
```

**Prevents Duplicate Reviews:** Ensures only one review per agent per day

---

### 7. Data Cleanup Strategies

**Auto-Pruning (key_events):**
```python
# memory_manager.py:318-327
def _cleanup_old_events(self, agent_id, keep=20):
    cur.execute("""
        DELETE FROM key_events
        WHERE agent_id = %s
        AND id NOT IN (
            SELECT id FROM key_events
            WHERE agent_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        )
    """, (agent_id, agent_id, keep))
```

**Token Budget Management:**
- `key_events.estimated_tokens` - Track memory usage
- Automatic cleanup maintains most recent 20 events per agent

---

### 8. Position Type Enum

**Values:** `LONG_TERM` | `SHORT_TERM`

**Business Rules:**
- **LONG_TERM:** Must hold ≥30 days from first_buy_date
- **SHORT_TERM:** High-frequency trading allowed
- **Account Allocation:** 70% LONG_TERM cash, 30% SHORT_TERM cash

**Validation:**
```python
# decision_validator.py:295-298
# Wash Trade Rule: Cannot sell LONG_TERM position within 30 days
position_age = (datetime.now().date() - position['first_buy_date']).days
if position['position_type'] == 'LONG_TERM' and position_age < 30:
    return False, "WASH_TRADE", "30-day holding period not met"
```

---

### 9. Aggregation Queries

**Transaction Statistics (ai_orchestrator.py:350-360):**
```sql
SELECT
    COUNT(*) as total_trades,
    COUNT(CASE WHEN action = 'BUY' THEN 1 END) as buy_count,
    COUNT(CASE WHEN action = 'SELL' THEN 1 END) as sell_count,
    SUM(CASE WHEN action = 'BUY' THEN total_amount ELSE 0 END) as total_bought,
    SUM(CASE WHEN action = 'SELL' THEN total_amount ELSE 0 END) as total_sold
FROM transactions
WHERE agent_id = %s
```

---

### 10. Snapshot Frequency

**Portfolio Snapshots:**
- **Frequency:** Every 2 hours (cron schedule)
- **Purpose:** Historical portfolio value tracking, P&L calculation
- **Storage:** JSONB positions_detail for complete position history

**Daily Summary Dependency:**
```python
# daily_summary.py:663-670
# Gets yesterday's last snapshot for daily_pnl calculation
SELECT total_portfolio_value
FROM portfolio_snapshots
WHERE agent_id = %s
AND DATE(snapshot_time) = CURRENT_DATE - INTERVAL '1 day'
ORDER BY snapshot_time DESC
LIMIT 1
```

---

## Summary Statistics

### Tables by Access Frequency

| Table | READ | WRITE | UPDATE | DELETE | Total Ops |
|-------|------|-------|--------|--------|-----------|
| positions | 4 | 1 | 3 | 1 | 9 |
| wallets | 5 | 0 | 7 | 0 | 12 |
| ai_state | 4 | 1 | 3 | 0 | 8 |
| transactions | 3 | 1 | 0 | 0 | 4 |
| ai_agents | 4 | 0 | 0 | 0 | 4 |
| key_events | 1 | 1 | 0 | 1 | 3 |
| compliance_violations | 1 | 1 | 0 | 0 | 2 |
| portfolio_snapshots | 1 | 1 | 0 | 0 | 2 |
| daily_reviews | 1 | 1 (upsert) | 0 | 0 | 2 |
| hourly_news_analysis | 0 | 2 | 0 | 0 | 2 |
| ai_learning_logs | 1 | 1 | 0 | 0 | 2 |
| weekly_reports | 0 | 1 | 0 | 0 | 1 |
| stocks | 1 | 0 | 0 | 0 | 1 |

### Top 5 Most Active Files

1. **portfolio_executor.py** - 17 operations (7 UPDATE, 3 READ, 2 WRITE, 1 DELETE)
2. **memory_manager.py** - 8 operations (3 READ, 2 WRITE, 2 UPDATE, 1 DELETE)
3. **decision_validator.py** - 7 operations (6 READ, 1 WRITE)
4. **ai_orchestrator.py** - 4 operations (4 READ)
5. **portfolio_snapshot_workflow.py** - 5 operations (3 READ, 1 WRITE, 1 UPDATE)

---

## Maintenance Recommendations

1. **Index Optimization:**
   - Ensure index on `transactions(agent_id, executed_at)` for statistics queries
   - Index on `portfolio_snapshots(agent_id, snapshot_time)` for daily summary queries
   - Index on `key_events(agent_id, created_at)` for efficient cleanup

2. **JSONB Performance:**
   - Consider GIN index on `ai_state.portfolio_summary` if querying JSONB fields
   - Use `jsonb_set()` for efficient nested updates

3. **Data Retention:**
   - Implement archival strategy for old `portfolio_snapshots` (>90 days)
   - Consider partitioning `transactions` table by date

4. **Monitoring:**
   - Track `state_version` conflicts in ai_state table
   - Monitor transaction rollback rate
   - Alert on compliance_violations accumulation

---

**End of Document**
