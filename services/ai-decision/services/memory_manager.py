"""
Memory Manager Service
Manage AI state, key events, wallets, and other structured memories
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from core import DatabaseManager, create_context_logger

logger = create_context_logger()


class MemoryManager:
    """AI memory management service"""
    
    def __init__(self, db: DatabaseManager):
        """
        Initialize the memory manager
        
        Args:
            db: database manager
        """
        self.db = db
    
    def load_ai_state(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Load AI state
        
        Args:
            agent_id: AI ID
            
        Returns:
            AI state dictionary; returns None if missing
        """
        logger.info(f"Loading AI state for {agent_id}")
        
        query = """
            SELECT
                agent_id,
                portfolio_summary,
                investment_thesis,
                market_view,
                weekly_trade_quota,
                monthly_trade_quota,
                long_term_allocation,
                cash_ratio,
                state_version,
                estimated_tokens,
                last_updated
            FROM ai_state
            WHERE agent_id = %s
        """
        
        try:
            results = self.db.execute_query(query, (agent_id,))
            
            if not results:
                logger.warning(f"AI state not found for {agent_id}, initializing...")
                return self._initialize_ai_state(agent_id)
            
            state = results[0]
            logger.info(f"Loaded AI state (version: {state['state_version']})")
            return state
        
        except Exception as e:
            logger.error(f"Failed to load AI state: {e}")
            return None
    
    def _initialize_ai_state(self, agent_id: str) -> Dict[str, Any]:
        """
        Initialize AI state (first use)
        
        Args:
            agent_id: AI ID
            
        Returns:
            Initialized state dictionary
        """
        logger.info(f"Initializing AI state for {agent_id}")
        
        initial_state = {
            'agent_id': agent_id,
            'portfolio_summary': {},
            'investment_thesis': '',
            'market_view': '',
            'weekly_trade_quota': {'used': 0, 'limit': 5, 'week': None},
            'monthly_trade_quota': {'used': 0, 'limit': 5, 'month': None},
            'long_term_allocation': 0.0,
            'cash_ratio': 100.0,
            'state_version': 1,
            'estimated_tokens': 0
        }
        
        query = """
            INSERT INTO ai_state (
                agent_id,
                portfolio_summary,
                investment_thesis,
                market_view,
                weekly_trade_quota,
                monthly_trade_quota,
                long_term_allocation,
                cash_ratio,
                state_version,
                estimated_tokens
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (agent_id) DO NOTHING
        """

        try:
            self.db.execute_update(
                query,
                (
                    agent_id,
                    '{}',
                    '',
                    '',
                    '{"used": 0, "limit": 5, "week": null}',
                    '{"used": 0, "limit": 5, "month": null}',
                    0.0,
                    100.0,
                    1,
                    0
                )
            )
            logger.info("AI state initialized successfully")
            return initial_state
        
        except Exception as e:
            logger.error(f"Failed to initialize AI state: {e}")
            return initial_state
    
    def update_ai_state(
        self,
        agent_id: str,
        updates: Dict[str, Any],
        current_version: Optional[int] = None
    ) -> bool:
        """
        Update AI state (optimistic locking)
        
        Args:
            agent_id: AI ID
            updates: fields to update {'portfolio_summary': {...}, ...}
            current_version: current version (for optimistic locking, optional)
            
        Returns:
            True if the update succeeds
        """
        logger.info(f"Updating AI state for {agent_id}")
        
        # Build SET clause
        set_clauses = []
        params = []
        
        for key, value in updates.items():
            if key in ['portfolio_summary', 'weekly_trade_quota', 'monthly_trade_quota']:
                # JSONB fields
                import json
                set_clauses.append(f"{key} = %s::jsonb")
                params.append(json.dumps(value))
            else:
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        # Increment version
        set_clauses.append("state_version = state_version + 1")
        set_clauses.append("last_updated = CURRENT_TIMESTAMP")
        
        # WHERE condition
        where_clause = "agent_id = %s"
        params.append(agent_id)
        
        # Optimistic locking
        if current_version is not None:
            where_clause += " AND state_version = %s"
            params.append(current_version)
        
        query = f"""
            UPDATE ai_state
            SET {', '.join(set_clauses)}
            WHERE {where_clause}
        """
        
        try:
            rowcount = self.db.execute_update(query, tuple(params))
            
            if rowcount == 0:
                if current_version is not None:
                    logger.warning(f"Version conflict detected for {agent_id}")
                else:
                    logger.warning(f"AI state not found for {agent_id}")
                return False
            
            logger.info("AI state updated successfully")
            return True
        
        except Exception as e:
            logger.error(f"Failed to update AI state: {e}")
            return False
    
    def get_key_events(
        self,
        agent_id: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get key events (latest N entries)
        
        Args:
            agent_id: AI ID
            limit: number of events to return
            
        Returns:
            List of key events
        """
        logger.info(f"Loading key events for {agent_id}")
        
        query = """
            SELECT 
                event_type,
                symbol,
                event_date,
                description,
                context,
                impact,
                estimated_tokens,
                created_at
            FROM key_events
            WHERE agent_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        
        try:
            results = self.db.execute_query(query, (agent_id, limit))
            logger.info(f"Loaded {len(results)} key events")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to get key events: {e}")
            return []
    
    def append_key_event(
        self,
        agent_id: str,
        event_type: str,
        symbol: Optional[str],
        description: str,
        context: Optional[Dict[str, Any]] = None,
        impact: Optional[str] = None,
        estimated_tokens: int = 100
    ) -> bool:
        """
        Append a key event
        
        Args:
            agent_id: AI ID
            event_type: event type (FIRST_BUY/CLEAR_POSITION/LARGE_TRADE/STRATEGY_CHANGE)
            symbol: stock symbol
            description: event description
            context: contextual information (optional)
            impact: impact description (optional)
            estimated_tokens: estimated token count
            
        Returns:
            True if the event is appended successfully
        """
        logger.info(f"Appending key event for {agent_id}: {event_type}")
        
        import json
        
        query = """
            INSERT INTO key_events (
                agent_id,
                event_type,
                symbol,
                event_date,
                description,
                context,
                impact,
                estimated_tokens
            ) VALUES (%s, %s, %s, CURRENT_DATE, %s, %s::jsonb, %s, %s)
        """
        
        try:
            self.db.execute_update(
                query,
                (
                    agent_id,
                    event_type,
                    symbol,
                    description,
                    json.dumps(context or {}),
                    impact,
                    estimated_tokens
                )
            )
            
            # Keep the most recent 20 events
            self._cleanup_old_events(agent_id, keep=20)
            
            logger.info("Key event appended successfully")
            return True
        
        except Exception as e:
            logger.error(f"Failed to append key event: {e}")
            return False
    
    def _cleanup_old_events(self, agent_id: str, keep: int = 20):
        """
        Clean up old events, keeping the latest N
        
        Args:
            agent_id: AI ID
            keep: number of events to keep
        """
        query = """
            DELETE FROM key_events
            WHERE agent_id = %s
              AND id NOT IN (
                  SELECT id FROM key_events
                  WHERE agent_id = %s
                  ORDER BY created_at DESC
                  LIMIT %s
              )
        """
        
        try:
            rowcount = self.db.execute_update(query, (agent_id, agent_id, keep))
            if rowcount > 0:
                logger.info(f"Cleaned up {rowcount} old events")
        
        except Exception as e:
            logger.error(f"Failed to cleanup old events: {e}")
    
    def get_wallet(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Get wallet state
        
        Args:
            agent_id: AI ID
            
        Returns:
            Wallet information dictionary
        """
        logger.info(f"Loading wallet for {agent_id}")
        
        query = """
            SELECT 
                cash_balance,
                long_term_cash,
                short_term_cash,
                reserved_cash,
                total_invested,
                total_withdrawn,
                last_transaction_at,
                updated_at
            FROM wallets
            WHERE agent_id = %s
        """
        
        try:
            results = self.db.execute_query(query, (agent_id,))
            
            if not results:
                logger.warning(f"Wallet not found for {agent_id}")
                return None
            
            wallet = results[0]
            logger.info(f"Loaded wallet (balance: ${wallet['cash_balance']})")
            return wallet
        
        except Exception as e:
            logger.error(f"Failed to get wallet: {e}")
            return None
    
    def update_wallet(
        self,
        agent_id: str,
        cash_change: float = 0.0,
        long_term_change: float = 0.0,
        short_term_change: float = 0.0
    ) -> bool:
        """
        Update wallet balances
        
        Args:
            agent_id: AI ID
            cash_change: total cash change (can be negative)
            long_term_change: change to the long-term account
            short_term_change: change to the short-term account
            
        Returns:
            True if the update succeeds
        """
        logger.info(f"Updating wallet for {agent_id}")
        
        query = """
            UPDATE wallets
            SET 
                cash_balance = cash_balance + %s,
                long_term_cash = long_term_cash + %s,
                short_term_cash = short_term_cash + %s,
                last_transaction_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE agent_id = %s
        """
        
        try:
            rowcount = self.db.execute_update(
                query,
                (cash_change, long_term_change, short_term_change, agent_id)
            )
            
            if rowcount == 0:
                logger.warning(f"Wallet not found for {agent_id}")
                return False
            
            logger.info("Wallet updated successfully")
            return True
        
        except Exception as e:
            logger.error(f"Failed to update wallet: {e}")
            return False
    
    def get_weekly_trade_quota(self, agent_id: str) -> Dict[str, Any]:
        """
        Get weekly trade quota
        
        Args:
            agent_id: AI ID
            
        Returns:
            {'used': int, 'limit': int, 'week': str}
        """
        state = self.load_ai_state(agent_id)
        
        if not state:
            return {'used': 0, 'limit': 5, 'week': None}
        
        return state.get('weekly_trade_quota', {'used': 0, 'limit': 5, 'week': None})
    
    def reset_weekly_trade_quota(self, agent_id: str, week: str) -> bool:
        """
        Reset weekly trade quota (run early Monday)

        Args:
            agent_id: AI ID
            week: week identifier (e.g., "2025-W01")

        Returns:
            True if the reset succeeds
        """
        logger.info(f"Resetting weekly trade quota for {agent_id} (week: {week})")

        new_quota = {
            'used': 0,
            'limit': 5,
            'week': week
        }

        return self.update_ai_state(agent_id, {'weekly_trade_quota': new_quota})

    def get_monthly_trade_quota(self, agent_id: str) -> Dict[str, Any]:
        """
        Get monthly trade quota

        Args:
            agent_id: AI ID

        Returns:
            {'used': int, 'limit': int, 'month': str}
        """
        state = self.load_ai_state(agent_id)

        if not state:
            return {'used': 0, 'limit': 5, 'month': None}

        return state.get('monthly_trade_quota', {'used': 0, 'limit': 5, 'month': None})

    def reset_monthly_trade_quota(self, agent_id: str, month: str) -> bool:
        """
        Reset monthly trade quota (run on the 1st of each month)

        Args:
            agent_id: AI ID
            month: month identifier (e.g., "2025-01")

        Returns:
            True if the reset succeeds
        """
        logger.info(f"Resetting monthly trade quota for {agent_id} (month: {month})")

        new_quota = {
            'used': 0,
            'limit': 5,
            'month': month
        }

        return self.update_ai_state(agent_id, {'monthly_trade_quota': new_quota})
