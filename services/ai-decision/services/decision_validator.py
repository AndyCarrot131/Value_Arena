"""
Decision Validator Service
Validate trading decisions against 6 investment rules
"""

from typing import Dict, Any, Tuple, List, Optional
from datetime import datetime, date
from core import DatabaseManager, create_context_logger
from utils import get_et_today

logger = create_context_logger()


class DecisionValidator:
    """Trading decision validator"""

    def __init__(self, db: DatabaseManager, test_mode: bool = False):
        """
        Initialize the validator

        Args:
            db: database manager
            test_mode: test mode (skip database writes)
        """
        self.db = db
        self.test_mode = test_mode
    
    def validate_decision(
        self,
        agent_id: str,
        decision: Dict[str, Any]
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Validate a trading decision (6 rules)
        
        Args:
            agent_id: AI ID
            decision: decision dictionary
                - decision_type: BUY/SELL
                - symbol: stock symbol
                - quantity: quantity
                - price: price
                - position_type: LONG_TERM/SHORT_TERM
                
        Returns:
            (is_valid, violation_type, reason)
            - is_valid: True if validation passes
            - violation_type: violation type (e.g., INVALID_STOCK)
            - reason: violation reason
        """
        logger.info(
            f"Validating decision for {agent_id}",
            extra={'details': {
                'decision_type': decision.get('decision_type'),
                'symbol': decision.get('symbol'),
                'position_type': decision.get('position_type')
            }}
        )
        
        # Rule 1: stock pool check
        result = self._validate_stock_pool(decision)
        if not result[0]:
            self._log_violation(agent_id, decision, result[1], result[2])
            return result
        
        # Rule 2: trade count check
        result = self._validate_trade_quota(agent_id, decision)
        if not result[0]:
            self._log_violation(agent_id, decision, result[1], result[2])
            return result
        
        # Rule 3: wallet balance check
        result = self._validate_wallet_balance(agent_id, decision)
        if not result[0]:
            self._log_violation(agent_id, decision, result[1], result[2])
            return result
        
        # Rule 4: wash trade check (SELL only)
        if decision.get('decision_type') == 'SELL':
            result = self._validate_wash_trade(agent_id, decision)
            if not result[0]:
                self._log_violation(agent_id, decision, result[1], result[2])
                return result
        
        logger.info("Decision validation passed")
        return (True, None, None)
    
    def _validate_stock_pool(self, decision: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Rule 1: stocks/ETFs must be in the allowed trading pool

        Args:
            decision: decision dictionary

        Returns:
            (is_valid, violation_type, reason)
        """
        symbol = decision.get('symbol')

        if not symbol:
            return (False, 'MISSING_SYMBOL', 'Missing stock symbol')

        query = """
            SELECT symbol, type FROM stocks
            WHERE symbol = %s AND enabled = TRUE AND type IN ('stock', 'etf')
        """

        try:
            results = self.db.execute_query(query, (symbol,))

            if not results:
                return (False, 'INVALID_STOCK', f'{symbol} is not in the allowed trading pool (only enabled stocks and ETFs are tradable)')

            return (True, None, None)

        except Exception as e:
            logger.error(f"Failed to validate stock pool: {e}")
            return (False, 'VALIDATION_ERROR', f'Validation failed: {e}')
    
    def _validate_trade_quota(self, agent_id: str, decision: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Rule 2: monthly trades < 5

        Args:
            agent_id: AI ID
            decision: decision dictionary

        Returns:
            (is_valid, violation_type, reason)
        """
        query = """
            SELECT monthly_trade_quota FROM ai_state
            WHERE agent_id = %s
        """

        try:
            results = self.db.execute_query(query, (agent_id,))

            if not results:
                return (False, 'STATE_NOT_FOUND', f'AI state not found: {agent_id}')

            quota = results[0]['monthly_trade_quota']
            used = quota.get('used', 0)
            limit = quota.get('limit', 5)

            if used >= limit:
                return (False, 'TRADE_QUOTA_EXCEEDED', f'Monthly trade quota reached ({used}/{limit})')

            return (True, None, None)

        except Exception as e:
            logger.error(f"Failed to validate trade quota: {e}")
            return (False, 'VALIDATION_ERROR', f'Validation failed: {e}')
    
    def _validate_wallet_balance(self, agent_id: str, decision: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Rule 3: check wallet balance based on position_type
        
        Args:
            agent_id: AI ID
            decision: decision dictionary
            
        Returns:
            (is_valid, violation_type, reason)
        """
        decision_type = decision.get('decision_type')
        
        # Only check BUY actions
        if decision_type != 'BUY':
            return (True, None, None)
        
        position_type = decision.get('position_type')
        quantity = decision.get('quantity', 0)
        price = decision.get('price', 0.0)
        total_amount = quantity * price
        
        if not position_type:
            return (False, 'MISSING_POSITION_TYPE', 'Missing account type (LONG_TERM/SHORT_TERM)')
        
        # Query wallet
        query = """
            SELECT long_term_cash, short_term_cash FROM wallets
            WHERE agent_id = %s
        """
        
        try:
            results = self.db.execute_query(query, (agent_id,))
            
            if not results:
                return (False, 'WALLET_NOT_FOUND', f'Wallet not found: {agent_id}')
            
            wallet = results[0]
            long_term_cash = float(wallet['long_term_cash'])
            short_term_cash = float(wallet['short_term_cash'])
            
            # Check balance for the corresponding account
            if position_type == 'LONG_TERM':
                if total_amount > long_term_cash:
                    return (False, 'INSUFFICIENT_LONG_TERM_CASH', 
                            f'Insufficient long-term balance: need ${total_amount:.2f}, available ${long_term_cash:.2f}')
            
            elif position_type == 'SHORT_TERM':
                if total_amount > short_term_cash:
                    return (False, 'INSUFFICIENT_SHORT_TERM_CASH', 
                            f'Insufficient short-term balance: need ${total_amount:.2f}, available ${short_term_cash:.2f}')
            
            else:
                return (False, 'INVALID_POSITION_TYPE', f'Invalid account type: {position_type}')
            
            return (True, None, None)

        except Exception as e:
            logger.error(f"Failed to validate wallet balance: {e}")
            return (False, 'VALIDATION_ERROR', f'Validation failed: {e}')

    def _validate_wash_trade(self, agent_id: str, decision: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Rule 5: wash trade check (long-term account cannot sell within 30 days of the first buy)
        
        Args:
            agent_id: AI ID
            decision: decision dictionary
            
        Returns:
            (is_valid, violation_type, reason)
        """
        symbol = decision.get('symbol')
        
        # Query positions
        query = """
            SELECT position_type, first_buy_date FROM positions
            WHERE agent_id = %s AND symbol = %s
        """
        
        try:
            results = self.db.execute_query(query, (agent_id, symbol))
            
            if not results:
                return (False, 'POSITION_NOT_FOUND', f'Position not found: {symbol}')
            
            position = results[0]
            position_type = position['position_type']
            first_buy_date = position['first_buy_date']
            
            # Only check long-term accounts
            if position_type == 'LONG_TERM':
                if not first_buy_date:
                    return (False, 'MISSING_FIRST_BUY_DATE', f'Missing first buy date: {symbol}')
                
                # Calculate holding days (use ET timezone)
                if isinstance(first_buy_date, str):
                    first_buy_date = datetime.strptime(first_buy_date, '%Y-%m-%d').date()

                holding_days = (get_et_today() - first_buy_date).days
                
                if holding_days < 30:
                    return (False, 'WASH_TRADE_VIOLATION', 
                            f'Long-term holding period is under 30 days: {symbol} (held {holding_days} days)')
            
            return (True, None, None)
        
        except Exception as e:
            logger.error(f"Failed to validate wash trade: {e}")
            return (False, 'VALIDATION_ERROR', f'Validation failed: {e}')
    
    def _log_violation(
        self,
        agent_id: str,
        decision: Dict[str, Any],
        violation_type: str,
        reason: str
    ):
        """
        Log a violation to the database

        Args:
            agent_id: AI ID
            decision: decision dictionary
            violation_type: violation type
            reason: violation reason
        """
        logger.warning(
            f"Compliance violation detected: {violation_type}",
            extra={'details': {'agent_id': agent_id, 'reason': reason}}
        )

        # TEST MODE: Skip database write
        if self.test_mode:
            logger.info("TEST MODE: Skipping violation logging to database")
            return

        import json

        query = """
            INSERT INTO compliance_violations (
                agent_id,
                violation_type,
                attempted_action,
                detection_method,
                severity,
                notes
            ) VALUES (%s, %s, %s::jsonb, %s, %s, %s)
        """

        try:
            self.db.execute_update(
                query,
                (
                    agent_id,
                    violation_type,
                    json.dumps(decision),
                    'PRE_EXECUTION_CHECK',
                    'blocked',
                    reason
                )
            )

        except Exception as e:
            logger.error(f"Failed to log violation: {e}")
    
    def get_recent_violations(
        self,
        agent_id: str,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Get recent violation records
        
        Args:
            agent_id: AI ID
            days: last N days
            
        Returns:
            List of violation records
        """
        query = """
            SELECT 
                violation_type,
                attempted_action,
                detection_method,
                severity,
                notes,
                detected_at
            FROM compliance_violations
            WHERE agent_id = %s
              AND detected_at > NOW() - INTERVAL '%s days'
            ORDER BY detected_at DESC
        """
        
        try:
            results = self.db.execute_query(query, (agent_id, days))
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to get violations: {e}")
            return []
