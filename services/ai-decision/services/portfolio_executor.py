"""
Portfolio Executor Service
Execute trades atomically and update positions, wallets, and transaction records
"""

from typing import Dict, Any, Optional
from datetime import datetime, date
import uuid
from core import DatabaseManager, create_context_logger
from utils import get_et_today

logger = create_context_logger()


class PortfolioExecutor:
    """Portfolio executor"""
    
    def __init__(self, db: DatabaseManager):
        """
        Initialize the executor
        
        Args:
            db: database manager
        """
        self.db = db
    
    def execute_trade(
        self,
        agent_id: str,
        decision: Dict[str, Any]
    ) -> bool:
        """
        Execute a trade atomically
        
        Args:
            agent_id: AI ID
            decision: decision dictionary
                - decision_id: UUID (optional)
                - decision_type: BUY/SELL
                - symbol: stock symbol
                - quantity: quantity
                - price: price
                - position_type: LONG_TERM/SHORT_TERM
                - reasoning: decision rationale (optional)
                - market_context: market context (optional)
                
        Returns:
            True if the trade succeeds
        """
        logger.info(
            f"Executing trade for {agent_id}",
            extra={'details': {
                'decision_type': decision.get('decision_type'),
                'symbol': decision.get('symbol'),
                'quantity': decision.get('quantity'),
                'position_type': decision.get('position_type')
            }}
        )
        
        decision_type = decision.get('decision_type')
        
        try:
            with self.db.transaction() as cur:
                if decision_type == 'BUY':
                    success = self._execute_buy(cur, agent_id, decision)
                elif decision_type == 'SELL':
                    success = self._execute_sell(cur, agent_id, decision)
                else:
                    logger.error(f"Invalid decision type: {decision_type}")
                    return False
                
                if not success:
                    raise Exception("Trade execution failed")
                
                # Update trade count
                self._update_trade_quota(cur, agent_id)
                
                logger.info("Trade executed successfully")
                return True
        
        except Exception as e:
            logger.error(f"Failed to execute trade: {e}")
            return False
    
    def _execute_buy(self, cur, agent_id: str, decision: Dict[str, Any]) -> bool:
        """
        Execute a buy operation
        
        Args:
            cur: database cursor
            agent_id: AI ID
            decision: decision dictionary
            
        Returns:
            True if the operation succeeds
        """
        symbol = decision['symbol']
        quantity = decision['quantity']
        price = decision['price']
        position_type = decision['position_type']
        total_amount = quantity * price
        
        # 1. Check if a position already exists
        cur.execute("""
            SELECT quantity, average_cost, position_type, first_buy_date
            FROM positions
            WHERE agent_id = %s AND symbol = %s
        """, (agent_id, symbol))
        
        existing_position = cur.fetchone()
        
        if existing_position:
            # Update position (add to existing)
            old_quantity = existing_position['quantity']
            old_avg_cost = float(existing_position['average_cost'])
            old_position_type = existing_position['position_type']
            first_buy_date = existing_position['first_buy_date']
            
            # Ensure position_type matches
            if old_position_type != position_type:
                logger.error(f"Position type mismatch: {old_position_type} vs {position_type}")
                return False
            
            new_quantity = old_quantity + quantity
            new_avg_cost = (old_quantity * old_avg_cost + total_amount) / new_quantity
            
            cur.execute("""
                UPDATE positions
                SET 
                    quantity = %s,
                    average_cost = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE agent_id = %s AND symbol = %s
            """, (new_quantity, new_avg_cost, agent_id, symbol))
        
        else:
            # Create new position (use ET timezone for first_buy_date)
            first_buy_date = get_et_today()

            cur.execute("""
                INSERT INTO positions (
                    agent_id,
                    symbol,
                    quantity,
                    average_cost,
                    position_type,
                    first_buy_date
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (agent_id, symbol, quantity, price, position_type, first_buy_date))
        
        # 2. Update wallet (deduct cash from the corresponding account)
        if position_type == 'LONG_TERM':
            cur.execute("""
                UPDATE wallets
                SET 
                    cash_balance = cash_balance - %s,
                    long_term_cash = long_term_cash - %s,
                    total_invested = total_invested + %s,
                    last_transaction_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE agent_id = %s
            """, (total_amount, total_amount, total_amount, agent_id))
        
        else:  # SHORT_TERM
            cur.execute("""
                UPDATE wallets
                SET 
                    cash_balance = cash_balance - %s,
                    short_term_cash = short_term_cash - %s,
                    total_invested = total_invested + %s,
                    last_transaction_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE agent_id = %s
            """, (total_amount, total_amount, total_amount, agent_id))
        
        # 3. Record the transaction
        self._record_transaction(cur, agent_id, decision, 'BUY', total_amount)
        
        return True
    
    def _execute_sell(self, cur, agent_id: str, decision: Dict[str, Any]) -> bool:
        """
        Execute a sell operation
        
        Args:
            cur: database cursor
            agent_id: AI ID
            decision: decision dictionary
            
        Returns:
            True if the operation succeeds
        """
        symbol = decision['symbol']
        quantity = decision['quantity']
        price = decision['price']
        total_amount = quantity * price
        
        # 1. Query current position
        cur.execute("""
            SELECT quantity, average_cost, position_type
            FROM positions
            WHERE agent_id = %s AND symbol = %s
        """, (agent_id, symbol))
        
        position = cur.fetchone()
        
        if not position:
            logger.error(f"Position not found: {symbol}")
            return False
        
        current_quantity = position['quantity']
        position_type = position['position_type']
        
        if quantity > current_quantity:
            logger.error(f"Insufficient quantity: trying to sell {quantity}, have {current_quantity}")
            return False
        
        # 2. Update position
        new_quantity = current_quantity - quantity
        
        if new_quantity > 0:
            # Partial sell
            cur.execute("""
                UPDATE positions
                SET 
                    quantity = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE agent_id = %s AND symbol = %s
            """, (new_quantity, agent_id, symbol))
        
        else:
            # Sell all shares and delete the position
            cur.execute("""
                DELETE FROM positions
                WHERE agent_id = %s AND symbol = %s
            """, (agent_id, symbol))
        
        # 3. Update wallet (add cash to the corresponding account)
        if position_type == 'LONG_TERM':
            cur.execute("""
                UPDATE wallets
                SET 
                    cash_balance = cash_balance + %s,
                    long_term_cash = long_term_cash + %s,
                    total_withdrawn = total_withdrawn + %s,
                    last_transaction_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE agent_id = %s
            """, (total_amount, total_amount, total_amount, agent_id))
        
        else:  # SHORT_TERM
            cur.execute("""
                UPDATE wallets
                SET 
                    cash_balance = cash_balance + %s,
                    short_term_cash = short_term_cash + %s,
                    total_withdrawn = total_withdrawn + %s,
                    last_transaction_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE agent_id = %s
            """, (total_amount, total_amount, total_amount, agent_id))
        
        # 4. Record the transaction
        decision_with_type = {**decision, 'position_type': position_type}
        self._record_transaction(cur, agent_id, decision_with_type, 'SELL', total_amount)
        
        return True
    
    def _record_transaction(
        self,
        cur,
        agent_id: str,
        decision: Dict[str, Any],
        action: str,
        total_amount: float
    ):
        """
        Record a transaction to the transactions table
        
        Args:
            cur: database cursor
            agent_id: AI ID
            decision: decision dictionary
            action: BUY/SELL
            total_amount: total transaction amount
        """
        import json
        
        decision_id = decision.get('decision_id')
        if not decision_id:
            decision_id = str(uuid.uuid4())
        
        cur.execute("""
            INSERT INTO transactions (
                agent_id,
                symbol,
                action,
                quantity,
                price,
                total_amount,
                reason,
                position_type,
                decision_id,
                market_context
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """, (
            agent_id,
            decision['symbol'],
            action,
            decision['quantity'],
            decision['price'],
            total_amount,
            decision.get('reasoning', ''),
            decision.get('position_type'),
            decision_id,
            json.dumps(decision.get('market_context', {}))
        ))
    
    def _update_trade_quota(self, cur, agent_id: str):
        """
        Update monthly trade count

        Args:
            cur: database cursor
            agent_id: AI ID
        """
        cur.execute("""
            UPDATE ai_state
            SET
                monthly_trade_quota = jsonb_set(
                    monthly_trade_quota,
                    '{used}',
                    (COALESCE((monthly_trade_quota->>'used')::int, 0) + 1)::text::jsonb
                ),
                last_updated = CURRENT_TIMESTAMP
            WHERE agent_id = %s
        """, (agent_id,))
    
    def update_position_values(
        self,
        agent_id: str,
        current_prices: Dict[str, float]
    ) -> bool:
        """
        Update position market values (periodic call)
        
        Args:
            agent_id: AI ID
            current_prices: dictionary of {symbol: price}
            
        Returns:
            True if the update succeeds
        """
        logger.info(f"Updating position values for {agent_id}")
        
        query = """
            SELECT symbol, quantity, average_cost FROM positions
            WHERE agent_id = %s
        """
        
        try:
            positions = self.db.execute_query(query, (agent_id,))
            
            if not positions:
                return True
            
            for position in positions:
                symbol = position['symbol']
                quantity = position['quantity']
                average_cost = float(position['average_cost'])
                
                # Get the current price
                current_price = current_prices.get(symbol)
                
                if current_price is None:
                    logger.warning(f"Price not found for {symbol}, skipping")
                    continue
                
                # Calculate market value and PnL
                current_value = quantity * current_price
                unrealized_pnl = current_value - (quantity * average_cost)
                
                # Update the position record
                update_query = """
                    UPDATE positions
                    SET 
                        current_value = %s,
                        unrealized_pnl = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE agent_id = %s AND symbol = %s
                """
                
                self.db.execute_update(
                    update_query,
                    (current_value, unrealized_pnl, agent_id, symbol)
                )
            
            logger.info("Position values updated successfully")
            return True
        
        except Exception as e:
            logger.error(f"Failed to update position values: {e}")
            return False
