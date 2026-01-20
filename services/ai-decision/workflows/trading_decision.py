"""
Trading Decision Workflow
Weekday trading decisions: Retrieve RAG, generate decision, validate rules, execute trade
"""

from typing import Dict, Any, Optional, List
import json
import uuid
from services import (
    DataCollector,
    MemoryManager,
    RAGRetriever,
    DecisionValidator,
    PortfolioExecutor,
    AIOrchestrator
)
from core import BedrockClient, OpenSearchClient, DatabaseManager, create_context_logger
from utils import is_trading_day, TokenRecorder, get_et_today

logger = create_context_logger()


class TradingDecisionWorkflow:
    """Trading Decision Workflow"""

    def __init__(
        self,
        data_collector: DataCollector,
        memory_manager: MemoryManager,
        rag_retriever: RAGRetriever,
        decision_validator: DecisionValidator,
        portfolio_executor: PortfolioExecutor,
        ai_orchestrator: AIOrchestrator,
        bedrock_client: BedrockClient,
        opensearch_client: OpenSearchClient,
        db: DatabaseManager,
        test_mode: bool = False
    ):
        """
        Initialize workflow

        Args:
            data_collector: Data collector
            memory_manager: Memory manager
            rag_retriever: RAG retriever
            decision_validator: Decision validator
            portfolio_executor: Portfolio executor
            ai_orchestrator: AI orchestrator
            bedrock_client: Bedrock client
            opensearch_client: OpenSearch client
            db: Database manager for token recording
            test_mode: Test mode flag (no DB writes, verbose logging)
        """
        self.data_collector = data_collector
        self.memory_manager = memory_manager
        self.rag_retriever = rag_retriever
        self.validator = decision_validator
        self.executor = portfolio_executor
        self.ai_orchestrator = ai_orchestrator
        self.bedrock = bedrock_client
        self.opensearch = opensearch_client
        self.db = db
        self.token_recorder = TokenRecorder(db)
        self.test_mode = test_mode

        if self.test_mode:
            logger.info("=" * 80)
            logger.info("RUNNING IN TEST MODE - No database writes will occur")
            logger.info("=" * 80)
    
    def run(self, agent_id: str) -> bool:
        """
        Execute trading decision workflow
        
        Args:
            agent_id: AI agent ID
            
        Returns:
            True if execution successful
        """
        logger.info(
            f"Starting trading decision workflow for {agent_id}",
            extra={'details': {'workflow': 'trading_decision', 'agent_id': agent_id}}
        )
        
        try:
            # 0. Check if trading day (skip check in test mode) - Use ET timezone
            if not self.test_mode and not is_trading_day(get_et_today()):
                logger.info("Not a trading day, skipping")
                return True

            if self.test_mode and not is_trading_day(get_et_today()):
                logger.warning("TEST MODE: Not a trading day, but continuing workflow for testing purposes")

            # 0.5. Check and reset monthly quota if needed (at start of new month)
            self._check_and_reset_monthly_quota(agent_id)

            # 1. Collect data
            logger.info("Step 1: Collecting market data")
            data = self._collect_market_data(agent_id)
            
            if not data:
                logger.error("Failed to collect market data")
                return False
            
            # 2. Check monthly trade quota
            monthly_quota = data['monthly_quota']
            if monthly_quota['used'] >= monthly_quota['limit']:
                logger.info(f"Monthly trade quota exhausted ({monthly_quota['used']}/{monthly_quota['limit']})")
                return True
            
            # 3. Retrieve RAG (similar historical decisions)
            logger.info("Step 2: Retrieving similar trading decisions")
            similar_decisions = self._retrieve_similar_decisions(agent_id, data)
            
            # 4. AI generates trading decision with retry logic for validation failures
            logger.info("Step 3: Generating trading decision")

            max_attempts = 3
            decision = None
            validation_failures = []
            is_valid = False

            for attempt in range(1, max_attempts + 1):
                logger.info(f"Decision generation attempt {attempt}/{max_attempts}")

                # Generate decision
                decision = self._generate_decision(agent_id, data, similar_decisions, attempt, validation_failures)

                if decision == "PARSE_ERROR":
                    logger.warning("Decision JSON parsing failed; retrying workflow on next attempt")
                    continue

                if not decision:
                    logger.info("AI decided to HOLD, no trade execution")
                    return True

                # 5. Validate decision (6 rules)
                logger.info(f"Step 4: Validating decision (attempt {attempt})")

                # Set validator test_mode to match workflow test_mode
                original_test_mode = self.validator.test_mode
                self.validator.test_mode = self.test_mode

                is_valid, violation_type, reason = self.validator.validate_decision(agent_id, decision)

                # Restore original test_mode
                self.validator.test_mode = original_test_mode

                if is_valid:
                    logger.info(f"Decision validation passed on attempt {attempt}")
                    break  # Validation successful, proceed
                else:
                    logger.warning(
                        f"Decision validation failed on attempt {attempt}/{max_attempts}: {violation_type}",
                        extra={'details': {
                            'reason': reason,
                            'rule_violated': violation_type,
                            'decision': decision
                        }}
                    )

                    # Record this failure
                    validation_failures.append({
                        'attempt': attempt,
                        'violation_type': violation_type,
                        'reason': reason
                    })

                    # If this is not the last attempt, retry with validation feedback
                    if attempt < max_attempts:
                        logger.info(f"Retrying decision generation with validation feedback...")
                    else:
                        # Last attempt failed, do not write to DB/RAG
                        logger.error(
                            f"All {max_attempts} attempts failed validation. Decision rejected.",
                            extra={'details': {
                                'final_violation': violation_type,
                                'final_reason': reason,
                                'all_failures': validation_failures
                            }}
                        )
                        return True  # Validation failure doesn't fail workflow

            if not is_valid:
                # All attempts exhausted without success
                return True
            
            # 6. Execute trade
            if self.test_mode:
                logger.info("TEST MODE: Skipping trade execution")
                logger.info(f"Would execute: {decision['decision_type']} {decision.get('quantity', 0)} shares of {decision.get('symbol', 'N/A')} at ${decision.get('price', 0):.2f}")
            else:
                logger.info("Step 5: Executing trade")
                success = self.executor.execute_trade(agent_id, decision)

                if not success:
                    logger.error("Trade execution failed")
                    return False

            # 7. Update position values
            if self.test_mode:
                logger.info("TEST MODE: Skipping position value update")
            else:
                logger.info("Step 6: Updating position values")
                self.executor.update_position_values(agent_id, data['prices'])

            # 8. Record key event
            if self.test_mode:
                logger.info("TEST MODE: Skipping key event recording")
            else:
                self._record_key_event(agent_id, decision)

            # 9. Write to RAG
            if self.test_mode:
                logger.info("TEST MODE: Skipping RAG write")
                logger.info(f"Would write decision to RAG: {decision.get('reasoning', '')[:100]}...")
            else:
                logger.info("Step 7: Writing decision to RAG")
                self._write_to_rag(agent_id, decision, data)
            
            logger.info(
                f"Trading decision completed for {agent_id}",
                extra={'details': {
                    'decision_type': decision['decision_type'],
                    'symbol': decision['symbol']
                }}
            )
            
            return True
        
        except Exception as e:
            logger.error(
                f"Trading decision workflow failed: {e}",
                extra={'details': {'agent_id': agent_id}}
            )
            return False

    def _check_and_reset_monthly_quota(self, agent_id: str):
        """
        Check if we're in a new month and reset monthly quota if needed

        Args:
            agent_id: AI agent ID
        """
        try:
            # Get current monthly quota
            current_quota = self.memory_manager.get_monthly_trade_quota(agent_id)
            current_month_in_quota = current_quota.get('month')

            # Get current month (ET timezone)
            today = get_et_today()
            current_month = f"{today.year}-{today.month:02d}"

            # Check if we need to reset (new month or first time)
            if current_month_in_quota != current_month:
                logger.info(f"New month detected: {current_month} (was: {current_month_in_quota})")

                # Reset monthly quota
                success = self.memory_manager.reset_monthly_trade_quota(agent_id, current_month)

                if success:
                    logger.info(f"Monthly trade quota reset for month {current_month}")
                else:
                    logger.warning("Failed to reset monthly trade quota")

        except Exception as e:
            logger.error(f"Failed to check/reset monthly quota: {e}")

    def _collect_market_data(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Collect market data

        Args:
            agent_id: AI agent ID

        Returns:
            Data dictionary
        """
        try:
            # Yesterday's daily summary
            daily_reviews = self.data_collector.get_daily_reviews(agent_id, days=1)
            yesterday_summary = daily_reviews[0] if daily_reviews else None

            # Stock/ETF list and 48-hour price data from DynamoDB
            stocks = self.data_collector.get_stock_list(enabled_only=True, stock_type='stock')
            etfs = self.data_collector.get_stock_list(enabled_only=True, stock_type='etf')
            all_symbols = [s['symbol'] for s in (stocks + etfs)]

            # Get 48-hour price changes from DynamoDB (stocks + ETFs)
            price_data_48h = self.data_collector.get_price_changes_48h(all_symbols)

            # Extract current prices for backward compatibility
            prices = {
                symbol: data.get('current_price')
                for symbol, data in price_data_48h.items()
                if data.get('current_price') is not None
            }

            # RAG: recent daily summaries (past 3 days) for all stocks/ETFs
            rag_daily_days = 3
            rag_daily_summaries = {}
            for symbol in all_symbols:
                try:
                    rag_daily_summaries[symbol] = self.rag_retriever.retrieve_recent_stock_daily_summaries(
                        agent_id=agent_id,
                        symbol=symbol,
                        days=rag_daily_days,
                        num_results=5
                    )
                except Exception as e:
                    logger.warning(f"Failed to retrieve RAG daily summaries for {symbol}: {e}")
                    rag_daily_summaries[symbol] = []

            # Current positions
            positions = self.data_collector.get_positions(agent_id)

            # Wallet status
            wallet = self.memory_manager.get_wallet(agent_id)

            # Monthly trade quota
            monthly_quota = self.memory_manager.get_monthly_trade_quota(agent_id)

            # AI state
            ai_state = self.memory_manager.load_ai_state(agent_id)

            # Key events
            key_events = self.memory_manager.get_key_events(agent_id, limit=20)

            # Recent news (24 hours)
            news = self.data_collector.collect_news(hours=24)

            # Market environment (inferred from market indices in DynamoDB)
            market_env = self._infer_market_environment(news, prices)

            return {
                'yesterday_summary': yesterday_summary,
                'stocks': stocks,
                'etfs': etfs,
                'prices': prices,
                'price_data_48h': price_data_48h,  # 48-hour detailed price data
                'positions': positions,
                'wallet': wallet,
                'monthly_quota': monthly_quota,
                'ai_state': ai_state,
                'key_events': key_events,
                'news': news,
                'market_environment': market_env,
                'rag_daily_summaries': rag_daily_summaries,
                'rag_daily_days': rag_daily_days
            }

        except Exception as e:
            logger.error(f"Failed to collect market data: {e}")
            return None
    
    def _infer_market_environment(self, news: list, prices: dict) -> Dict[str, str]:
        """
        Infer market environment from market indices (DynamoDB data)

        Args:
            news: News list
            prices: Price dictionary

        Returns:
            Market environment dictionary
        """
        try:
            # Get market indices from database
            market_indices = self.data_collector.get_market_indices()

            # Get 48-hour price data for market indices
            indices_data = self.data_collector.get_price_changes_48h(market_indices)

            # Analyze S&P 500 trend
            sp500_trend = 'NEUTRAL'
            sp500_data = indices_data.get('^GSPC', {})
            if sp500_data and 'change_pct' in sp500_data:
                change_pct = sp500_data['change_pct']
                if change_pct > 2.0:
                    sp500_trend = 'STRONG_BULL'
                elif change_pct > 0.5:
                    sp500_trend = 'BULL'
                elif change_pct < -2.0:
                    sp500_trend = 'STRONG_BEAR'
                elif change_pct < -0.5:
                    sp500_trend = 'BEAR'

            # Analyze VIX level
            vix_level = 'MEDIUM'
            vix_data = indices_data.get('^VIX', {})
            if vix_data and 'current_price' in vix_data:
                vix_price = vix_data['current_price']
                if vix_price > 30:
                    vix_level = 'HIGH'
                elif vix_price > 20:
                    vix_level = 'MEDIUM_HIGH'
                elif vix_price < 12:
                    vix_level = 'LOW'
                elif vix_price < 15:
                    vix_level = 'MEDIUM_LOW'

            # Determine sector rotation by comparing index performances
            sector_rotation = 'Balanced'
            nasdaq_data = indices_data.get('^IXIC', {})
            dji_data = indices_data.get('^DJI', {})

            if nasdaq_data and dji_data:
                nasdaq_change = nasdaq_data.get('change_pct', 0)
                dji_change = dji_data.get('change_pct', 0)

                if nasdaq_change > dji_change + 1.0:
                    sector_rotation = 'Technology/Growth'
                elif dji_change > nasdaq_change + 1.0:
                    sector_rotation = 'Value/Traditional'

            # Small cap vs large cap
            market_breadth = 'Neutral'
            rut_data = indices_data.get('^RUT', {})  # Russell 2000
            if rut_data and sp500_data:
                rut_change = rut_data.get('change_pct', 0)
                sp500_change = sp500_data.get('change_pct', 0)

                if rut_change > sp500_change + 1.0:
                    market_breadth = 'Small Cap Leading'
                elif sp500_change > rut_change + 1.0:
                    market_breadth = 'Large Cap Leading'

            # Gold performance (risk-on vs risk-off)
            gold_data = indices_data.get('GLD', {})
            risk_sentiment = 'Neutral'
            if gold_data:
                gold_change = gold_data.get('change_pct', 0)
                if gold_change > 2.0:
                    risk_sentiment = 'Risk-Off'
                elif gold_change < -2.0:
                    risk_sentiment = 'Risk-On'

            logger.info(
                f"Market environment inferred from indices",
                extra={'details': {
                    'sp500_trend': sp500_trend,
                    'vix_level': vix_level,
                    'sector_rotation': sector_rotation,
                    'market_breadth': market_breadth,
                    'risk_sentiment': risk_sentiment,
                    'indices_analyzed': len(indices_data)
                }}
            )

            return {
                'sp500_trend': sp500_trend,
                'vix_level': vix_level,
                'sector_rotation': sector_rotation,
                'market_breadth': market_breadth,
                'risk_sentiment': risk_sentiment,
                'indices_data': indices_data  # Include raw data for detailed analysis
            }

        except Exception as e:
            logger.error(f"Failed to infer market environment: {e}")
            # Fallback to neutral values
            return {
                'sp500_trend': 'NEUTRAL',
                'vix_level': 'MEDIUM',
                'sector_rotation': 'Balanced',
                'market_breadth': 'Neutral',
                'risk_sentiment': 'Neutral'
            }
    
    def _retrieve_similar_decisions(
        self,
        agent_id: str,
        data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Retrieve similar historical trading decisions
        
        Args:
            agent_id: AI agent ID
            data: Market data
            
        Returns:
            Similar decisions list
        """
        try:
            # Build news summary
            recent_news_summary = "\n".join([
                f"- {n['title']}" for n in data['news'][:5]
            ])
            
            # Retrieve similar decisions for each stock (top 3 stocks)
            all_similar = []
            
            for stock in data['stocks'][:3]:
                symbol = stock['symbol']
                
                results = self.rag_retriever.retrieve_for_trading_decision(
                    agent_id=agent_id,
                    symbol=symbol,
                    portfolio=data['positions'],
                    market_environment=data['market_environment'],
                    recent_news=recent_news_summary,
                    num_results=5
                )
                
                all_similar.extend(results)
            
            # Deduplicate and sort by similarity
            unique_decisions = {}
            for result in all_similar:
                decision_id = result.get('metadata', {}).get('decision_id')
                if decision_id and decision_id not in unique_decisions:
                    unique_decisions[decision_id] = result
            
            sorted_decisions = sorted(
                unique_decisions.values(),
                key=lambda x: x['score'],
                reverse=True
            )
            
            return sorted_decisions[:10]
        
        except Exception as e:
            logger.error(f"Failed to retrieve similar decisions: {e}")
            return []
    
    def _generate_decision(
        self,
        agent_id: str,
        data: Dict[str, Any],
        similar_decisions: List[Dict[str, Any]],
        attempt: int = 1,
        validation_failures: List[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        AI generates trading decision

        Args:
            agent_id: AI agent ID
            data: Market data
            similar_decisions: Similar historical decisions
            attempt: Current attempt number (1-3)
            validation_failures: List of previous validation failures

        Returns:
            Decision dictionary (None if HOLD)
        """
        if validation_failures is None:
            validation_failures = []

        # Build prompt with validation failure feedback if this is a retry
        prompt = self._build_decision_prompt(data, similar_decisions, validation_failures)

        messages = [
            {
                "role": "system",
                "content": self._get_system_prompt()
            },
            {
                "role": "user",
                "content": prompt
            }
        ]

        # TEST MODE: Print full prompt
        if self.test_mode:
            logger.info("=" * 80)
            logger.info("TEST MODE: SYSTEM PROMPT")
            logger.info("=" * 80)
            logger.info(self._get_system_prompt())
            logger.info("=" * 80)
            logger.info("TEST MODE: USER PROMPT")
            logger.info("=" * 80)
            logger.info(prompt)
            logger.info("=" * 80)

        # Call AI
        result = self.ai_orchestrator.call_single_agent(
            agent_id=agent_id,
            messages=messages,
            temperature=0.7
        )

        # Record token usage regardless of success (if we got a response)
        if result and result.get('usage'):
            self.token_recorder.record_from_usage(
                agent_id=agent_id,
                service='trading_decision',
                usage=result['usage']
            )

        if not result or not result['success']:
            logger.error(f"AI call failed: {result.get('error') if result else 'Unknown error'}")
            return None

        # Parse decision
        response_text = result.get('response') or ""

        # TEST MODE: Print AI response
        if self.test_mode:
            logger.info("=" * 80)
            logger.info("TEST MODE: AI RESPONSE")
            logger.info("=" * 80)
            logger.info(response_text)
            logger.info("=" * 80)

        decision = self.ai_orchestrator.parse_json_response(response_text)

        if not decision:
            logger.warning(
                "Failed to parse decision JSON; retrying with stricter JSON instruction",
                extra={'details': {'response_preview': (response_text or '')[:500]}}
            )

            retry_messages = messages + [
                {
                    "role": "user",
                    "content": "The previous reply was not valid JSON. Respond again with ONLY a JSON object matching the required decision fields. Do not include code fences, markdown, or any explanatory text."
                }
            ]

            retry_result = self.ai_orchestrator.call_single_agent(
                agent_id=agent_id,
                messages=retry_messages,
                temperature=0.7
            )

            # Record token usage for retry call
            if retry_result and retry_result.get('usage'):
                self.token_recorder.record_from_usage(
                    agent_id=agent_id,
                    service='trading_decision',
                    usage=retry_result['usage']
                )

            if not retry_result or not retry_result['success']:
                logger.error(f"AI retry call failed: {retry_result.get('error') if retry_result else 'Unknown error'}")
                return "PARSE_ERROR"

            response_text = retry_result.get('response') or ""

            if self.test_mode:
                logger.info("=" * 80)
                logger.info("TEST MODE: AI RESPONSE (RETRY)")
                logger.info("=" * 80)
                logger.info(response_text)
                logger.info("=" * 80)

            decision = self.ai_orchestrator.parse_json_response(response_text)

            if not decision:
                logger.warning(
                    "Failed to parse decision JSON after retry",
                    extra={'details': {'response_preview': (response_text or '')[:500]}}
                )
                return "PARSE_ERROR"

        # Check decision type
        decision_type = decision.get('decision_type', 'HOLD')

        if decision_type == 'HOLD':
            logger.info("AI decided to HOLD")
            return None

        # Add decision_id
        if 'decision_id' not in decision:
            decision['decision_id'] = str(uuid.uuid4())

        return decision
    
    def _get_system_prompt(self) -> str:
        """
        Get system prompt
        
        Returns:
            System prompt text
        """
        return """You are a professional value investor focused on long-term holdings of quality assets.

Investment Rules (must strictly follow):
1. Trading Pool: Trade from designated stocks and ETFs pool
   - Can trade enabled stocks (type='stock') and ETFs (type='etf')
   - All symbols must be from the approved trading pool
2. Trading Frequency Limit:
   - Monthly: Maximum 5 trades per month (all trades cumulative)
   - Both long-term and short-term accounts share this quota
   - One trading decision per day
3. Dual Account System:
   - Long-term Account (70% funds): Expected hold 1-10 years, minimum 30-day holding period
   - Short-term Account (30% funds): Can trade quickly, buy today sell tomorrow
4. Allow Cash Position: If no suitable opportunities, holding cash is a good choice
5. Prohibited: Leverage, options, short selling

Decision Principles:
- Focus on long-term value, not chasing trends
- Learn from similar historical cases
- Stay rational, avoid emotional decisions
- Every trade must have solid reasoning

Response requirements:
- Respond with ONLY a raw JSON object. Do not include code fences, markdown, or any extra prose before or after the JSON.
- Required fields: decision_type (BUY/SELL/HOLD), symbol, quantity, price, position_type (LONG_TERM/SHORT_TERM), reasoning.
- If no suitable opportunity, return {"decision_type": "HOLD", "reasoning": "<brief reason>"}."""
    
    def _build_decision_prompt(
        self,
        data: Dict[str, Any],
        similar_decisions: List[Dict[str, Any]],
        validation_failures: List[Dict[str, Any]] = None
    ) -> str:
        """
        Build decision prompt
        
        Args:
            data: Market data
            similar_decisions: Similar historical decisions
            
        Returns:
            Prompt text
        """
        parts = ["# Trading Decision Task", ""]

        # Monthly trade quota
        monthly_quota = data['monthly_quota']
        parts.append("## Monthly Trade Quota")
        parts.append(f"- Used: {monthly_quota['used']}/{monthly_quota['limit']} trades this month")
        parts.append("")
        
        # Wallet status
        wallet = data['wallet']
        parts.append("## Wallet Status")
        parts.append(f"- Total Cash: ${float(wallet['cash_balance']):.2f}")
        parts.append(f"- Long-term Account: ${float(wallet['long_term_cash']):.2f} (70%)")
        parts.append(f"- Short-term Account: ${float(wallet['short_term_cash']):.2f} (30%)")
        parts.append("")
        
        # Current positions
        if data['positions']:
            parts.append("## Current Positions")
            for pos in data['positions']:
                parts.append(f"- {pos['symbol']}: {pos['quantity']} shares ({pos['position_type']})")
                parts.append(f"  Cost Basis: ${float(pos['average_cost']):.2f}")
                parts.append(f"  Unrealized P&L: ${float(pos['unrealized_pnl']):.2f}")
                if pos['first_buy_date']:
                    parts.append(f"  First Buy Date: {pos['first_buy_date']}")
            parts.append("")
        
        # Stock pool with 48-hour price changes
        parts.append("## Stock Pool (stocks/ETFs) - 48 Hour Performance")
        price_data_48h = data.get('price_data_48h', {})

        for stock in data['stocks'] + data.get('etfs', []):
            symbol = stock['symbol']
            price_info = price_data_48h.get(symbol, {})

            if price_info and 'current_price' in price_info:
                current_price = price_info['current_price']
                change_pct = price_info.get('change_pct', 0)
                high_48h = price_info.get('high_48h', current_price)
                low_48h = price_info.get('low_48h', current_price)
                volatility = price_info.get('volatility', 0)

                change_indicator = "↑" if change_pct > 0 else "↓" if change_pct < 0 else "→"

                parts.append(
                    f"- {symbol} ({stock['name']}): ${current_price:.2f} "
                    f"{change_indicator} {change_pct:+.2f}% (48h) | "
                    f"Range: ${low_48h:.2f}-${high_48h:.2f} | "
                    f"Volatility: {volatility:.2f}%"
                )
            else:
                current_price = data['prices'].get(symbol, 'N/A')
                if current_price != 'N/A':
                    parts.append(f"- {symbol} ({stock['name']}): ${current_price:.2f} (limited data)")
                else:
                    parts.append(f"- {symbol} ({stock['name']}): N/A")

        parts.append("")

        # Market indices performance
        market_env = data.get('market_environment', {})
        indices_data = market_env.get('indices_data', {})

        if indices_data:
            parts.append("## Market Indices (48 Hour Performance)")
            for symbol, idx_data in indices_data.items():
                if 'current_price' in idx_data:
                    current_price = idx_data['current_price']
                    change_pct = idx_data.get('change_pct', 0)
                    change_indicator = "↑" if change_pct > 0 else "↓" if change_pct < 0 else "→"

                    parts.append(
                        f"- {symbol}: ${current_price:.2f} "
                        f"{change_indicator} {change_pct:+.2f}% (48h)"
                    )
            parts.append("")

        # Market environment summary
        if market_env:
            parts.append("## Market Environment Analysis")
            parts.append(f"- S&P 500 Trend: {market_env.get('sp500_trend', 'UNKNOWN')}")
            parts.append(f"- VIX Level: {market_env.get('vix_level', 'UNKNOWN')}")
            parts.append(f"- Sector Rotation: {market_env.get('sector_rotation', 'UNKNOWN')}")
            parts.append(f"- Market Breadth: {market_env.get('market_breadth', 'UNKNOWN')}")
            parts.append(f"- Risk Sentiment: {market_env.get('risk_sentiment', 'UNKNOWN')}")
            parts.append("")

        # Yesterday's summary
        if data['yesterday_summary']:
            summary = data['yesterday_summary']
            parts.append("## Yesterday's Market Summary")
            parts.append(summary['review_content'][:500] + "...")
            parts.append("")

        # Response format reminder
        parts.append("## Response Format")
        parts.append(
            "Return ONLY a JSON object with fields: decision_type (BUY/SELL/HOLD), symbol, quantity, price, "
            "position_type (LONG_TERM/SHORT_TERM), reasoning. Do not wrap in code fences or add any text before or after."
        )
        parts.append("If no trade is warranted, return {\"decision_type\": \"HOLD\", \"reasoning\": \"<brief reason>\"}.")

        # RAG: recent daily stock/ETF summaries (last 3 days)
        rag_daily_summaries = data.get('rag_daily_summaries', {})
        rag_daily_days = data.get('rag_daily_days', 3)
        if rag_daily_summaries:
            parts.append(f"## RAG Daily Summaries (last {rag_daily_days} days)")
            shown_symbols = 0
            for symbol, summaries in rag_daily_summaries.items():
                if not summaries:
                    continue
                shown_symbols += 1
                if shown_symbols > 10:  # avoid token bloat
                    break
                parts.append(f"- {symbol}:")
                for mem in summaries[:2]:  # limit per symbol
                    meta = mem.get('metadata', {})
                    parts.append(f"  ({meta.get('date', 'Unknown')}) {mem.get('content', '')[:180]}")
            parts.append("")

        # Recent news (top 3)
        if data['news']:
            parts.append("## Recent News")
            for i, article in enumerate(data['news'][:3], 1):
                parts.append(f"{i}. {article['title']}")
            parts.append("")
        
        # RAG retrieval results
        if similar_decisions:
            parts.append("## Similar Historical Decisions")
            formatted_cases = self.rag_retriever.format_results_for_prompt(
                similar_decisions,
                max_cases=5
            )
            parts.append(formatted_cases)
            parts.append("")
        
        # Validation failures feedback (if retry)
        if validation_failures and len(validation_failures) > 0:
            parts.append("## ⚠️ IMPORTANT: Previous Validation Failures")
            parts.append("")
            parts.append(f"Your previous {len(validation_failures)} decision(s) failed validation:")
            parts.append("")

            for i, failure in enumerate(validation_failures, 1):
                parts.append(f"### Attempt {failure['attempt']} - Rule Violated: {failure['violation_type']}")
                parts.append(f"**Reason**: {failure['reason']}")
                parts.append("")

            parts.append("Please generate a NEW decision that addresses these validation failures:")
            parts.append("")

            # Add specific guidance based on violation types
            violation_types = [f['violation_type'] for f in validation_failures]

            if 'INSUFFICIENT_LONG_TERM_CASH' in violation_types or 'INSUFFICIENT_SHORT_TERM_CASH' in violation_types:
                parts.append("- **INSUFFICIENT_CASH**: Reduce the quantity or choose a different account type that has sufficient funds.")
                parts.append("")

            if 'WASH_TRADE_VIOLATION' in violation_types:
                parts.append("- **WASH_TRADE_VIOLATION**: Long-term positions must be held for at least 30 days before selling.")
                parts.append("")

            if 'TRADE_QUOTA_EXCEEDED' in violation_types:
                parts.append("- **TRADE_QUOTA_EXCEEDED**: You have used all 5 trades for this week. Consider HOLD.")
                parts.append("")

            if 'INVALID_STOCK' in violation_types:
                parts.append("- **INVALID_STOCK**: Only trade stocks from the 20-stock pool listed above.")
                parts.append("")

        # Task instructions
        parts.append("## Please Make Trading Decision")
        parts.append("")
        parts.append("Respond in JSON format:")
        parts.append("```json")
        parts.append("{")
        parts.append('  "decision_type": "BUY",  // BUY/SELL/HOLD')
        parts.append('  "symbol": "NVDA",')
        parts.append('  "quantity": 10,')
        parts.append('  "price": 450.23,  // Use current price')
        parts.append('  "position_type": "LONG_TERM",  // LONG_TERM/SHORT_TERM')
        parts.append('  "reasoning": "Detailed reasoning for this decision...",')
        parts.append('  "confidence": 0.85,')
        parts.append('  "market_context": {')
        parts.append('    "sp500_trend": "BULL",')
        parts.append('    "vix_level": "MEDIUM"')
        parts.append('  }')
        parts.append("}")
        parts.append("```")
        parts.append("")
        parts.append("If no suitable opportunity, return:")
        parts.append('```json\n{"decision_type": "HOLD"}\n```')

        return "\n".join(parts)
    
    def _record_key_event(self, agent_id: str, decision: Dict[str, Any]):
        """
        Record key event
        
        Args:
            agent_id: AI agent ID
            decision: Decision dictionary
        """
        try:
            symbol = decision['symbol']
            decision_type = decision['decision_type']
            quantity = decision['quantity']

            # Determine event type
            event_type = None
            description = None
            
            if decision_type == 'BUY':
                # Check if first buy or adding to position
                query = """
                    SELECT COUNT(*) as count FROM transactions
                    WHERE agent_id = %s AND symbol = %s AND action = 'BUY'
                """
                results = self.data_collector.db.execute_query(query, (agent_id, symbol))

                if results and results[0]['count'] == 0:
                    event_type = 'first_buy'
                    description = f"First buy {symbol}: {quantity} shares @ ${decision['price']:.2f}"
                else:
                    event_type = 'add_position'
                    description = f"Added to {symbol}: {quantity} shares @ ${decision['price']:.2f}"

            elif decision_type == 'SELL':
                # Check if clearing position
                query = """
                    SELECT quantity FROM positions
                    WHERE agent_id = %s AND symbol = %s
                """
                results = self.data_collector.db.execute_query(query, (agent_id, symbol))

                if not results or results[0]['quantity'] == 0:
                    event_type = 'liquidation'
                    description = f"Cleared position {symbol}: {quantity} shares @ ${decision['price']:.2f}"
            
            # Record event
            if event_type:
                self.memory_manager.append_key_event(
                    agent_id=agent_id,
                    event_type=event_type,
                    symbol=symbol,
                    description=description,
                    context=decision.get('market_context', {}),
                    impact=decision.get('reasoning', '')[:200]
                )
        
        except Exception as e:
            logger.warning(f"Failed to record key event: {e}")
    
    def _write_to_rag(
        self,
        agent_id: str,
        decision: Dict[str, Any],
        data: Dict[str, Any]
    ):
        """
        Write decision to RAG
        
        Args:
            agent_id: AI agent ID
            decision: Decision dictionary
            data: Market data
        """
        try:
            # Generate embedding
            reasoning = decision.get('reasoning', '')
            embedding = self.bedrock.generate_embedding(reasoning)
            
            # Write to OpenSearch
            self.opensearch.index_decision(
                decision_id=decision['decision_id'],
                agent_id=agent_id,
                decision_embedding=embedding,
                reasoning=reasoning,
                decision_type=decision['decision_type'],
                symbol=decision['symbol'],
                quality_weight=0.5,  # Evaluate after 30 days
                metadata={
                    'agent_id': agent_id,
                    'type': 'trading_decision',
                    'position_type': decision.get('position_type'),
                    'quantity': decision.get('quantity'),
                    'price': decision.get('price'),
                    'confidence': decision.get('confidence', 0.5),
                    **decision.get('market_context', {})
                }
            )
            
            logger.info("Decision written to RAG knowledge base")
        
        except Exception as e:
            logger.warning(f"Failed to write to RAG: {e}")
