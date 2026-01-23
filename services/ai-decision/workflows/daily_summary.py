"""
Daily Summary Workflow
Summarize daily market dynamics, retrieve RAG context, and write to OpenSearch.
"""

from typing import Dict, Any, Optional, List
import json
import uuid
from services import DataCollector, MemoryManager, RAGRetriever, AIOrchestrator
from core import BedrockClient, OpenSearchClient, DatabaseManager, create_context_logger
from utils import TokenRecorder, get_et_today

logger = create_context_logger()


class DailySummaryWorkflow:
    """Nightly Daily Summary Workflow"""

    def __init__(
        self,
        data_collector: DataCollector,
        memory_manager: MemoryManager,
        rag_retriever: RAGRetriever,
        ai_orchestrator: AIOrchestrator,
        bedrock_client: BedrockClient,
        opensearch_client: OpenSearchClient,
        db: DatabaseManager,
        test_mode: bool = False
    ):
        """
        Initialize the workflow

        Args:
            data_collector: Data Collector service
            memory_manager: Memory Manager service
            rag_retriever: RAG Retriever service
            ai_orchestrator: AI Orchestrator service
            bedrock_client: Bedrock Client
            opensearch_client: OpenSearch Client
            db: Database Manager for token recording
            test_mode: If True, skip database and RAG writes
        """
        self.data_collector = data_collector
        self.memory_manager = memory_manager
        self.rag_retriever = rag_retriever
        self.ai_orchestrator = ai_orchestrator
        self.bedrock = bedrock_client
        self.opensearch = opensearch_client
        self.db = db
        self.token_recorder = TokenRecorder(db)
        self.test_mode = test_mode
    
    def run(self, agent_id: str) -> bool:
        """
        Execute the nightly daily summary

        Args:
            agent_id: AI ID

        Returns:
            True if execution is successful
        """
        logger.info(
            f"Starting daily summary for {agent_id} (test_mode={self.test_mode})",
            extra={'details': {'workflow': 'daily_summary', 'agent_id': agent_id, 'test_mode': self.test_mode}}
        )

        try:
            # 1. Collect today's data
            logger.info("Step 1: Collecting today's data")
            data = self._collect_today_data(agent_id)

            if not data:
                logger.error("Failed to collect data")
                return False

            # 2. Retrieve RAG (similar historical daily summaries)
            logger.info("Step 2: Retrieving similar historical summaries")
            similar_summaries = self._retrieve_similar_summaries(agent_id, data)

            # 3. AI generates daily summary
            logger.info("Step 3: Generating daily summary")
            summary = self._generate_summary(agent_id, data, similar_summaries)

            if not summary:
                logger.error("Failed to generate summary")
                return False

            if self.test_mode:
                logger.info("TEST MODE: Skipping database and RAG writes")
                logger.info(f"Generated summary length: {len(summary)} characters")
                logger.info(f"Summary preview: {summary[:200]}...")

                # Generate stock summaries in test mode (but don't save)
                logger.info("TEST MODE: Generating per-stock analysis (no writes)")
                stock_summaries = self._generate_stock_summaries(agent_id, data)
                if stock_summaries:
                    logger.info(f"TEST MODE: Generated {len(stock_summaries)} stock analyses")
                    for s in stock_summaries[:3]:  # Preview first 3
                        logger.info(f"  {s['symbol']}: {s['content'][:100]}...")

                logger.info("Daily summary completed (test mode - no writes)")
                return True

            # 4. Save to database
            logger.info("Step 4: Saving to database")
            success = self._save_summary(agent_id, summary, data)

            if not success:
                logger.error("Failed to save summary")
                return False

            # 5. Generate embedding and write to OpenSearch
            logger.info("Step 5: Writing to OpenSearch")
            success = self._write_to_rag(agent_id, summary)

            if not success:
                logger.warning("Failed to write to RAG (non-critical)")

            # 6. Generate per-stock analysis
            logger.info("Step 6: Generating per-stock analysis")
            stock_summaries = self._generate_stock_summaries(agent_id, data)

            if stock_summaries:
                # 7. Save stock summaries to database
                logger.info("Step 7: Saving stock summaries to database")
                self._save_stock_summaries(agent_id, stock_summaries)

                # 8. Index stock summaries to RAG
                logger.info("Step 8: Indexing stock summaries to RAG")
                self._index_stock_summaries_to_rag(agent_id, stock_summaries)

            logger.info(
                f"Daily summary completed for {agent_id}",
                extra={'details': {'date': str(get_et_today()), 'stock_summaries_count': len(stock_summaries)}}
            )

            return True

        except Exception as e:
            logger.error(
                f"Daily summary workflow failed: {e}",
                extra={'details': {'agent_id': agent_id}}
            )
            return False
    
    def _collect_today_data(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Collect data for the current day

        Args:
            agent_id: AI ID

        Returns:
            Data dictionary
        """
        try:
            # CHANGE: Reduce news window from 24h to 12h
            news = self.data_collector.collect_news(hours=12)
            news_analysis = self.data_collector.get_hourly_news_analysis(agent_id, hours=12)

            # Existing data collection
            positions = self.data_collector.get_positions(agent_id)
            wallet = self.memory_manager.get_wallet(agent_id)
            transactions = self.data_collector.get_recent_transactions(agent_id, days=1)
            decision_history = self.data_collector.get_recent_transactions(agent_id, days=5, limit=50)
            portfolio_value = self.data_collector.calculate_portfolio_value(agent_id)

            # NEW: Get all stock symbols (including ETFs and indices)
            all_stocks = self.data_collector.get_stock_list(enabled_only=True)
            stock_symbols = [s['symbol'] for s in all_stocks]

            # NEW: Get market indices
            market_indices = self.data_collector.get_market_indices()
            all_symbols = stock_symbols + market_indices

            # NEW: Get 48h price changes from DynamoDB
            logger.info(f"Fetching 48h price data for {len(all_symbols)} symbols")
            price_changes_48h = self.data_collector.get_price_changes_48h(all_symbols)

            # NEW: Get recent earnings reports (last 7 days for "new" flag)
            recent_earnings = self.data_collector.get_recent_earnings_reports(
                symbols=stock_symbols,
                days=7
            )

            # NEW: Get LATEST earnings for ALL stocks (even if months old)
            all_earnings = self.data_collector.get_latest_earnings_reports(
                symbols=stock_symbols,
                limit_per_symbol=1
            )

            # ENHANCED: Better market environment inference
            market_env = self._infer_market_environment(
                news=news,
                price_changes=price_changes_48h,
                market_indices=market_indices
            )

            return {
                'news': news,
                'news_analysis': news_analysis,
                'positions': positions,
                'wallet': wallet,
                'transactions': transactions,
                'decision_history': decision_history,
                'portfolio_value': portfolio_value,
                'price_changes_48h': price_changes_48h,
                'recent_earnings': recent_earnings,  # New reports in last 7 days
                'all_earnings': all_earnings,        # Latest report per symbol (any date)
                'market_environment': market_env,
                'market_indices': market_indices
            }

        except Exception as e:
            logger.error(f"Failed to collect today's data: {e}")
            return None
    
    def _infer_market_environment(
        self,
        news: list,
        price_changes: Dict[str, Dict],
        market_indices: List[str]
    ) -> Dict[str, Any]:
        """
        Infer market environment from news + price action

        Args:
            news: List of news items
            price_changes: 48h price changes dict
            market_indices: List of index symbols

        Returns:
            Market environment dictionary with trends
        """
        # Calculate index performance (handle None values)
        # Try ^GSPC (S&P 500 Index) first, fallback to SPY (ETF) or VOO (ETF)
        sp500_data = price_changes.get('^GSPC') or price_changes.get('SPY') or price_changes.get('VOO') or {}
        sp500_change = sp500_data.get('change_pct', 0.0) if sp500_data else 0.0

        # Try ^VIX (VIX Index) first, fallback to VXX (ETF)
        vix_data = price_changes.get('^VIX') or price_changes.get('VIX') or {}
        vix_price = vix_data.get('current_price') if vix_data else None

        # Use fallback of 15.0 if VIX data is missing
        vix_price_safe = vix_price if vix_price is not None else 15.0

        # Classify macro news
        macro_news = [n for n in news if n.get('classification') == 'macro']

        return {
            'sp500_trend': 'BULL' if sp500_change > 1.0 else 'BEAR' if sp500_change < -1.0 else 'SIDEWAYS',
            'sp500_change_48h': sp500_change,
            'vix_level': 'HIGH' if vix_price_safe > 25 else 'MEDIUM' if vix_price_safe > 15 else 'LOW',
            'vix_price': vix_price_safe,
            'macro_news_count': len(macro_news),
            'market_indices': {idx: price_changes.get(idx, {}) for idx in market_indices}
        }
    
    def _retrieve_similar_summaries(
        self,
        agent_id: str,
        data: Dict[str, Any]
    ) -> list:
        """
        Retrieve similar historical daily summaries
        
        Args:
            agent_id: AI ID
            data: Today's data
            
        Returns:
            List of similar summaries
        """
        try:
            # Construct brief summary
            today_brief = f"""
Today's Market Environment: {data['market_environment']['sp500_trend']}, VIX: {data['market_environment']['vix_level']}
News Count: {len(data['news'])}
Transaction Count: {len(data['transactions'])}
Position Count: {len(data['positions'])}
"""
            
            # Retrieval
            results = self.rag_retriever.retrieve_for_daily_summary(
                agent_id=agent_id,
                today_summary=today_brief,
                market_environment=data['market_environment'],
                num_results=5
            )
            
            logger.info(f"Retrieved {len(results)} similar summaries")
            return results
        
        except Exception as e:
            logger.error(f"Failed to retrieve similar summaries: {e}")
            return []
    
    def _generate_summary(
        self,
        agent_id: str,
        data: Dict[str, Any],
        similar_summaries: list,
        max_retries: int = 2
    ) -> Optional[str]:
        """
        AI generates the daily summary with retry logic

        Args:
            agent_id: AI ID
            data: Today's data
            similar_summaries: Similar historical summaries
            max_retries: Maximum retry attempts for format validation failures

        Returns:
            Summary text
        """
        # Build Prompt
        prompt = self._build_summary_prompt(data, similar_summaries)

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

        for attempt in range(max_retries + 1):
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
                    service='daily_summary',
                    usage=result['usage']
                )

            if not result or not result['success']:
                logger.error(f"AI call failed: {result.get('error') if result else 'Unknown error'}")
                return None

            response = result['response']

            # Validate response format
            validation_error = self._validate_summary_format(response)

            if validation_error is None:
                # Format is valid
                return response

            # Format validation failed
            logger.warning(
                f"Summary format validation failed (attempt {attempt + 1}/{max_retries + 1}): {validation_error}"
            )

            if attempt < max_retries:
                # Add retry feedback to messages
                logger.info(f"Retrying summary generation with format feedback...")
                messages = messages + [
                    {
                        "role": "assistant",
                        "content": response
                    },
                    {
                        "role": "user",
                        "content": f"""Your response format was incorrect. Issue: {validation_error}

Please regenerate the summary following the correct format:
- Use clear section headings with ## (e.g., ## Daily Overview, ## Macro Summary, ## Stock Analysis, ## Portfolio Performance, ## Tomorrow's Outlook)
- Write in natural language paragraphs
- Include all 5 required sections
- Output PLAIN TEXT only (no JSON)

Please try again."""
                    }
                ]
            else:
                # Max retries reached, return the response anyway (better than nothing)
                logger.warning(f"Max retries reached for summary generation, using last response despite format issues")
                return response

        return None

    def _validate_summary_format(self, response: str) -> Optional[str]:
        """
        Validate that the summary response has the expected format

        Args:
            response: AI response text

        Returns:
            None if valid, error message string if invalid
        """
        if not response or len(response.strip()) < 100:
            return "Response is too short (less than 100 characters)"

        # Check for required section headers
        required_sections = [
            ('Daily Overview', ['## Daily Overview', '## daily overview', '**Daily Overview**']),
            ('Macro Summary', ['## Macro Summary', '## Macro', '## macro', '**Macro Summary**']),
            ('Stock Analysis', ['## Stock Analysis', '## Stock', '## stock', '**Stock Analysis**']),
            ('Portfolio', ['## Portfolio', '## portfolio', '**Portfolio**']),
            ('Outlook', ['## Outlook', "## Tomorrow", '## outlook', '**Outlook**', "**Tomorrow's Outlook**"])
        ]

        missing_sections = []
        for section_name, patterns in required_sections:
            found = any(pattern.lower() in response.lower() for pattern in patterns)
            if not found:
                missing_sections.append(section_name)

        if missing_sections:
            return f"Missing required sections: {', '.join(missing_sections)}"

        return None
    
    def _get_system_prompt(self) -> str:
        """
        Get system Prompt

        Returns:
            System Prompt
        """
        return """You are a professional portfolio manager conducting your end-of-day market review.

Your tasks:
1. **Daily Overview**: Summarize the top 3 most impactful news events from the last 12 hours. Explain how they affect the market and your portfolio.

2. **Macro Summary**: Analyze the broader market environment using:
   - Major indices performance (S&P 500, Nasdaq, Dow, Russell 2000)
   - Volatility indicators (VIX)
   - Relevant macro news (Fed policy, economic data, geopolitics)
   - Sector rotation trends

3. **Stock Analysis**: Analyze ALL stocks with relevant news in the last 12 hours:
   - **For your holdings**: Provide detailed analysis including:
     - 48-hour price action and technical setup
     - Related news and their implications
     - Earnings report context (show latest report even if old, highlight if new within 7 days)
     - How today's events affect your investment thesis
     - Position sizing appropriateness
   - **For stocks NOT held but with news**: Brief analysis of:
     - What happened and why it's newsworthy
     - Potential investment opportunity or risk
     - Whether it warrants further investigation

4. **Portfolio Performance**: Review today's transactions (if any) and overall portfolio positioning relative to your long-term strategy (70% long-term / 30% short-term allocation).

5. **Tomorrow's Outlook**: Identify key catalysts and market factors to monitor.

Important principles:
- Focus on fundamental analysis and long-term value
- Distinguish between noise and signal
- Learn from similar historical patterns (provided in context)
- Maintain emotional discipline - avoid reactionary decisions
- Consider risk-adjusted returns, not just absolute performance

Output format:
Write a comprehensive, well-structured summary in natural language paragraphs. Use clear section headings (## for main sections). Be thorough but focused:
- Daily Overview + Macro Summary: ~200-300 words
- Stock Analysis (holdings): ~100-150 words per position (all holdings included)
- Stock Analysis (news-mentioned): ~50-80 words per stock (limited by batch size to prevent token overflow)
- Portfolio Review + Outlook: ~150-200 words
Total target: 800-1500 words depending on number of holdings and news volume.

Note: The number of news-mentioned stocks shown is dynamically limited based on total analysis load to stay within token limits.

User Requirement: Output PLAIN TEXT paragraphs only (no JSON, no structured data), stored directly in review_content field."""
    
    def _build_summary_prompt(
        self,
        data: Dict[str, Any],
        similar_summaries: list
    ) -> str:
        """
        Build Daily Summary Prompt

        Args:
            data: Today's data
            similar_summaries: Similar historical summaries

        Returns:
            Prompt text
        """
        parts = ["# Daily Summary Task", ""]

        # 1. Portfolio Status
        parts.append("## Current Portfolio")
        parts.append(f"- Total Assets: ${data['portfolio_value']['total_value']:.2f}")
        parts.append(f"- Cash: ${data['portfolio_value']['cash']:.2f}")
        parts.append(f"- Market Value of Positions: ${data['portfolio_value']['stocks']:.2f}")
        parts.append("")

        # 2. NEW: Top 3 News Analysis
        if data['news_analysis']:
            parts.append("## Top 3 News (Last 12 Hours)")
            # Sort by confidence_score
            top_news = sorted(
                data['news_analysis'],
                key=lambda x: x.get('confidence_score', 0),
                reverse=True
            )[:3]

            for i, analysis in enumerate(top_news, 1):
                # Find corresponding news article
                news_article = next(
                    (n for n in data['news'] if n['news_id'] == analysis['news_id']),
                    None
                )
                if news_article:
                    parts.append(f"{i}. **{news_article['title']}**")
                    parts.append(f"   - Sentiment: {analysis['sentiment']}")
                    parts.append(f"   - Impact: {analysis['impact_prediction']}")
                    parts.append(f"   - Confidence: {analysis['confidence_score']}")
                    parts.append(f"   - Analysis: {analysis['analysis']}")
            parts.append("")

        # 3. NEW: Macro Market Summary
        parts.append("## Macro Market Environment (48h)")
        env = data['market_environment']
        parts.append(f"- S&P 500 Trend: {env['sp500_trend']} ({env['sp500_change_48h']:+.2f}%)")
        parts.append(f"- VIX Level: {env['vix_level']} (${env['vix_price']:.2f})")
        parts.append(f"- Macro News Count: {env['macro_news_count']}")

        # Market indices performance
        parts.append("\n### Major Indices (48h Change)")
        for idx_symbol in data['market_indices']:
            idx_data = data['price_changes_48h'].get(idx_symbol, {})
            if idx_data and 'change_pct' in idx_data:
                parts.append(f"- {idx_symbol}: {idx_data.get('change_pct', 0):+.2f}%")
        parts.append("")

        # 4. NEW: Earnings Reports (if any recent ones)
        if data.get('recent_earnings'):
            parts.append("## Recent Earnings Reports (Last 7 Days)")
            for report in data['recent_earnings']:
                parts.append(f"- **{report['symbol']}** - {report['report_type']} "
                            f"(FY{report['fiscal_year']} Q{report.get('fiscal_quarter', 'N/A')})")
                parts.append(f"  Filed: {report['filing_date']}")
                if report.get('summary_en'):
                    parts.append(f"  Summary: {report['summary_en'][:200]}...")
            parts.append("")

        # 5. NEW: Stock/ETF Analysis Section
        parts.append("## Stock & ETF Analysis (48h)")

        # 5a. Your Holdings (always included)
        if data['positions']:
            parts.append("### Your Current Holdings")
            for pos in data['positions']:
                symbol = pos['symbol']
                price_data = data['price_changes_48h'].get(symbol, {})
                parts.append(f"- **{symbol}** ({pos['quantity']} shares, {pos['position_type']})")
                parts.append(f"  Current: ${price_data.get('current_price', 0):.2f} "
                            f"({price_data.get('change_pct', 0):+.2f}% / 48h)")
                parts.append(f"  Range: ${price_data.get('low_48h', 0):.2f} - ${price_data.get('high_48h', 0):.2f}")
                parts.append(f"  Unrealized P&L: ${pos['unrealized_pnl']:.2f}")

                # Check if there's related news
                related_news = [
                    n for n in data['news_analysis']
                    if symbol in n.get('mentioned_stocks', [])
                ]
                if related_news:
                    parts.append(f"  **Related News: {len(related_news)} article(s)**")
                    for news in related_news[:2]:  # Show top 2
                        news_article = next(
                            (n for n in data['news'] if n['news_id'] == news['news_id']),
                            None
                        )
                        if news_article:
                            parts.append(f"    - {news_article['title'][:80]}...")
                            parts.append(f"      Sentiment: {news['sentiment']}, "
                                        f"Impact: {news['impact_prediction'][:50]}...")

                # Check if there's earnings
                related_earnings = [
                    e for e in data.get('all_earnings', [])
                    if e['symbol'] == symbol
                ]
                if related_earnings:
                    latest_earnings = related_earnings[0]
                    # Check if it's new (within 7 days)
                    is_new = any(
                        e['symbol'] == symbol
                        for e in data.get('recent_earnings', [])
                    )
                    new_tag = " **[NEW]**" if is_new else ""
                    parts.append(f"  **Latest Earnings{new_tag}: {latest_earnings['report_type']} "
                                f"(FY{latest_earnings['fiscal_year']} "
                                f"Q{latest_earnings.get('fiscal_quarter', '')})**")
                    parts.append(f"    Filed: {latest_earnings['filing_date']}")
            parts.append("")

        # 5b. Stocks with News (NOT in holdings)
        holding_symbols = {pos['symbol'] for pos in data['positions']}
        news_mentioned_stocks = set()
        for analysis in data['news_analysis']:
            for stock in analysis.get('mentioned_stocks', []):
                if stock not in holding_symbols:
                    news_mentioned_stocks.add(stock)

        if news_mentioned_stocks:
            # Calculate batch size based on total stock count to prevent token overflow
            # User requirement: batch = int(len(stocks) / 15) + 1
            # This limits news-mentioned stocks shown in prompt based on total analysis load
            total_stocks = len(data.get('positions', [])) + len(news_mentioned_stocks)
            batch_size = int(total_stocks / 15) + 1
            max_news_stocks = max(3, min(batch_size, 10))  # Ensure between 3-10

            logger.info(f"Token management: holdings={len(data.get('positions', []))}, "
                       f"news_mentioned={len(news_mentioned_stocks)}, total={total_stocks}, "
                       f"max_news_stocks_shown={max_news_stocks} (batch_size={batch_size})")

            parts.append("### Stocks with News (Not Currently Held)")
            for symbol in sorted(news_mentioned_stocks)[:max_news_stocks]:
                price_data = data['price_changes_48h'].get(symbol, {})
                parts.append(f"- **{symbol}**")
                parts.append(f"  Current: ${price_data.get('current_price', 0):.2f} "
                            f"({price_data.get('change_pct', 0):+.2f}% / 48h)")

                # Related news
                related_news = [
                    n for n in data['news_analysis']
                    if symbol in n.get('mentioned_stocks', [])
                ]
                parts.append(f"  **News: {len(related_news)} article(s)**")
                for news in related_news[:1]:  # Show top 1
                    news_article = next(
                        (n for n in data['news'] if n['news_id'] == news['news_id']),
                        None
                    )
                    if news_article:
                        parts.append(f"    - {news_article['title'][:80]}...")
                        parts.append(f"      {news['sentiment']}: {news['analysis'][:100]}...")

                # Earnings if available
                related_earnings = [
                    e for e in data.get('all_earnings', [])
                    if e['symbol'] == symbol
                ]
                if related_earnings:
                    latest_earnings = related_earnings[0]
                    parts.append(f"  Latest Earnings: {latest_earnings['report_type']} "
                                f"({latest_earnings['filing_date']})")
            parts.append("")

        # 6. Today's Transactions
        if data['transactions']:
            parts.append("## Today's Transactions")
            for tx in data['transactions']:
                parts.append(f"- {tx['action']} {tx['symbol']}: {tx['quantity']} shares @ ${tx['price']:.2f}")
            parts.append("")
        else:
            parts.append("## Today's Transactions")
            parts.append("- No transactions")
            parts.append("")

        # 6b. Decision History (last 5 days)
        history = data.get('decision_history', [])
        parts.append("## Decision History (last 5 days)")
        if history:
            for tx in history[:10]:
                executed = tx.get('executed_at', 'N/A')
                action = tx.get('action', 'N/A')
                symbol = tx.get('symbol', 'N/A')
                qty = tx.get('quantity', 0)
                price = tx.get('price', 0.0)
                pos_type = tx.get('position_type', 'N/A')
                reason = (tx.get('reason') or '')[:120]
                parts.append(
                    f"- {executed}: {action} {symbol} {qty} @ ${float(price):.2f} ({pos_type}) | Reason: {reason}"
                )
        else:
            parts.append("- No trades in the last 5 days")
        parts.append("")

        # 7. RAG similar summaries
        if similar_summaries:
            parts.append("## Similar Historical Situations")
            formatted_cases = self.rag_retriever.format_results_for_prompt(
                similar_summaries,
                max_cases=3
            )
            parts.append(formatted_cases)
            parts.append("")

        # 8. Updated task description
        parts.append("## Please generate today's comprehensive summary")
        parts.append("")
        parts.append("Your summary should include:")
        parts.append("1. **Daily Overview**: Summarize top 3 news events and their implications")
        parts.append("2. **Macro Summary**: Analyze market indices, VIX, and macro environment")
        parts.append("3. **Stock Analysis**: For each holding and news-mentioned stock, analyze:")
        parts.append("   - Price action (48h)")
        parts.append("   - Related news impact")
        parts.append("   - Earnings report implications (if any)")
        parts.append("   - Overall assessment and outlook")
        parts.append("4. **Portfolio Review**: Overall performance and positioning")
        parts.append("5. **Tomorrow's Outlook**: Key factors to watch")
        parts.append("")
        parts.append("Write in natural, structured paragraphs with clear sections.")

        return "\n".join(parts)
    
    def _save_summary(
        self,
        agent_id: str,
        summary: str,
        data: Dict[str, Any]
    ) -> bool:
        """
        Save the daily summary to the database

        Args:
            agent_id: AI ID
            summary: Summary text
            data: Today's data

        Returns:
            True if saved successfully
        """
        try:
            # Calculate actual daily PnL from portfolio snapshots
            yesterday_value = self._get_portfolio_value_yesterday(agent_id)
            today_value = data['portfolio_value']['total_value']
            daily_pnl = today_value - yesterday_value if yesterday_value else 0.0

            # Get initial capital from ai_agents table
            agent_query = "SELECT initial_capital FROM ai_agents WHERE agent_id = %s"
            agent_result = self.data_collector.db.execute_query(agent_query, (agent_id,))
            initial_capital = float(agent_result[0]['initial_capital']) if agent_result else 100000.0

            total_pnl = today_value - initial_capital

            query = """
                INSERT INTO daily_reviews (
                    review_date,
                    agent_id,
                    portfolio_value,
                    daily_pnl,
                    total_pnl,
                    transactions_count,
                    review_content
                ) VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_date, agent_id) DO UPDATE SET
                    portfolio_value = EXCLUDED.portfolio_value,
                    daily_pnl = EXCLUDED.daily_pnl,
                    total_pnl = EXCLUDED.total_pnl,
                    transactions_count = EXCLUDED.transactions_count,
                    review_content = EXCLUDED.review_content
            """

            self.data_collector.db.execute_update(
                query,
                (
                    agent_id,
                    today_value,
                    daily_pnl,
                    total_pnl,
                    len(data['transactions']),
                    summary
                )
            )

            logger.info(f"Summary saved to database (Daily P&L: ${daily_pnl:.2f})")
            return True

        except Exception as e:
            logger.error(f"Failed to save summary: {e}")
            return False

    def _get_portfolio_value_yesterday(self, agent_id: str) -> Optional[float]:
        """
        Get yesterday's portfolio value from portfolio_snapshots

        Args:
            agent_id: AI ID

        Returns:
            Yesterday's portfolio value, or None if not found
        """
        try:
            query = """
                SELECT total_portfolio_value
                FROM portfolio_snapshots
                WHERE agent_id = %s
                  AND DATE(snapshot_time) = CURRENT_DATE - INTERVAL '1 day'
                ORDER BY snapshot_time DESC
                LIMIT 1
            """
            result = self.data_collector.db.execute_query(query, (agent_id,))
            if result and len(result) > 0:
                return float(result[0]['total_portfolio_value'])
            return None

        except Exception as e:
            logger.error(f"Failed to get yesterday's portfolio value: {e}")
            return None
    
    def _write_to_rag(self, agent_id: str, summary: str) -> bool:
        """
        Generate embedding and write to OpenSearch
        
        Args:
            agent_id: AI ID
            summary: Summary text
            
        Returns:
            True if written successfully
        """
        try:
            # Generate embedding
            embedding = self.bedrock.generate_embedding(summary)
            
            # Write to OpenSearch
            decision_id = str(uuid.uuid4())
            
            self.opensearch.index_decision(
                decision_id=decision_id,
                agent_id=agent_id,
                decision_embedding=embedding,
                reasoning=summary,
                decision_type='DAILY_SUMMARY',
                symbol='N/A',
                quality_weight=0.5,
                metadata={
                    'type': 'daily_summary',
                    'date': str(get_et_today()),
                    'agent_id': agent_id
                }
            )
            
            logger.info("Summary written to RAG knowledge base")
            return True
        
        except Exception as e:
            logger.error(f"Failed to write to RAG: {e}")
            return False

    def _generate_stock_summaries(
        self,
        agent_id: str,
        data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate per-stock daily analysis

        Args:
            agent_id: AI ID
            data: Today's collected data

        Returns:
            List of stock summaries [{'symbol': str, 'content': str, ...}, ...]
        """
        try:
            # Identify all stocks mentioned in news today
            news_mentioned_stocks = set()
            for analysis in data['news_analysis']:
                for stock in analysis.get('mentioned_stocks', []):
                    news_mentioned_stocks.add(stock)

            # Also include current holdings
            holding_symbols = {pos['symbol'] for pos in data['positions']}

            # Combine all stocks to analyze
            all_stocks_to_analyze = news_mentioned_stocks.union(holding_symbols)

            # OPTIMIZATION 1: Filter out market indices (symbols starting with ^)
            # Only analyze individual stocks and ETFs
            all_stocks_to_analyze = {
                symbol for symbol in all_stocks_to_analyze
                if not symbol.startswith('^')
            }

            # OPTIMIZATION 2: Filter out stocks not in our database stocks table
            # This prevents analyzing random stocks mentioned in news that we don't track
            valid_symbols = self._get_valid_stock_symbols()
            filtered_stocks = all_stocks_to_analyze.intersection(valid_symbols)

            # Log which stocks were filtered out
            filtered_out = all_stocks_to_analyze - filtered_stocks
            if filtered_out:
                logger.info(f"Filtered out {len(filtered_out)} stocks not in database: {sorted(filtered_out)}")

            all_stocks_to_analyze = filtered_stocks

            if not all_stocks_to_analyze:
                logger.info("No stocks to analyze (no news mentions and no holdings)")
                return []

            logger.info(f"Generating analysis for {len(all_stocks_to_analyze)} stocks (excluding market indices): {sorted(all_stocks_to_analyze)}")

            stock_summaries = []

            # OPTIMIZATION 2: Process stocks in batches of 5 to save tokens
            stocks_list = sorted(all_stocks_to_analyze)
            batch_size = 5

            for batch_start in range(0, len(stocks_list), batch_size):
                batch_symbols = stocks_list[batch_start:batch_start + batch_size]

                try:
                    # Prepare batch data for all stocks in this batch
                    batch_stock_data = []
                    for symbol in batch_symbols:
                        # 1. Get news analysis for this stock
                        stock_news_analysis = [
                            a for a in data['news_analysis']
                            if symbol in a.get('mentioned_stocks', [])
                        ]

                        # 2. Retrieve stock memories from RAG
                        stock_memories = self.rag_retriever.retrieve_stock_memories(
                            agent_id=agent_id,
                            symbol=symbol,
                            num_results=3
                        )

                        # 3. Retrieve latest weekly summary from RAG
                        weekly_memories = self.rag_retriever.retrieve_latest_stock_weekly_summary(
                            agent_id=agent_id,
                            symbol=symbol,
                            num_results=1
                        )

                        # 4. Retrieve recent daily summaries (RAG) for past 5 days
                        rag_daily_summaries = self.rag_retriever.retrieve_recent_stock_daily_summaries(
                            agent_id=agent_id,
                            symbol=symbol,
                            days=5,
                            num_results=5
                        )

                        batch_stock_data.append({
                            'symbol': symbol,
                            'news_analysis': stock_news_analysis,
                            'stock_memories': stock_memories,
                            'weekly_memories': weekly_memories,
                            'rag_daily_summaries': rag_daily_summaries,
                            'mentioned_in_news': symbol in news_mentioned_stocks,
                            'is_holding': symbol in holding_symbols
                        })

                    # 3. Build combined prompt for batch
                    batch_prompt = self._build_batch_stock_analysis_prompt(
                        batch_stock_data=batch_stock_data,
                        data=data
                    )

                    # 4. Call AI with retry logic for format validation
                    batch_analyses = self._call_batch_analysis_with_retry(
                        agent_id=agent_id,
                        batch_prompt=batch_prompt,
                        batch_symbols=batch_symbols,
                        max_retries=2
                    )

                    # Store results
                    for stock_data, analysis_content in zip(batch_stock_data, batch_analyses):
                        if analysis_content:
                            sentiment = self._extract_sentiment_from_analysis(stock_data['news_analysis'])

                            stock_summaries.append({
                                'symbol': stock_data['symbol'],
                                'content': analysis_content.strip(),
                                'mentioned_in_news': stock_data['mentioned_in_news'],
                                'is_holding': stock_data['is_holding'],
                                'sentiment': sentiment,
                                'key_events': [a.get('impact_prediction', '')[:100] for a in stock_data['news_analysis'][:3]]
                            })

                            logger.info(f"Generated analysis for {stock_data['symbol']} ({len(analysis_content)} chars)")
                        else:
                            logger.warning(f"Failed to parse analysis for {stock_data['symbol']}")

                except Exception as e:
                    logger.error(f"Error generating batch analysis for {batch_symbols}: {e}")
                    continue

            logger.info(f"Successfully generated {len(stock_summaries)} stock analyses")
            return stock_summaries

        except Exception as e:
            logger.error(f"Failed to generate stock summaries: {e}")
            return []

    def _call_batch_analysis_with_retry(
        self,
        agent_id: str,
        batch_prompt: str,
        batch_symbols: List[str],
        max_retries: int = 2
    ) -> List[str]:
        """
        Call AI for batch stock analysis with retry logic

        Args:
            agent_id: AI agent ID
            batch_prompt: The batch analysis prompt
            batch_symbols: List of stock symbols in the batch
            max_retries: Maximum retry attempts for format validation failures

        Returns:
            List of analysis texts (one per symbol, in order)
        """
        messages = [
            {
                "role": "system",
                "content": self._get_batch_stock_analysis_system_prompt()
            },
            {
                "role": "user",
                "content": batch_prompt
            }
        ]

        for attempt in range(max_retries + 1):
            # 批量分析使用 2 倍 timeout
            result = self.ai_orchestrator.call_single_agent(
                agent_id=agent_id,
                messages=messages,
                temperature=0.7,
                timeout_multiplier=2.0
            )

            # Record token usage regardless of parse success (if we got a response)
            if result and result.get('usage'):
                self.token_recorder.record_from_usage(
                    agent_id=agent_id,
                    service='daily_summary',
                    usage=result['usage']
                )

            if not result or not result['success']:
                logger.warning(f"Failed to generate batch analysis for {batch_symbols}")
                return [""] * len(batch_symbols)

            response = result['response']

            # Parse batch response
            batch_analyses = self._parse_batch_analysis_response(response, batch_symbols)

            # Validate: check how many symbols were successfully parsed
            parsed_count = sum(1 for a in batch_analyses if a)
            expected_count = len(batch_symbols)

            if parsed_count == expected_count:
                # All symbols parsed successfully
                return batch_analyses

            # Some symbols failed to parse
            missing_symbols = [
                sym for sym, analysis in zip(batch_symbols, batch_analyses)
                if not analysis
            ]

            logger.warning(
                f"Batch analysis format validation failed (attempt {attempt + 1}/{max_retries + 1}): "
                f"parsed {parsed_count}/{expected_count}, missing: {missing_symbols}"
            )

            if attempt < max_retries:
                # Add retry feedback to messages
                logger.info(f"Retrying batch analysis with format feedback...")
                messages = messages + [
                    {
                        "role": "assistant",
                        "content": response
                    },
                    {
                        "role": "user",
                        "content": f"""Your response format was incorrect. I could not parse analyses for these symbols: {missing_symbols}

CRITICAL: You MUST use this EXACT format for EVERY stock:

[SYMBOL: AAPL]
Your analysis text here...

[SYMBOL: TSLA]
Your analysis text here...

Rules:
1. Start EVERY stock analysis with [SYMBOL: XXX] on its own line
2. The symbol marker MUST use square brackets: [SYMBOL: XXX]
3. Put the analysis text immediately after
4. Include ALL {len(batch_symbols)} stocks: {batch_symbols}

Please regenerate the analysis for ALL stocks using the correct format."""
                    }
                ]
            else:
                # Max retries reached, return what we have
                logger.warning(f"Max retries reached for batch analysis, using partial results")
                return batch_analyses

        return [""] * len(batch_symbols)

    def _get_stock_analysis_system_prompt(self) -> str:
        """
        Get system prompt for stock analysis

        Returns:
            System prompt
        """
        return """You are a professional portfolio manager analyzing individual stocks.

Your task:
Generate a concise daily analysis for a specific stock (target: 50 words).

Your analysis should include:
1. Key news/events impact assessment
2. Your current view based on historical analysis
3. Brief outlook or action consideration

Principles:
- Be concise and actionable
- Focus on what changed today
- Reference your past analysis if relevant
- Maintain consistency with your investment thesis

Output: Plain text paragraph, approximately 50 words."""

    def _build_stock_analysis_prompt(
        self,
        symbol: str,
        news_analysis: List[Dict[str, Any]],
        stock_memories: List[Dict[str, Any]],
        data: Dict[str, Any]
    ) -> str:
        """
        Build prompt for individual stock analysis

        Args:
            symbol: Stock symbol
            news_analysis: News analyses mentioning this stock
            stock_memories: RAG retrieved memories
            data: Today's data

        Returns:
            Prompt text
        """
        parts = [f"# Daily Analysis for {symbol}", ""]

        # 1. Today's news
        if news_analysis:
            parts.append("## Today's News Analysis")
            for i, analysis in enumerate(news_analysis[:3], 1):  # Top 3
                news_article = next(
                    (n for n in data['news'] if n['news_id'] == analysis['news_id']),
                    None
                )
                if news_article:
                    parts.append(f"{i}. **{news_article['title']}**")
                    parts.append(f"   Your analysis: {analysis['analysis'][:150]}...")
                    parts.append(f"   Sentiment: {analysis['sentiment']}")
                    parts.append(f"   Impact: {analysis['impact_prediction'][:100]}...")
            parts.append("")
        else:
            parts.append("## Today's News")
            parts.append("No news mentioning this stock today.")
            parts.append("")

        # 2. Your historical view
        parts.append("## Your Historical View")
        if stock_memories:
            formatted_memories = self.rag_retriever.format_stock_memories_for_prompt(
                stock_memories,
                max_memories=3
            )
            parts.append(formatted_memories)
        else:
            parts.append("No previous analysis found.")
        parts.append("")

        # 3. Current position (if holding)
        holding = next((p for p in data['positions'] if p['symbol'] == symbol), None)
        if holding:
            parts.append("## Your Current Position")
            parts.append(f"- Holding: {holding['quantity']} shares")
            parts.append(f"- Average cost: ${holding['average_cost']:.2f}")
            parts.append(f"- Current P&L: ${holding['unrealized_pnl']:.2f}")
            parts.append("")

        # 4. 48h price action
        price_data = data['price_changes_48h'].get(symbol, {})
        if price_data:
            parts.append("## 48-Hour Price Action")
            parts.append(f"- Current: ${price_data.get('current_price', 0):.2f}")
            parts.append(f"- Change: {price_data.get('change_pct', 0):+.2f}%")
            parts.append(f"- Range: ${price_data.get('low_48h', 0):.2f} - ${price_data.get('high_48h', 0):.2f}")
            parts.append("")

        # 5. Task
        parts.append("## Task")
        parts.append(f"Generate a concise daily analysis for {symbol} (~50 words).")
        parts.append("Focus on: today's key developments, your view, and brief outlook.")

        return "\n".join(parts)

    def _get_valid_stock_symbols(self) -> set:
        """
        Get all valid stock symbols from the database stocks table

        Returns:
            Set of valid stock symbols (str)
        """
        try:
            query = "SELECT symbol FROM stocks WHERE enabled = true"
            result = self.db.execute_query(query)
            return {row['symbol'] for row in result}
        except Exception as e:
            logger.error(f"Failed to fetch valid stock symbols: {e}")
            # Return empty set to fail-safe (won't filter anything if DB query fails)
            return set()

    def _extract_sentiment_from_analysis(self, news_analysis: List[Dict[str, Any]]) -> str:
        """
        Extract overall sentiment from news analyses

        Args:
            news_analysis: List of news analyses

        Returns:
            Sentiment: POSITIVE/NEGATIVE/NEUTRAL/MIXED
        """
        if not news_analysis:
            return 'NEUTRAL'

        sentiments = [a.get('sentiment', 'NEUTRAL') for a in news_analysis]
        sentiment_counts = {
            'POSITIVE': sentiments.count('POSITIVE'),
            'NEGATIVE': sentiments.count('NEGATIVE'),
            'NEUTRAL': sentiments.count('NEUTRAL')
        }

        # If mixed sentiments, return MIXED
        if sentiment_counts['POSITIVE'] > 0 and sentiment_counts['NEGATIVE'] > 0:
            return 'MIXED'

        # Otherwise return the most common sentiment
        return max(sentiment_counts, key=sentiment_counts.get)

    def _save_stock_summaries(
        self,
        agent_id: str,
        stock_summaries: List[Dict[str, Any]]
    ) -> bool:
        """
        Save stock summaries to database

        Args:
            agent_id: AI ID
            stock_summaries: List of stock summaries

        Returns:
            True if saved successfully
        """
        try:
            query = """
                INSERT INTO stock_summaries (
                    agent_id,
                    symbol,
                    summary_date,
                    summary_type,
                    content,
                    mentioned_in_news,
                    is_holding,
                    sentiment,
                    key_events
                ) VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (agent_id, symbol, summary_date, summary_type) DO UPDATE SET
                    content = EXCLUDED.content,
                    mentioned_in_news = EXCLUDED.mentioned_in_news,
                    is_holding = EXCLUDED.is_holding,
                    sentiment = EXCLUDED.sentiment,
                    key_events = EXCLUDED.key_events,
                    created_at = CURRENT_TIMESTAMP
            """

            for summary in stock_summaries:
                self.data_collector.db.execute_update(
                    query,
                    (
                        agent_id,
                        summary['symbol'],
                        'daily',  # summary_type
                        summary['content'],
                        summary['mentioned_in_news'],
                        summary['is_holding'],
                        summary['sentiment'],
                        summary['key_events']  # PostgreSQL array
                    )
                )

            logger.info(f"Saved {len(stock_summaries)} stock summaries to database")
            return True

        except Exception as e:
            logger.error(f"Failed to save stock summaries: {e}")
            return False

    def _index_stock_summaries_to_rag(
        self,
        agent_id: str,
        stock_summaries: List[Dict[str, Any]]
    ) -> bool:
        """
        Index stock summaries to OpenSearch RAG

        Args:
            agent_id: AI ID
            stock_summaries: List of stock summaries

        Returns:
            True if indexed successfully
        """
        try:
            for summary in stock_summaries:
                # Generate embedding
                embedding = self.bedrock.generate_embedding(summary['content'])

                # Index to OpenSearch
                decision_id = str(uuid.uuid4())

                self.opensearch.index_decision(
                    decision_id=decision_id,
                    agent_id=agent_id,
                    decision_embedding=embedding,
                    reasoning=summary['content'],
                    decision_type='STOCK_DAILY_SUMMARY',
                    symbol=summary['symbol'],
                    quality_weight=0.5,
                    metadata={
                        'type': 'stock_daily_summary',
                        'date': str(get_et_today()),
                        'symbol': summary['symbol'],
                        'agent_id': agent_id,
                        'sentiment': summary['sentiment'],
                        'is_holding': summary['is_holding'],
                        'mentioned_in_news': summary['mentioned_in_news']
                    }
                )

            logger.info(f"Indexed {len(stock_summaries)} stock summaries to RAG")
            return True

        except Exception as e:
            logger.error(f"Failed to index stock summaries to RAG: {e}")
            return False

    def _get_batch_stock_analysis_system_prompt(self) -> str:
        """
        Get system prompt for batch stock analysis

        Returns:
            System prompt
        """
        return """You are a professional portfolio manager analyzing multiple stocks simultaneously.

Your task:
Generate concise daily analyses for multiple stocks in one response (target: 50 words per stock).

For each stock, your analysis should include:
1. Key news/events impact assessment
2. Your current view based on historical analysis
3. Brief outlook or action consideration

Principles:
- Be concise and actionable
- Focus on what changed today
- Reference your past analysis if relevant
- Maintain consistency with your investment thesis

CRITICAL OUTPUT FORMAT REQUIREMENTS:
You MUST strictly follow this exact format for EVERY stock:

[SYMBOL: AAPL]
Apple's new AI features announcement strengthens its competitive moat in mobile AI. Recent price action shows positive momentum. Maintaining bullish outlook pending broader market sentiment. Consider adding on dips below $180.

[SYMBOL: TSLA]
Tesla faces competitive pressure from GM's modernization efforts, though impact appears minimal. 48h price decline of -1.2% reflects broader EV sector weakness. Monitoring for Q1 delivery numbers before action.

MANDATORY RULES:
1. Start EVERY stock analysis with [SYMBOL: XXX] on its own line (where XXX is the stock ticker)
2. The symbol marker MUST use square brackets and the exact format: [SYMBOL: XXX]
3. Put the analysis text immediately after the symbol marker (can be on next line or same line after the marker)
4. Separate different stock analyses with a blank line
5. Do NOT use any other format, headers, or numbering
6. Do NOT add markdown formatting like bold, italics, or bullet points to the symbol markers
7. The response should contain ONLY the symbol markers and analysis text - no introduction, no conclusion, no extra commentary

If you do not follow this format EXACTLY, the parsing will fail and your analysis will be lost."""

    def _build_batch_stock_analysis_prompt(
        self,
        batch_stock_data: List[Dict[str, Any]],
        data: Dict[str, Any]
    ) -> str:
        """
        Build combined prompt for batch stock analysis

        Args:
            batch_stock_data: List of stock data dicts
            data: Today's overall data

        Returns:
            Combined prompt text
        """
        parts = ["# Batch Daily Stock Analysis", ""]
        parts.append(f"Analyze the following {len(batch_stock_data)} stocks:")
        parts.append("")

        for i, stock_data in enumerate(batch_stock_data, 1):
            symbol = stock_data['symbol']
            news_analysis = stock_data['news_analysis']
            stock_memories = stock_data['stock_memories']
            weekly_memories = stock_data.get('weekly_memories', [])
            rag_daily_summaries = stock_data.get('rag_daily_summaries', [])

            parts.append(f"## Stock {i}: {symbol}")
            parts.append("")

            # Today's news
            if news_analysis:
                parts.append("**Today's News:**")
                for j, analysis in enumerate(news_analysis[:2], 1):  # Top 2 to save tokens
                    news_article = next(
                        (n for n in data['news'] if n['news_id'] == analysis['news_id']),
                        None
                    )
                    if news_article:
                        parts.append(f"{j}. {news_article['title'][:80]}...")
                        parts.append(f"   Sentiment: {analysis['sentiment']}, Impact: {analysis['impact_prediction'][:60]}...")
                parts.append("")
            else:
                parts.append("**Today's News:** No news")
                parts.append("")

            # Historical view (condensed)
            if stock_memories:
                parts.append("**Your Historical View:**")
                for memory in stock_memories[:2]:  # Top 2 to save tokens
                    content = memory.get('content', '')
                    metadata = memory.get('metadata', {})
                    date = metadata.get('date', 'Unknown')
                    parts.append(f"- ({date}) {content[:100]}...")
                parts.append("")
            else:
                parts.append("**Your Historical View:** No previous analysis")
                parts.append("")

            # Latest weekly summary (RAG)
            parts.append("**Latest Weekly Summary (RAG):**")
            if weekly_memories:
                wm = weekly_memories[0]
                meta = wm.get('metadata', {})
                parts.append(f"- ({meta.get('date', 'Unknown')}) {wm.get('content', '')[:300]}")
            else:
                parts.append("- None retrieved from RAG")
            parts.append("")

            # Recent daily summaries (RAG)
            parts.append("**Recent Daily Summaries (RAG, last 5 days):**")
            if rag_daily_summaries:
                for mem in rag_daily_summaries[:3]:
                    meta = mem.get('metadata', {})
                    parts.append(f"- ({meta.get('date', 'Unknown')}) {mem.get('content', '')[:250]}")
            else:
                parts.append("- None retrieved from RAG")
            parts.append("")

            # Current position (if holding)
            holding = next((p for p in data['positions'] if p['symbol'] == symbol), None)
            if holding:
                parts.append(f"**Current Position:** {holding['quantity']} shares @ ${holding['average_cost']:.2f}")
                parts.append("")

            # 48h price action (condensed)
            price_data = data['price_changes_48h'].get(symbol, {})
            if price_data:
                parts.append(f"**48h Price:** ${price_data.get('current_price', 0):.2f} ({price_data.get('change_pct', 0):+.2f}%)")
                parts.append("")

            parts.append("---")
            parts.append("")

        parts.append("## Task")
        parts.append(f"Generate a ~50-word analysis for EACH of the {len(batch_stock_data)} stocks above.")
        parts.append("")
        parts.append("CRITICAL: You MUST use this EXACT format:")
        parts.append("")
        parts.append("[SYMBOL: " + batch_stock_data[0]['symbol'] + "]")
        parts.append("[Your analysis here in ~50 words]")
        parts.append("")
        parts.append("[SYMBOL: " + batch_stock_data[1]['symbol'] + "]" if len(batch_stock_data) > 1 else "[SYMBOL: NEXT_STOCK]")
        parts.append("[Your analysis here in ~50 words]")
        parts.append("")
        parts.append("... (continue for all stocks)")
        parts.append("")
        parts.append("DO NOT add any introduction, conclusion, or other text. ONLY the symbol markers and analyses.")

        return "\n".join(parts)

    def _parse_batch_analysis_response(
        self,
        response: str,
        expected_symbols: List[str]
    ) -> List[str]:
        """
        Parse batch analysis response into individual stock analyses

        Args:
            response: AI response text
            expected_symbols: List of symbols we expect in order

        Returns:
            List of analysis texts (one per symbol, in order)
        """
        try:
            analyses = []
            lines = response.strip().split('\n')

            # Parse by looking for [SYMBOL: XXX] markers
            current_symbol = None
            current_analysis = []

            for line in lines:
                # Check if this is a symbol marker
                if line.strip().startswith('[SYMBOL:') and line.strip().endswith(']'):
                    # Save previous analysis if any
                    if current_symbol and current_analysis:
                        analyses.append({
                            'symbol': current_symbol,
                            'content': '\n'.join(current_analysis).strip()
                        })

                    # Extract new symbol
                    current_symbol = line.strip()[8:-1].strip()
                    current_analysis = []
                else:
                    # Add to current analysis
                    if current_symbol and line.strip():
                        current_analysis.append(line)

            # Don't forget the last one
            if current_symbol and current_analysis:
                analyses.append({
                    'symbol': current_symbol,
                    'content': '\n'.join(current_analysis).strip()
                })

            # Map to expected order
            result = []
            analysis_dict = {a['symbol']: a['content'] for a in analyses}

            for symbol in expected_symbols:
                if symbol in analysis_dict:
                    result.append(analysis_dict[symbol])
                else:
                    # Fallback: try to find any analysis mentioning this symbol
                    logger.warning(f"Could not find analysis for {symbol} in batch response")
                    result.append("")

            return result

        except Exception as e:
            logger.error(f"Failed to parse batch analysis response: {e}")
            return [""] * len(expected_symbols)
