"""
One-off Stock Analysis Workflow
Generate ad-hoc stock analyses while forcing inclusion of the latest financial report summary.
"""

from typing import Dict, Any, List, Optional
import uuid
from datetime import timedelta

from services import DataCollector, RAGRetriever, AIOrchestrator
from core import BedrockClient, OpenSearchClient, create_context_logger
from utils import get_et_today

logger = create_context_logger()


class StockAnalysisWorkflow:
    """
    One-off Stock Analysis Workflow

    Differences vs WeeklySummaryWorkflow:
    - Targets a specific set of symbols (or all enabled by default) for an ad-hoc run
    - Financial report summary is mandatory: workflow skips a symbol if no completed report
    """

    def __init__(
        self,
        data_collector: DataCollector,
        rag_retriever: RAGRetriever,
        ai_orchestrator: AIOrchestrator,
        bedrock_client: BedrockClient,
        opensearch_client: OpenSearchClient,
        test_mode: bool = False
    ):
        """
        Initialize workflow

        Args:
            data_collector: Data Collector service
            rag_retriever: RAG Retriever service
            ai_orchestrator: AI Orchestrator service
            bedrock_client: Bedrock Client
            opensearch_client: OpenSearch Client
            test_mode: If True, skip DB/RAG writes and print prompts/responses
        """
        self.data_collector = data_collector
        self.rag_retriever = rag_retriever
        self.ai_orchestrator = ai_orchestrator
        self.bedrock = bedrock_client
        self.opensearch = opensearch_client
        self.test_mode = test_mode

        if self.test_mode:
            logger.info("Running StockAnalysisWorkflow in TEST MODE (no DB/RAG writes)")

    def run(self, agent_id: str, symbols: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Execute ad-hoc stock analysis

        Args:
            agent_id: AI agent ID
            symbols: Optional list of symbols to analyze (defaults to all enabled stocks/ETFs)

        Returns:
            List of generated stock analyses
        """
        logger.info(
            f"Starting stock analysis for {agent_id} (test_mode={self.test_mode})",
            extra={'details': {'workflow': 'stock_analysis', 'agent_id': agent_id, 'symbols': symbols or 'ALL'}}
        )

        try:
            # 1. Collect data
            logger.info("Step 1: Collecting input data")
            data = self._collect_analysis_data(agent_id, symbols)

            if not data or not data['symbols']:
                logger.error("No symbols available for stock analysis")
                return []

            # 2. Generate stock analyses
            logger.info("Step 2: Generating stock analyses")
            stock_summaries = self._generate_stock_analyses(agent_id, data)

            if not stock_summaries:
                logger.warning("No stock analyses generated")
                return []

            if self.test_mode:
                logger.info(f"TEST MODE: Generated {len(stock_summaries)} stock analyses (no writes)")
                return stock_summaries

            # 3. Save to database
            logger.info("Step 3: Saving stock analyses to database")
            self._save_stock_summaries(agent_id, stock_summaries)

            # 4. Index to RAG
            logger.info("Step 4: Indexing stock analyses to RAG")
            self._index_stock_summaries_to_rag(agent_id, stock_summaries)

            logger.info(
                f"Stock analysis completed for {agent_id}",
                extra={'details': {'date': str(get_et_today()), 'stock_summaries_count': len(stock_summaries)}}
            )

            return stock_summaries

        except Exception as e:
            logger.error(
                f"Stock analysis workflow failed: {e}",
                extra={'details': {'agent_id': agent_id}}
            )
            return []

    def _collect_analysis_data(
        self,
        agent_id: str,
        symbols: Optional[List[str]]
    ) -> Optional[Dict[str, Any]]:
        """
        Collect data required for stock analysis
        """
        try:
            lookback_days = 7
            lookback_hours = lookback_days * 24

            # Stocks and ETFs
            stocks = self.data_collector.get_stock_list(enabled_only=True, stock_type='stock')
            etfs = self.data_collector.get_stock_list(enabled_only=True, stock_type='etf')
            all_assets = {s['symbol']: s for s in stocks + etfs}

            # Filter symbols if provided
            if symbols:
                filtered_symbols = [s for s in symbols if s in all_assets]
                missing_symbols = [s for s in symbols if s not in all_assets]
                if missing_symbols:
                    logger.warning(f"Symbols not found and skipped: {missing_symbols}")
                symbols = filtered_symbols
            else:
                symbols = list(all_assets.keys())

            if not symbols:
                return None

            # News (titles) and news analyses (with own analysis)
            news = self.data_collector.collect_news(hours=lookback_hours, symbols=symbols)
            news_analysis = self.data_collector.get_hourly_news_analysis(agent_id, hours=lookback_hours)

            # Positions (to flag holdings)
            positions = self.data_collector.get_positions(agent_id)
            holding_symbols = {p['symbol'] for p in positions}

            # Decision history (last 5 days)
            decision_history = self.data_collector.get_recent_transactions(agent_id, days=5, limit=50)

            # Previous summaries
            daily_summaries = self._fetch_recent_daily_summaries(agent_id, symbols, lookback_days)
            weekly_summaries = self._fetch_latest_weekly_summaries(agent_id, symbols)

            news_by_id = {n['news_id']: n for n in news}

            return {
                'assets': all_assets,
                'symbols': symbols,
                'news': news,
                'news_analysis': news_analysis,
                'news_by_id': news_by_id,
                'positions': positions,
                'holding_symbols': holding_symbols,
                'daily_summaries': daily_summaries,
                'weekly_summaries': weekly_summaries,
                'lookback_days': lookback_days,
                'decision_history': decision_history
            }

        except Exception as e:
            logger.error(f"Failed to collect stock analysis data: {e}")
            return None

    def _fetch_recent_daily_summaries(
        self,
        agent_id: str,
        symbols: List[str],
        lookback_days: int
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch last N days of daily stock summaries for given symbols
        """
        if not symbols:
            return {}

        try:
            query = """
                SELECT symbol, summary_date, content
                FROM stock_summaries
                WHERE agent_id = %s
                  AND summary_type = 'daily'
                  AND summary_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND symbol = ANY(%s)
                ORDER BY symbol, summary_date DESC
            """
            results = self.data_collector.db.execute_query(query, (agent_id, lookback_days, symbols))

            summaries: Dict[str, List[Dict[str, Any]]] = {}
            for row in results or []:
                summaries.setdefault(row['symbol'], []).append(row)

            return summaries
        except Exception as e:
            logger.error(f"Failed to fetch recent daily summaries: {e}")
            return {}

    def _fetch_latest_weekly_summaries(
        self,
        agent_id: str,
        symbols: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch latest weekly summary per symbol
        """
        if not symbols:
            return {}

        try:
            query = """
                SELECT DISTINCT ON (symbol) symbol, summary_date, content
                FROM stock_summaries
                WHERE agent_id = %s
                  AND summary_type = 'weekly'
                  AND symbol = ANY(%s)
                ORDER BY symbol, summary_date DESC
            """
            results = self.data_collector.db.execute_query(query, (agent_id, symbols))

            latest = {}
            for row in results or []:
                latest[row['symbol']] = row

            return latest
        except Exception as e:
            logger.error(f"Failed to fetch latest weekly summaries: {e}")
            return {}

    def _get_latest_financial_report(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest completed financial report (summary required)
        """
        reports = self.data_collector.collect_financial_reports(symbol, limit=1)

        if not reports:
            logger.error(f"No completed financial report found for {symbol}")
            return None

        report = reports[0]
        if not report.get('summary_en'):
            logger.error(f"Latest financial report for {symbol} is missing summary_en")
            return None

        return report

    def _generate_stock_analyses(
        self,
        agent_id: str,
        data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate stock analyses for provided symbols
        """
        summaries: List[Dict[str, Any]] = []

        for symbol in data['symbols']:
            try:
                stock_info = data['assets'].get(symbol, {})
                news_analysis = [
                    a for a in data['news_analysis']
                    if symbol in (a.get('mentioned_stocks') or [])
                ]
                news_analysis_sorted = sorted(
                    news_analysis,
                    key=lambda x: (
                        x.get('confidence_score', 0),
                        str(x.get('created_at', ''))
                    ),
                    reverse=True
                )[:5]

                news_items = []
                for a in news_analysis_sorted:
                    news_article = data['news_by_id'].get(a['news_id'])
                    if news_article:
                        news_items.append({
                            'title': news_article.get('title', ''),
                            'analysis': a.get('analysis', ''),
                            'sentiment': a.get('sentiment', 'NEUTRAL'),
                            'impact': a.get('impact_prediction', ''),
                            'published_at': news_article.get('published_at')
                        })

                daily_history = data['daily_summaries'].get(symbol, [])
                last_weekly = data['weekly_summaries'].get(symbol)
                is_holding = symbol in data['holding_symbols']

                # Mandatory financial report summary
                financial_report = self._get_latest_financial_report(symbol)
                if not financial_report:
                    logger.warning(f"Skipping {symbol} due to missing financial report summary")
                    continue

                rag_memories = self.rag_retriever.retrieve_stock_memories(
                    agent_id=agent_id,
                    symbol=symbol,
                    num_results=5
                )
                rag_daily_summaries = self.rag_retriever.retrieve_recent_stock_daily_summaries(
                    agent_id=agent_id,
                    symbol=symbol,
                    days=5,
                    num_results=5
                )

                prompt = self._build_stock_prompt(
                    symbol=symbol,
                    stock_info=stock_info,
                    news_items=news_items,
                    last_weekly=last_weekly,
                    daily_history=daily_history,
                    financial_report=financial_report,
                    rag_memories=rag_memories,
                    rag_daily_summaries=rag_daily_summaries,
                    rag_daily_days=5,
                    decision_history=data.get('decision_history', []),
                    is_holding=is_holding,
                    lookback_days=data['lookback_days']
                )

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

                result = self.ai_orchestrator.call_single_agent(
                    agent_id=agent_id,
                    messages=messages,
                    temperature=0.65
                )

                if not result or not result.get('success'):
                    logger.warning(f"AI failed for {symbol}: {result.get('error') if result else 'unknown error'}")
                    continue

                response_text = result['response']

                if self.test_mode:
                    logger.info(f"[TEST MODE] Prompt for {symbol}:\n{prompt}")
                    logger.info(f"[TEST MODE] AI response for {symbol}:\n{response_text}")

                sentiment = self._extract_sentiment_from_analysis(news_analysis)
                key_events = [n['title'] for n in news_items][:5]

                summaries.append({
                    'symbol': symbol,
                    'content': response_text.strip(),
                    'mentioned_in_news': len(news_items) > 0,
                    'is_holding': is_holding,
                    'sentiment': sentiment,
                    'key_events': key_events
                })

                logger.info(f"Generated stock analysis for {symbol} ({len(response_text)} chars)")

            except Exception as e:
                logger.error(f"Failed to generate stock analysis for {symbol}: {e}")
                continue

        return summaries

    def _get_system_prompt(self) -> str:
        """
        System prompt for one-off stock analysis
        """
        return """You are a professional portfolio manager writing an ad-hoc stock analysis.

For each stock/ETF, produce a focused recap (180-260 words) that MUST incorporate:
1) Key news headlines from the last 7 days with your interpretation
2) Synthesis of your latest weekly summary and past 7 daily summaries
3) Insights from the latest completed financial report summary (MANDATORY)
4) Relevant memories retrieved from your knowledge base (RAG)
5) Your stance and action bias for the near term

Principles:
- Use the financial report summary as a core anchor; flag if its implications diverge from recent price/news
- Be analytic and consistent with prior views, explaining what changed
- Emphasize signal over noise; highlight catalysts and risks
- Output plain text paragraphs only (no lists or markdown)."""

    def _build_stock_prompt(
        self,
        symbol: str,
        stock_info: Dict[str, Any],
        news_items: List[Dict[str, Any]],
        last_weekly: Optional[Dict[str, Any]],
        daily_history: List[Dict[str, Any]],
        financial_report: Dict[str, Any],
        rag_memories: List[Dict[str, Any]],
        rag_daily_summaries: List[Dict[str, Any]],
        rag_daily_days: int,
        decision_history: List[Dict[str, Any]],
        is_holding: bool,
        lookback_days: int
    ) -> str:
        """
        Build per-stock prompt (financial report summary required)
        """
        parts = [f"# One-Off Stock Analysis Input for {symbol}", ""]

        if stock_info:
            parts.append(f"Name: {stock_info.get('name', 'N/A')}, Sector: {stock_info.get('sector', 'N/A')}, Type: {stock_info.get('type', 'N/A')}")
        parts.append(f"Holding status: {'HOLDING' if is_holding else 'NOT HOLDING'}")
        parts.append("")

        parts.append(f"## News & Your Analysis (last {lookback_days} days)")
        if news_items:
            for i, item in enumerate(news_items, 1):
                parts.append(f"{i}. {item['title']}")
                parts.append(f"   Your analysis: {item.get('analysis', '')[:220]}")
                parts.append(f"   Sentiment: {item.get('sentiment', 'NEUTRAL')}, Impact: {item.get('impact', '')[:160]}")
        else:
            parts.append("No news with your analysis in this window.")
        parts.append("")

        parts.append("## Last Weekly Summary")
        if last_weekly:
            parts.append(f"Date: {last_weekly.get('summary_date')}, Content: {last_weekly.get('content', '')[:800]}")
        else:
            parts.append("No previous weekly summary.")
        parts.append("")

        parts.append(f"## Daily Summaries (last {lookback_days} days)")
        if daily_history:
            for d in daily_history[:7]:
                parts.append(f"- {d.get('summary_date')}: {d.get('content', '')[:400]}")
        else:
            parts.append("No daily summaries found.")
        parts.append("")

        parts.append("## Financial Report (MANDATORY latest summary)")
        parts.append(
            f"{financial_report.get('report_type', 'N/A')} FY{financial_report.get('fiscal_year', 'N/A')} "
            f"Q{financial_report.get('fiscal_quarter', 'N/A')} filed {financial_report.get('filing_date')}"
        )
        parts.append(f"Summary: {financial_report.get('summary_en', '')[:800]}")
        parts.append("")

        parts.append("## Retrieved Memories (RAG)")
        if rag_memories:
            for i, mem in enumerate(rag_memories[:5], 1):
                meta = mem.get('metadata', {})
                parts.append(f"{i}. ({meta.get('date', 'unknown date')}) {mem.get('content', '')[:400]}")
        else:
            parts.append("No prior memories retrieved for this symbol.")
        parts.append("")

        parts.append(f"## RAG Daily Summaries (last {rag_daily_days} days)")
        if rag_daily_summaries:
            for i, mem in enumerate(rag_daily_summaries[:5], 1):
                meta = mem.get('metadata', {})
                parts.append(f"{i}. ({meta.get('date', 'unknown date')}) {mem.get('content', '')[:400]}")
        else:
            parts.append("No RAG daily summaries retrieved in this window.")
        parts.append("")

        # Decision history (last 5 days)
        parts.append("## Decision History (last 5 days)")
        if decision_history:
            for tx in decision_history[:10]:
                executed = tx.get('executed_at', 'N/A')
                action = tx.get('action', 'N/A')
                symbol = tx.get('symbol', 'N/A')
                qty = tx.get('quantity', 0)
                price = tx.get('price', 0.0)
                pos_type = tx.get('position_type', 'N/A')
                reason = (tx.get('reason') or '')[:160]
                parts.append(f"- {executed}: {action} {symbol} {qty} @ ${float(price):.2f} ({pos_type}) | Reason: {reason}")
        else:
            parts.append("- No trades in the last 5 days")
        parts.append("")

        parts.append("## Task")
        parts.append(
            "Write a 250-400 word recap. Blend news, your past summaries, the latest financial report summary, "
            "and memories. State conviction and near-term bias clearly. Output plain text only."
        )

        return "\n".join(parts)

    def _extract_sentiment_from_analysis(self, news_analysis: List[Dict[str, Any]]) -> str:
        """
        Derive sentiment label from news analyses
        """
        if not news_analysis:
            return 'NEUTRAL'

        sentiments = [a.get('sentiment', 'NEUTRAL') for a in news_analysis]
        sentiment_counts = {
            'POSITIVE': sentiments.count('POSITIVE'),
            'NEGATIVE': sentiments.count('NEGATIVE'),
            'NEUTRAL': sentiments.count('NEUTRAL')
        }

        if sentiment_counts['POSITIVE'] > 0 and sentiment_counts['NEGATIVE'] > 0:
            return 'MIXED'

        return max(sentiment_counts, key=sentiment_counts.get)

    def _save_stock_summaries(
        self,
        agent_id: str,
        stock_summaries: List[Dict[str, Any]]
    ) -> bool:
        """
        Save stock analyses to database
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
                        'weekly',
                        summary['content'],
                        summary['mentioned_in_news'],
                        summary['is_holding'],
                        summary['sentiment'],
                        summary['key_events']
                    )
                )

            logger.info(f"Saved {len(stock_summaries)} stock analyses to database")
            return True

        except Exception as e:
            logger.error(f"Failed to save stock analyses: {e}")
            return False

    def _index_stock_summaries_to_rag(
        self,
        agent_id: str,
        stock_summaries: List[Dict[str, Any]]
    ) -> bool:
        """
        Index stock analyses to OpenSearch RAG
        """
        try:
            for summary in stock_summaries:
                embedding = self.bedrock.generate_embedding(summary['content'])
                decision_id = str(uuid.uuid4())

                self.opensearch.index_decision(
                    decision_id=decision_id,
                    agent_id=agent_id,
                    decision_embedding=embedding,
                    reasoning=summary['content'],
                    decision_type='STOCK_WEEKLY_SUMMARY',
                    symbol=summary['symbol'],
                    quality_weight=0.5,
                    metadata={
                        'type': 'stock_analysis',
                        'date': str(get_et_today()),
                        'symbol': summary['symbol'],
                        'agent_id': agent_id,
                        'sentiment': summary['sentiment'],
                        'is_holding': summary['is_holding'],
                        'mentioned_in_news': summary['mentioned_in_news']
                    }
                )

            logger.info(f"Indexed {len(stock_summaries)} stock analyses to RAG")
            return True

        except Exception as e:
            logger.error(f"Failed to index stock analyses to RAG: {e}")
            return False
