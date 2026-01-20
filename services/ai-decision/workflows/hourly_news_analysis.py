"""
Hourly News Analysis Workflow
每小时新闻分析：分析最近 1 小时的新闻（不检索 RAG）
"""

from typing import Dict, Any, List, Optional
import json
from services import DataCollector, MemoryManager, AIOrchestrator
from core import DatabaseManager, create_context_logger
from utils import TokenRecorder

logger = create_context_logger()


class HourlyNewsAnalysisWorkflow:
    """每小时新闻分析工作流"""

    def __init__(
        self,
        data_collector: DataCollector,
        memory_manager: MemoryManager,
        ai_orchestrator: AIOrchestrator,
        db: DatabaseManager,
        test_mode: bool = False
    ):
        """
        初始化工作流

        Args:
            data_collector: 数据收集器
            memory_manager: 记忆管理器
            ai_orchestrator: AI 编排器
            db: Database Manager for token recording
            test_mode: 测试模式（不写入数据库）
        """
        self.data_collector = data_collector
        self.memory_manager = memory_manager
        self.ai_orchestrator = ai_orchestrator
        self.db = db
        self.token_recorder = TokenRecorder(db)
        self.test_mode = test_mode
    
    def run(self, agent_id: str) -> bool:
        """
        执行每小时新闻分析
        
        Args:
            agent_id: AI ID
            
        Returns:
            True 如果执行成功
        """
        logger.info(
            f"Starting hourly news analysis for {agent_id} (test_mode={self.test_mode})",
            extra={'details': {'workflow': 'hourly_news_analysis', 'agent_id': agent_id, 'test_mode': self.test_mode}}
        )
        
        try:
            # 1. 收集最近未分析的新闻
            logger.info("Step 1: Collecting unanalyzed news")
            news = self._collect_recent_news(agent_id)

            if not news:
                logger.info("No unanalyzed news, skipping analysis")
                return True
            
            logger.info(f"Collected {len(news)} news articles")
            
            # 2. 获取相关股票价格
            logger.info("Step 2: Fetching stock prices")
            prices = self._get_related_stock_prices(news)
            
            # 3. 获取当前持仓（用于判断影响）
            logger.info("Step 3: Loading portfolio")
            positions = self.data_collector.get_positions(agent_id)

            # 4. AI 分析新闻
            logger.info("Step 4: AI news analysis")
            analysis = self._analyze_news_with_fallback(agent_id, news, prices, positions)

            if not analysis:
                logger.error("AI analysis failed completely (including fallback)")
                return False
            
            # 5. 保存分析结果
            if self.test_mode:
                logger.info("Step 5: Skipping analysis save (test mode)")
                logger.info(
                    f"[TEST MODE] Would save analysis for {len(analysis.get('news_analysis', []))} news articles"
                )
            else:
                logger.info("Step 5: Saving analysis")
                success = self._save_analysis(agent_id, news, analysis)

                if not success:
                    logger.error("Failed to save analysis")
                    return False

            logger.info(
                f"Hourly news analysis completed for {agent_id}",
                extra={'details': {'news_count': len(news), 'test_mode': self.test_mode}}
            )
            
            return True
        
        except Exception as e:
            logger.error(
                f"Hourly news analysis failed: {e}",
                extra={'details': {'agent_id': agent_id}}
            )
            return False
    
    def _collect_recent_news(self, agent_id: str) -> List[Dict[str, Any]]:
        """
        收集最近未分析的新闻

        过滤条件：
        - 最近 2.5 小时内（以 fetched_at 计算）
        - 当前 agent 没有分析过
        - classification 不为 'irrelevant'

        Args:
            agent_id: AI ID

        Returns:
            新闻列表
        """
        try:
            # 使用新的 collect_unanalyzed_news 方法
            # 4 hour window to allow retries for failed JSON parsing
            news = self.data_collector.collect_unanalyzed_news(
                agent_id=agent_id,
                hours=4.0
            )

            return news

        except Exception as e:
            logger.error(f"Failed to collect news: {e}")
            return []
    
    def _get_related_stock_prices(self, news: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        获取新闻相关的股票价格
        
        Args:
            news: 新闻列表
            
        Returns:
            {symbol: price} 字典
        """
        try:
            # 提取所有相关股票
            symbols = set()
            for article in news:
                related_stocks = article.get('related_stocks') or []
                symbols.update(related_stocks)
            
            if not symbols:
                return {}
            
            # 获取价格
            return self.data_collector.collect_stock_prices(list(symbols))
        
        except Exception as e:
            logger.error(f"Failed to get stock prices: {e}")
            return {}
    
    def _analyze_news_with_fallback(
        self,
        agent_id: str,
        news: List[Dict[str, Any]],
        prices: Dict[str, float],
        positions: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        AI 分析新闻（带降级机制）

        先尝试批量分析所有新闻，如果失败则降级为逐条分析

        Args:
            agent_id: AI ID
            news: 新闻列表
            prices: 股票价格
            positions: 当前持仓

        Returns:
            分析结果
        """
        # 首先尝试批量分析 (使用 2 倍 timeout)
        logger.info(f"Attempting batch analysis for {len(news)} news articles")
        analysis = self._analyze_news(agent_id, news, prices, positions, timeout_multiplier=2.0)

        if analysis:
            logger.info("Batch analysis succeeded")
            return analysis

        # 批量分析失败，降级为逐条分析
        logger.warning(
            f"Batch analysis failed for {len(news)} articles, falling back to one-by-one analysis"
        )

        aggregated_analysis = {
            'news_analysis': [],
            'overall_market_sentiment': 'NEUTRAL',
            'key_themes': []
        }

        success_count = 0
        for i, article in enumerate(news, 1):
            logger.info(f"Analyzing article {i}/{len(news)} individually: {article.get('title', 'N/A')[:50]}")

            single_analysis = self._analyze_news(agent_id, [article], prices, positions)

            if single_analysis and single_analysis.get('news_analysis'):
                # 提取单条新闻的分析结果
                news_items = single_analysis['news_analysis']
                if news_items:
                    # 更新 news_index 为实际索引
                    for item in news_items:
                        item['news_index'] = i
                    aggregated_analysis['news_analysis'].extend(news_items)
                    success_count += 1
            else:
                logger.warning(f"Failed to analyze article {i}: {article.get('title', 'N/A')[:50]}")

        logger.info(
            f"One-by-one analysis completed: {success_count}/{len(news)} succeeded"
        )

        # 如果至少有一条成功，返回聚合结果
        if success_count > 0:
            return aggregated_analysis
        else:
            logger.error("All individual analyses failed")
            return None

    def _analyze_news(
        self,
        agent_id: str,
        news: List[Dict[str, Any]],
        prices: Dict[str, float],
        positions: List[Dict[str, Any]],
        timeout_multiplier: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        """
        AI 分析新闻 (with retry logic for JSON parsing failures)

        Args:
            agent_id: AI ID
            news: 新闻列表
            prices: 股票价格
            positions: 当前持仓
            timeout_multiplier: timeout 倍数 (批量分析时使用 2.0)

        Returns:
            分析结果
        """
        max_retries = 3
        current_timeout_multiplier = timeout_multiplier

        for attempt in range(max_retries):
            # 构建 Prompt
            prompt = self._build_news_analysis_prompt(news, prices, positions)

            # 重试时添加格式强调
            if attempt > 0:
                prompt = self._add_format_emphasis(prompt)
                # 重试时使用 2x timeout
                current_timeout_multiplier = timeout_multiplier * 2.0
                logger.info(f"Retry attempt {attempt + 1}/{max_retries} with format emphasis and {current_timeout_multiplier}x timeout")

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

            # 调用 AI（不检索 RAG）
            result = self.ai_orchestrator.call_single_agent(
                agent_id=agent_id,
                messages=messages,
                temperature=0.7,
                timeout_multiplier=current_timeout_multiplier
            )

            # Record token usage regardless of success (if we got a response)
            if result and result.get('usage'):
                self.token_recorder.record_from_usage(
                    agent_id=agent_id,
                    service='news_summary',
                    usage=result['usage']
                )

            if not result or not result['success']:
                logger.error(f"AI call failed: {result.get('error') if result else 'Unknown error'}")
                return None

            # 解析响应
            response_text = result['response']

            try:
                analysis = self.ai_orchestrator.parse_json_response(response_text)

                if analysis and isinstance(analysis, dict):
                    # 验证是否包含必要字段
                    if 'news_analysis' in analysis or 'raw_analysis' not in analysis:
                        return analysis

                # JSON 解析失败或格式不正确，准备重试
                if attempt < max_retries - 1:
                    logger.warning(
                        f"JSON parsing failed or invalid format on attempt {attempt + 1}, will retry with format emphasis"
                    )
                    continue
                else:
                    # 最后一次尝试仍然失败，返回原始响应
                    logger.error(f"All {max_retries} attempts failed to get valid JSON")
                    return {'raw_analysis': response_text}

            except Exception as e:
                logger.error(f"Failed to parse AI response: {e}")
                if attempt < max_retries - 1:
                    continue
                return {'raw_analysis': response_text}

        return None

    def _add_format_emphasis(self, prompt: str) -> str:
        """
        Add format emphasis to prompt for retry attempts.

        Args:
            prompt: Original prompt

        Returns:
            Prompt with format emphasis added
        """
        format_emphasis = """
## CRITICAL: JSON FORMAT REQUIREMENTS

Your response MUST be valid JSON. Follow these rules strictly:
1. Output ONLY the JSON object, no other text before or after
2. Use double quotes (") for all strings, NOT single quotes
3. Ensure all brackets and braces are properly closed
4. Do NOT include trailing commas after the last item in arrays or objects
5. Escape special characters in strings (use \\" for quotes, \\\\ for backslashes)
6. Do NOT truncate or abbreviate the response - complete the entire JSON structure

Example of CORRECT format:
{
  "news_analysis": [
    {
      "news_index": 1,
      "sentiment": "POSITIVE",
      "mentioned_stocks": ["AAPL"],
      "short_term_impact": "description here",
      "long_term_impact": "description here",
      "key_insights": "description here",
      "affects_my_portfolio": false,
      "confidence": 0.8
    }
  ],
  "overall_market_sentiment": "BULLISH",
  "key_themes": ["theme1", "theme2"]
}

"""
        return format_emphasis + prompt
    
    def _get_system_prompt(self) -> str:
        """
        获取系统 Prompt
        
        Returns:
            系统 Prompt
        """
        return """You are a professional financial news analyst, focused on quickly understanding the impact of news on stocks.

Your tasks:
1. Quickly read and understand each news article.
2. Assess news sentiment (POSITIVE/NEGATIVE/NEUTRAL/MIXED).
3. Predict the short-term and long-term impact on relevant stocks.
4. Identify the core information behind the news.

Important Notes:

- This is an hourly routine analysis; no trading decisions are required.

- Focus on the substantive content of the news, not sensationalist headlines.

- Distinguish between short-term fluctuations and long-term trends.

- If the news is relevant to your holdings, please mark it clearly.

Please reply in JSON format."""
    
    def _build_news_analysis_prompt(
        self,
        news: List[Dict[str, Any]],
        prices: Dict[str, float],
        positions: List[Dict[str, Any]]
    ) -> str:
        """
        构建新闻分析 Prompt
        
        Args:
            news: 新闻列表
            prices: 股票价格
            positions: 当前持仓
            
        Returns:
            Prompt 文本
        """
        parts = ["# Hourly News Analysis Task", ""]
        
        # 当前持仓（用于判断影响）
        if positions:
            parts.append("## Your Current Holdings")
            parts.append("")
            for pos in positions:
                parts.append(f"- {pos['symbol']}: {pos['quantity']} 股 ({pos['position_type']})")
            parts.append("")
        
        # 新闻列表
        parts.append("## Recent news in the last hour")
        parts.append("")
        
        for i, article in enumerate(news, 1):
            parts.append(f"### News {i}")
            parts.append(f"**Title**: {article['title']}")
            parts.append(f"**Source**: {article['source']}")
            parts.append(f"**Related Stocks**: {', '.join(article.get('related_stocks') or [])}")
            
            # 添加股价信息
            related_stocks = article.get('related_stocks') or []
            if related_stocks:
                price_info = []
                for symbol in related_stocks:
                    price = prices.get(symbol)
                    if price:
                        price_info.append(f"{symbol}: ${price:.2f}")
                
                if price_info:
                    parts.append(f"**Current Price**: {', '.join(price_info)}")
            
            parts.append(f"**Content Summary**: {article['content'][:300]}...")
            parts.append("")
        
        # 任务说明
        parts.append("## Your Task")
        parts.append("")
        parts.append("Analyze each news article, output JSON format:")
        parts.append("```json")
        parts.append("{")
        parts.append('  "news_analysis": [')
        parts.append('    {')
        parts.append('      "news_index": 1,')
        parts.append('      "sentiment": "POSITIVE",')
        parts.append('      "mentioned_stocks": ["NVDA"],')
        parts.append('      "short_term_impact": "Stock price may increase by 2-3%",')
        parts.append('      "long_term_impact": "Strengthen AI leadership position",')
        parts.append('      "key_insights": "New product launch exceeded expectations",')
        parts.append('      "affects_my_portfolio": true,')
        parts.append('      "confidence": 0.8')
        parts.append('    },')
        parts.append('    ...')
        parts.append('  ],')
        parts.append('  "overall_market_sentiment": "BULLISH",')
        parts.append('  "key_themes": ["AI Chips", "Cloud Computing"]')
        parts.append("}")
        parts.append("```")
        
        return "\n".join(parts)
    
    def _parse_confidence(self, value) -> float:
        """
        Parse confidence value from AI response, handling malformed values.

        Args:
            value: Raw confidence value (could be float, int, or malformed string)

        Returns:
            Cleaned confidence value as float (0.0 to 1.0)
        """
        if isinstance(value, (int, float)):
            return max(0.0, min(1.0, float(value)))

        if isinstance(value, str):
            import re
            # Extract numeric value from string like "pg0.95", "0.8", "80%"
            match = re.search(r'(\d+\.?\d*)', value)
            if match:
                num = float(match.group(1))
                # If > 1, assume it's a percentage
                if num > 1:
                    num = num / 100.0
                return max(0.0, min(1.0, num))

        return 0.5  # Default fallback

    def _save_analysis(
        self,
        agent_id: str,
        news: List[Dict[str, Any]],
        analysis: Dict[str, Any]
    ) -> bool:
        """
        保存分析结果

        Args:
            agent_id: AI ID
            news: 新闻列表
            analysis: 分析结果

        Returns:
            True 如果保存成功
        """
        try:
            news_analyses = analysis.get('news_analysis', [])

            # 保存每条新闻的分析
            for news_analysis in news_analyses:
                news_index = news_analysis.get('news_index', 1) - 1

                if news_index >= len(news):
                    continue

                article = news[news_index]

                query = """
                    INSERT INTO hourly_news_analysis (
                        agent_id,
                        news_id,
                        analysis,
                        sentiment,
                        mentioned_stocks,
                        impact_prediction,
                        confidence_score
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                # 构建分析文本
                analysis_text = f"""
Short-term impact: {news_analysis.get('short_term_impact', 'N/A')}
Long-term impact: {news_analysis.get('long_term_impact', 'N/A')}
Key insights: {news_analysis.get('key_insights', 'N/A')}
"""

                # Clean and validate confidence score
                confidence = self._parse_confidence(news_analysis.get('confidence', 0.5))

                self.data_collector.db.execute_update(
                    query,
                    (
                        agent_id,
                        article['news_id'],
                        analysis_text,
                        news_analysis.get('sentiment', 'NEUTRAL'),
                        news_analysis.get('mentioned_stocks', []),
                        news_analysis.get('short_term_impact', ''),
                        confidence
                    )
                )
            
            saved_count = len(news_analyses)
            logger.info(
                f"Saved {saved_count} news analyses out of {len(news)} total news articles"
            )

            # 警告：如果有新闻没有被分析
            if saved_count < len(news):
                logger.warning(
                    f"{len(news) - saved_count} news articles were not analyzed or saved"
                )

            return True

        except Exception as e:
            logger.error(f"Failed to save analysis: {e}")
            return False