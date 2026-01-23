"""
RAG Retriever Service
Retrieve similar historical decisions using OpenSearch k-NN
"""

from typing import List, Dict, Any, Optional
from core import BedrockClient, OpenSearchClient, create_context_logger

logger = create_context_logger()


class RAGRetriever:
    """RAG retrieval service"""

    def __init__(self, opensearch_client: OpenSearchClient, bedrock_client: BedrockClient):
        """
        Initialize the RAG retriever

        Args:
            opensearch_client: OpenSearch client (for k-NN search)
            bedrock_client: Bedrock client (for generating query embeddings)
        """
        self.opensearch = opensearch_client
        self.bedrock = bedrock_client
    
    def retrieve_similar_decisions(
        self,
        context: Dict[str, Any],
        num_results: int = 10,
        filter_by_agent: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Retrieve similar historical decisions
        
        Args:
            context: query context
                - agent_id: AI ID
                - market_environment: market context {'sp500_trend': str, 'vix_level': str, ...}
                - portfolio: current positions [{'symbol': str, 'quantity': int, ...}, ...]
                - considering_symbol: stock under consideration (optional)
                - recent_news: recent news summary (optional)
            num_results: number of results to return
            filter_by_agent: whether to retrieve only this AI's historical decisions
            
        Returns:
            Similar decisions [{'content': str, 'score': float, 'metadata': dict}, ...]
        """
        logger.info(
            f"Retrieving similar decisions for {context.get('agent_id')}",
            extra={'details': {'num_results': num_results, 'filter_by_agent': filter_by_agent}}
        )
        
        # Build query text
        query_text = self._build_query_text(context)

        # Retrieve
        try:
            # Generate query embedding
            query_vector = self.bedrock.generate_embedding(query_text)

            # Build filter
            filter_conditions = None
            if filter_by_agent:
                filter_conditions = {'term': {'agent_id': context.get('agent_id')}}

            # k-NN search
            results = self.opensearch.knn_search(
                query_vector=query_vector,
                filter_conditions=filter_conditions,
                num_results=num_results
            )

            logger.info(
                f"Retrieved {len(results)} similar decisions",
                extra={'details': {
                    'avg_score': sum(r['score'] for r in results) / len(results) if results else 0
                }}
            )

            return results

        except Exception as e:
            logger.error(f"Failed to retrieve similar decisions: {e}")
            return []
    
    def _build_query_text(self, context: Dict[str, Any]) -> str:
        """
        Build the query text
        
        Args:
            context: query context
            
        Returns:
            Query text
        """
        parts = []
        
        # Market environment
        market_env = context.get('market_environment', {})
        if market_env:
            parts.append("current market environment:")
            parts.append(f"- S&P 500 trend: {market_env.get('sp500_trend', 'UNKNOWN')}")
            parts.append(f"- VIX level: {market_env.get('vix_level', 'UNKNOWN')}")
            
            if 'sector_rotation' in market_env:
                parts.append(f"- sector rotation: {market_env['sector_rotation']}")
            
            parts.append("")
        
        # Current positions
        portfolio = context.get('portfolio', [])
        if portfolio:
            parts.append("my current portfolio:")
            for position in portfolio:
                symbol = position.get('symbol')
                quantity = position.get('quantity')
                position_type = position.get('position_type', 'UNKNOWN')
                parts.append(f"- {symbol}: {quantity} shares ({position_type})")
            parts.append("")
        
        # Stock under consideration
        considering = context.get('considering_symbol')
        if considering:
            parts.append(f"I am considering trading stock {considering}")
            parts.append("")
        
        # Recent news summary
        recent_news = context.get('recent_news')
        if recent_news:
            parts.append("recent relevant news summary:")
            parts.append(recent_news)
            parts.append("")
        
        # Current task
        task = context.get('task')
        if task:
            parts.append(f"current task: {task}")
            parts.append("")
        
        return "\n".join(parts)
    
    def retrieve_for_daily_summary(
        self,
        agent_id: str,
        today_summary: str,
        market_environment: Dict[str, Any],
        num_results: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Retrieve similar historical daily summaries (helper)
        
        Args:
            agent_id: AI ID
            today_summary: brief summary for today
            market_environment: market environment
            num_results: number of results to return
            
        Returns:
            List of similar daily summaries
        """
        context = {
            'agent_id': agent_id,
            'market_environment': market_environment,
            'task': f"generate daily summary. today summary: {today_summary}"
        }
        
        return self.retrieve_similar_decisions(
            context=context,
            num_results=num_results,
            filter_by_agent=True  # Retrieve only this AI's history
        )
    
    def retrieve_for_trading_decision(
        self,
        agent_id: str,
        symbol: str,
        portfolio: List[Dict[str, Any]],
        market_environment: Dict[str, Any],
        recent_news: str,
        num_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Retrieve similar historical decisions for a trading decision (helper)
        
        Args:
            agent_id: AI ID
            symbol: stock under consideration
            portfolio: current positions
            market_environment: market environment
            recent_news: recent news summary
            num_results: number of results to return
            
        Returns:
            Similar decision list
        """
        context = {
            'agent_id': agent_id,
            'considering_symbol': symbol,
            'portfolio': portfolio,
            'market_environment': market_environment,
            'recent_news': recent_news,
            'task': f"decision to trade or not {symbol}"
        }
        
        # Retrieve historical decisions from all AIs (learn from others)
        all_results = self.retrieve_similar_decisions(
            context=context,
            num_results=num_results,
            filter_by_agent=False
        )
        
        # Retrieve this AI's own history (self-reflection)
        self_results = self.retrieve_similar_decisions(
            context=context,
            num_results=5,
            filter_by_agent=True
        )
        
        # Merge results (deduplicate)
        combined = []
        seen_ids = set()
        
        for result in all_results + self_results:
            decision_id = result.get('metadata', {}).get('decision_id')
            if decision_id and decision_id not in seen_ids:
                combined.append(result)
                seen_ids.add(decision_id)
        
        # Sort by similarity
        combined.sort(key=lambda x: x['score'], reverse=True)

        return combined[:num_results]

    def format_results_for_prompt(
        self,
        results: List[Dict[str, Any]],
        max_cases: int = 5
    ) -> str:
        """
        Format retrieval results as prompt text
        
        Args:
            results: list of retrieval results
            max_cases: maximum number of cases to include
            
        Returns:
            Formatted text
        """
        if not results:
            return "No similar historical cases found."
        
        parts = ["Similar historical cases:", ""]
        
        for i, result in enumerate(results[:max_cases], 1):
            score = result.get('score', 0.0)
            content = result.get('content', '')
            metadata = result.get('metadata', {})
            
            parts.append(f"Case {i} (similarity: {score:.2f}):")
            
            # Add metadata
            if 'symbol' in metadata:
                parts.append(f"  Symbol: {metadata['symbol']}")
            if 'decision_type' in metadata:
                parts.append(f"  Decision type: {metadata['decision_type']}")
            
            # Add content
            parts.append(f"  Decision reasoning:\n{content}")
            parts.append("")

        return "\n".join(parts)

    def retrieve_stock_memories(
        self,
        agent_id: str,
        symbol: str,
        num_results: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Retrieve this agent's historical memories for a specific stock
        Used to generate daily single-stock analysis
    
        Args:
            agent_id: AI ID
            symbol: stock symbol
            num_results: number of results to return
    
        Returns:
            Historical memories [{'content': str, 'score': float, 'metadata': dict}, ...]
        """
        logger.info(
            f"Retrieving stock memories for {agent_id} - {symbol}",
            extra={'details': {'symbol': symbol, 'num_results': num_results}}
        )

        # Build query text: historical analysis and decisions for the specific stock
        query_text = f"""
Retrieve my previous analysis and decisions about {symbol}.
I want to understand:
- My past investment thesis on this company
- Previous trading decisions and their outcomes
- Key events and news I analyzed before
- My sentiment evolution over time
"""

        try:
            # Generate query embedding
            query_vector = self.bedrock.generate_embedding(query_text)

            # Build filter: agent_id + symbol
            filter_conditions = {
                'bool': {
                    'must': [
                        {'term': {'agent_id': agent_id}},
                        {'term': {'symbol': symbol}}
                    ]
                }
            }

            # k-NN search
            results = self.opensearch.knn_search(
                query_vector=query_vector,
                filter_conditions=filter_conditions,
                num_results=num_results
            )

            logger.info(
                f"Retrieved {len(results)} stock memories for {symbol}",
                extra={'details': {
                    'avg_score': sum(r['score'] for r in results) / len(results) if results else 0
                }}
            )

            return results

        except Exception as e:
            logger.error(f"Failed to retrieve stock memories: {e}")
            return []

    def retrieve_recent_stock_daily_summaries(
        self,
        agent_id: str,
        symbol: str,
        days: int = 7,
        num_results: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Retrieve recent STOCK_DAILY_SUMMARY RAG entries for a symbol (time bounded)

        Args:
            agent_id: AI ID
            symbol: Stock/ETF symbol
            days: Lookback window in days
            num_results: Max records to return

        Returns:
            List of summaries with metadata
        """
        logger.info(
            f"Retrieving recent STOCK_DAILY_SUMMARY memories for {symbol}",
            extra={'details': {'symbol': symbol, 'days': days, 'num_results': num_results}}
        )

        query_text = f"Retrieve my daily stock summaries for {symbol} over the past {days} days."

        try:
            # Generate query embedding
            query_vector = self.bedrock.generate_embedding(query_text)

            # Build filter: agent + symbol + type
            filter_conditions = {
                'bool': {
                    'must': [
                        {'term': {'agent_id': agent_id}},
                        {'term': {'symbol': symbol}},
                        {'term': {'metadata.type': 'stock_daily_summary'}}
                    ]
                }
            }

            # k-NN search
            results = self.opensearch.knn_search(
                query_vector=query_vector,
                filter_conditions=filter_conditions,
                num_results=num_results
            )

            logger.info(
                f"Retrieved {len(results)} recent daily summaries for {symbol}",
                extra={'details': {
                    'avg_score': sum(r['score'] for r in results) / len(results) if results else 0
                }}
            )

            return results

        except Exception as e:
            logger.error(f"Failed to retrieve recent daily summaries: {e}")
            return []

    def retrieve_latest_stock_weekly_summary(
        self,
        agent_id: str,
        symbol: str,
        num_results: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Retrieve the latest STOCK_WEEKLY_SUMMARY entry for a symbol from RAG
        """
        logger.info(
            f"Retrieving latest STOCK_WEEKLY_SUMMARY for {symbol}",
            extra={'details': {'symbol': symbol, 'num_results': num_results}}
        )

        query_text = f"Retrieve my latest weekly stock summary for {symbol}."

        try:
            # Generate query embedding
            query_vector = self.bedrock.generate_embedding(query_text)

            # Build filter: agent + symbol + type
            filter_conditions = {
                'bool': {
                    'must': [
                        {'term': {'agent_id': agent_id}},
                        {'term': {'symbol': symbol}},
                        {'term': {'metadata.type': 'stock_weekly_summary'}}
                    ]
                }
            }

            # k-NN search
            results = self.opensearch.knn_search(
                query_vector=query_vector,
                filter_conditions=filter_conditions,
                num_results=num_results
            )

            logger.info(
                f"Retrieved {len(results)} weekly summaries for {symbol}",
                extra={'details': {
                    'avg_score': sum(r['score'] for r in results) / len(results) if results else 0
                }}
            )

            return results

        except Exception as e:
            logger.error(f"Failed to retrieve weekly summaries: {e}")
            return []

    def format_stock_memories_for_prompt(
        self,
        results: List[Dict[str, Any]],
        max_memories: int = 3
    ) -> str:
        """
        Format stock memories as prompt text

        Args:
            results: retrieval result list
            max_memories: maximum number of memories to include

        Returns:
            Formatted text
        """
        if not results:
            return "No previous analysis found for this stock."

        parts = ["Your previous analysis:", ""]

        for i, result in enumerate(results[:max_memories], 1):
            score = result.get('score', 0.0)
            content = result.get('content', '')
            metadata = result.get('metadata', {})

            # Extract date and decision type
            date = metadata.get('date', 'Unknown date')
            decision_type = metadata.get('type', metadata.get('decision_type', 'Analysis'))

            parts.append(f"Memory {i} ({date}, {decision_type}):")
            parts.append(content[:200])  # Limit length to avoid excessive tokens
            parts.append("")

        return "\n".join(parts)
