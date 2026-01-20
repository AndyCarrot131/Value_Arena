"""
Token Counter
Estimate token count for AI context optimization
"""

import re
from typing import List, Dict, Any


class TokenCounter:
    """Token counter for context optimization"""
    
    # Approximate tokens per character (English text average)
    CHARS_PER_TOKEN = 4
    
    @staticmethod
    def estimate_tokens(text: str) -> int:
        """
        Estimate token count for text
        
        Args:
            text: Input text
            
        Returns:
            Estimated token count
        """
        if not text:
            return 0
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Rough estimate: 1 token ≈ 4 characters
        char_count = len(text)
        
        # Adjust for different content types
        # Code typically uses more tokens per character
        if '```' in text or 'def ' in text or 'class ' in text:
            return int(char_count / 3)
        
        # JSON uses more tokens due to special characters
        if text.strip().startswith('{') or text.strip().startswith('['):
            return int(char_count / 3.5)
        
        # Natural language
        return int(char_count / TokenCounter.CHARS_PER_TOKEN)
    
    @staticmethod
    def estimate_messages_tokens(messages: List[Dict[str, str]]) -> int:
        """
        Estimate total tokens for message list
        
        Args:
            messages: Message list [{"role": "...", "content": "..."}, ...]
            
        Returns:
            Estimated total tokens
        """
        total = 0
        
        for message in messages:
            # Role overhead (approximately 4 tokens per message)
            total += 4
            
            # Content tokens
            content = message.get('content', '')
            total += TokenCounter.estimate_tokens(content)
        
        return total
    
    @staticmethod
    def truncate_to_token_limit(text: str, max_tokens: int) -> str:
        """
        Truncate text to fit token limit
        
        Args:
            text: Input text
            max_tokens: Maximum token count
            
        Returns:
            Truncated text
        """
        current_tokens = TokenCounter.estimate_tokens(text)
        
        if current_tokens <= max_tokens:
            return text
        
        # Calculate approximate character limit
        target_chars = max_tokens * TokenCounter.CHARS_PER_TOKEN
        
        # Add buffer for safety
        target_chars = int(target_chars * 0.9)
        
        if len(text) <= target_chars:
            return text
        
        # Truncate at word boundary
        truncated = text[:target_chars]
        last_space = truncated.rfind(' ')
        
        if last_space > 0:
            truncated = truncated[:last_space]
        
        return truncated + "..."
    
    @staticmethod
    def optimize_context(
        system_prompt: str,
        user_prompt: str,
        max_total_tokens: int = 8000
    ) -> tuple:
        """
        Optimize context to fit token limit
        
        Args:
            system_prompt: System prompt
            user_prompt: User prompt
            max_total_tokens: Maximum total tokens
            
        Returns:
            (optimized_system_prompt, optimized_user_prompt)
        """
        system_tokens = TokenCounter.estimate_tokens(system_prompt)
        user_tokens = TokenCounter.estimate_tokens(user_prompt)
        total_tokens = system_tokens + user_tokens
        
        # If within limit, no optimization needed
        if total_tokens <= max_total_tokens:
            return system_prompt, user_prompt
        
        # Preserve system prompt, truncate user prompt
        available_for_user = max_total_tokens - system_tokens - 100  # 100 token buffer
        
        if available_for_user < 1000:
            # System prompt too long, need to truncate both
            system_limit = int(max_total_tokens * 0.3)
            user_limit = int(max_total_tokens * 0.7)
            
            optimized_system = TokenCounter.truncate_to_token_limit(system_prompt, system_limit)
            optimized_user = TokenCounter.truncate_to_token_limit(user_prompt, user_limit)
        else:
            optimized_system = system_prompt
            optimized_user = TokenCounter.truncate_to_token_limit(user_prompt, available_for_user)
        
        return optimized_system, optimized_user
    
    @staticmethod
    def estimate_ai_state_tokens(ai_state: Dict[str, Any]) -> int:
        """
        Estimate tokens for AI state
        
        Args:
            ai_state: AI state dictionary
            
        Returns:
            Estimated token count
        """
        total = 0
        
        # Portfolio summary
        if 'portfolio_summary' in ai_state:
            portfolio_str = str(ai_state['portfolio_summary'])
            total += TokenCounter.estimate_tokens(portfolio_str)
        
        # Investment thesis
        if 'investment_thesis' in ai_state:
            total += TokenCounter.estimate_tokens(ai_state['investment_thesis'])
        
        # Market view
        if 'market_view' in ai_state:
            total += TokenCounter.estimate_tokens(ai_state['market_view'])
        
        # Other fields (approximately)
        total += 50
        
        return total
    
    @staticmethod
    def estimate_key_events_tokens(events: List[Dict[str, Any]]) -> int:
        """
        Estimate tokens for key events list
        
        Args:
            events: Key events list
            
        Returns:
            Estimated token count
        """
        total = 0
        
        for event in events:
            # Event type and symbol (approximately 10 tokens)
            total += 10
            
            # Description
            if 'description' in event:
                total += TokenCounter.estimate_tokens(event['description'])
            
            # Impact
            if 'impact' in event:
                total += TokenCounter.estimate_tokens(event['impact'])
        
        return total
    
    @staticmethod
    def estimate_rag_results_tokens(results: List[Dict[str, Any]]) -> int:
        """
        Estimate tokens for RAG retrieval results
        
        Args:
            results: RAG results list
            
        Returns:
            Estimated token count
        """
        total = 0
        
        for result in results:
            # Metadata overhead (approximately 20 tokens)
            total += 20
            
            # Content
            if 'content' in result:
                total += TokenCounter.estimate_tokens(result['content'])
        
        return total
    
    @staticmethod
    def build_context_summary(
        ai_state_tokens: int,
        key_events_tokens: int,
        rag_tokens: int,
        other_tokens: int = 1000
    ) -> Dict[str, Any]:
        """
        Build context usage summary
        
        Args:
            ai_state_tokens: AI state tokens
            key_events_tokens: Key events tokens
            rag_tokens: RAG results tokens
            other_tokens: Other content tokens (prompt, etc.)
            
        Returns:
            Summary dictionary
        """
        total = ai_state_tokens + key_events_tokens + rag_tokens + other_tokens
        
        return {
            'total_tokens': total,
            'ai_state': ai_state_tokens,
            'key_events': key_events_tokens,
            'rag_results': rag_tokens,
            'other': other_tokens,
            'breakdown': {
                'ai_state_pct': round(ai_state_tokens / total * 100, 1) if total > 0 else 0,
                'key_events_pct': round(key_events_tokens / total * 100, 1) if total > 0 else 0,
                'rag_results_pct': round(rag_tokens / total * 100, 1) if total > 0 else 0,
                'other_pct': round(other_tokens / total * 100, 1) if total > 0 else 0
            }
        }