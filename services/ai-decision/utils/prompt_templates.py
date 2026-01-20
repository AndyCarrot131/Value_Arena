"""
Prompt Templates
AI prompt templates for different workflows
"""

from typing import Dict, Any


class PromptTemplates:
    """AI Prompt template manager"""

    @staticmethod
    def get_news_analysis_system_prompt() -> str:
        """
        Get news analysis system prompt
        
        Returns:
            System prompt text
        """
        return """You are a professional financial news analyst, focused on quickly understanding the impact of news on stocks.

Your tasks:
1. Quickly read and understand each news article
2. Determine news sentiment (POSITIVE/NEGATIVE/NEUTRAL/MIXED)
3. Predict short-term and long-term impact on related stocks
4. Identify core information behind the news

Important notes:
- This is routine hourly analysis, no trading decisions needed
- Focus on news substance, not clickbait headlines
- Distinguish between short-term volatility and long-term trends
- If news relates to your positions, specially mark it

Please respond in JSON format."""
    
    @staticmethod
    def get_daily_summary_system_prompt() -> str:
        """
        Get daily summary system prompt
        
        Returns:
            System prompt text
        """
        return """You are a professional portfolio manager who reviews daily market dynamics and portfolio performance every evening.

Your tasks:
1. Summarize today's key market events and trends
2. Review your portfolio performance and today's trades
3. Analyze whether today's operations align with long-term strategy
4. Look ahead to tomorrow's market and possible operations

Important notes:
- Focus on long-term value, not short-term volatility
- Learn from similar historical cases
- Stay rational, avoid emotional decisions
- Remember your fund allocation: 70% long-term + 30% short-term

Please summarize in natural paragraph format (not bullet points or JSON)."""
    
    @staticmethod
    def get_trading_decision_system_prompt() -> str:
        """
        Get trading decision system prompt
        
        Returns:
            System prompt text
        """
        return """You are a professional value investor focused on long-term holdings of quality assets.

Investment Rules (must strictly follow):
1. Stock Pool: Only trade 20 designated stocks
2. Trading Frequency: Maximum 5 trades per week (all trades cumulative)
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

Please respond in JSON format. If no suitable opportunity, return HOLD."""

    @staticmethod
    def format_portfolio_summary(positions: list) -> str:
        """
        Format portfolio summary
        
        Args:
            positions: Position list
            
        Returns:
            Formatted text
        """
        if not positions:
            return "No current positions"
        
        lines = ["Current Positions:"]
        for pos in positions:
            lines.append(f"- {pos['symbol']}: {pos['quantity']} shares ({pos['position_type']})")
            lines.append(f"  Cost Basis: ${float(pos['average_cost']):.2f}")
            if 'unrealized_pnl' in pos:
                lines.append(f"  Unrealized P&L: ${float(pos['unrealized_pnl']):.2f}")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_market_environment(env: Dict[str, Any]) -> str:
        """
        Format market environment
        
        Args:
            env: Market environment dictionary
            
        Returns:
            Formatted text
        """
        lines = ["Current Market Environment:"]
        lines.append(f"- S&P 500 Trend: {env.get('sp500_trend', 'UNKNOWN')}")
        lines.append(f"- VIX Level: {env.get('vix_level', 'UNKNOWN')}")
        
        if 'sector_rotation' in env:
            lines.append(f"- Sector Rotation: {env['sector_rotation']}")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_news_summary(news: list, limit: int = 5) -> str:
        """
        Format news summary
        
        Args:
            news: News list
            limit: Maximum number of news items
            
        Returns:
            Formatted text
        """
        if not news:
            return "No recent news"
        
        lines = ["Recent News:"]
        for i, article in enumerate(news[:limit], 1):
            lines.append(f"{i}. {article['title']}")
            if 'source' in article:
                lines.append(f"   Source: {article['source']}")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_wallet_status(wallet: Dict[str, Any]) -> str:
        """
        Format wallet status
        
        Args:
            wallet: Wallet dictionary
            
        Returns:
            Formatted text
        """
        lines = ["Wallet Status:"]
        lines.append(f"- Total Cash: ${float(wallet.get('cash_balance', 0)):.2f}")
        lines.append(f"- Long-term Account: ${float(wallet.get('long_term_cash', 0)):.2f} (70%)")
        lines.append(f"- Short-term Account: ${float(wallet.get('short_term_cash', 0)):.2f} (30%)")
        
        return "\n".join(lines)
    
    @staticmethod
    def format_trade_quota(quota: Dict[str, Any]) -> str:
        """
        Format trade quota
        
        Args:
            quota: Trade quota dictionary
            
        Returns:
            Formatted text
        """
        used = quota.get('used', 0)
        limit = quota.get('limit', 5)
        week = quota.get('week', 'N/A')
        
        return f"Weekly Trade Quota ({week}): {used}/{limit}"
    
    @staticmethod
    def build_json_response_template(decision_type: str) -> str:
        """
        Build JSON response template
        
        Args:
            decision_type: Decision type (trading/learning/analysis)
            
        Returns:
            JSON template text
        """
        if decision_type == 'trading':
            return """```json
{
  "decision_type": "BUY",  // BUY/SELL/HOLD
  "symbol": "NVDA",
  "quantity": 10,
  "price": 450.23,
  "position_type": "LONG_TERM",  // LONG_TERM/SHORT_TERM
  "reasoning": "Detailed reasoning...",
  "confidence": 0.85,
  "market_context": {
    "sp500_trend": "BULL",
    "vix_level": "MEDIUM"
  }
}
```"""
        
        elif decision_type == 'learning':
            return """```json
{
  "analysis": [
    {
      "symbol": "NVDA",
      "rating": "A+",
      "hold_type": "LONG_TERM",
      "reasoning": "..."
    }
  ],
  "overall_market_view": "...",
  "learning_insights": "..."
}
```"""
        
        elif decision_type == 'news_analysis':
            return """```json
{
  "news_analysis": [
    {
      "news_index": 1,
      "sentiment": "POSITIVE",
      "mentioned_stocks": ["NVDA"],
      "short_term_impact": "...",
      "long_term_impact": "...",
      "key_insights": "...",
      "confidence": 0.8
    }
  ],
  "overall_market_sentiment": "BULLISH"
}
```"""
        
        else:
            return "{}"