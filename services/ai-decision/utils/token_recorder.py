"""
Token Recorder
Record AI token usage to database for monitoring and cost tracking
"""

from typing import Dict, Any, Optional
from datetime import date
from core import DatabaseManager, create_context_logger
from .timezone_utils import get_et_today

logger = create_context_logger()


class TokenRecorder:
    """Record token usage to ai_token_records table"""

    # Valid service names
    VALID_SERVICES = {'daily_summary', 'news_summary', 'weekly_summary', 'trading_decision'}

    def __init__(self, db: DatabaseManager):
        """
        Initialize the token recorder

        Args:
            db: Database manager instance
        """
        self.db = db

    def record(
        self,
        agent_id: str,
        service: str,
        token_in: int,
        token_out: int,
        record_date: Optional[date] = None
    ) -> bool:
        """
        Record token usage for a service call

        Args:
            agent_id: AI agent ID (e.g., 'claude', 'gpt')
            service: Service name (daily_summary, news_summary, weekly_summary, trading_decision)
            token_in: Input tokens (prompt tokens)
            token_out: Output tokens (completion tokens)
            record_date: Date of record (defaults to today)

        Returns:
            True if recorded successfully
        """
        if service not in self.VALID_SERVICES:
            logger.error(f"Invalid service name: {service}. Must be one of {self.VALID_SERVICES}")
            return False

        if record_date is None:
            record_date = get_et_today()

        try:
            # Use UPSERT to accumulate tokens for the same date/agent/service
            query = """
                INSERT INTO ai_token_records (
                    record_date,
                    agent_id,
                    service,
                    token_in,
                    token_out,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (record_date, agent_id, service) DO UPDATE SET
                    token_in = ai_token_records.token_in + EXCLUDED.token_in,
                    token_out = ai_token_records.token_out + EXCLUDED.token_out,
                    updated_at = CURRENT_TIMESTAMP
            """

            self.db.execute_update(
                query,
                (record_date, agent_id, service, token_in, token_out)
            )

            logger.info(
                f"Token usage recorded: {agent_id}/{service} - in:{token_in}, out:{token_out}",
                extra={'details': {
                    'agent_id': agent_id,
                    'service': service,
                    'token_in': token_in,
                    'token_out': token_out,
                    'date': str(record_date)
                }}
            )

            return True

        except Exception as e:
            logger.error(f"Failed to record token usage: {e}")
            return False

    def record_from_usage(
        self,
        agent_id: str,
        service: str,
        usage: Dict[str, Any],
        record_date: Optional[date] = None
    ) -> bool:
        """
        Record token usage from API response usage dict

        Args:
            agent_id: AI agent ID
            service: Service name
            usage: Usage dict from API response (e.g., {'prompt_tokens': 100, 'completion_tokens': 50})
            record_date: Date of record (defaults to today)

        Returns:
            True if recorded successfully
        """
        if not usage:
            logger.warning(f"Empty usage dict for {agent_id}/{service}, skipping record")
            return False

        # Handle different API response formats
        # OpenAI/Anthropic format: prompt_tokens, completion_tokens
        # Some APIs use: input_tokens, output_tokens
        token_in = usage.get('prompt_tokens') or usage.get('input_tokens') or 0
        token_out = usage.get('completion_tokens') or usage.get('output_tokens') or 0

        if token_in == 0 and token_out == 0:
            logger.warning(f"No token counts found in usage dict for {agent_id}/{service}")
            return False

        return self.record(
            agent_id=agent_id,
            service=service,
            token_in=token_in,
            token_out=token_out,
            record_date=record_date
        )

    def get_daily_summary(
        self,
        record_date: Optional[date] = None
    ) -> Dict[str, Dict[str, int]]:
        """
        Get daily token usage summary

        Args:
            record_date: Date to query (defaults to today)

        Returns:
            {service: {'token_in': int, 'token_out': int, 'total': int}, ...}
        """
        if record_date is None:
            record_date = get_et_today()

        try:
            query = """
                SELECT service, SUM(token_in) as token_in, SUM(token_out) as token_out
                FROM ai_token_records
                WHERE record_date = %s
                GROUP BY service
            """

            results = self.db.execute_query(query, (record_date,))

            summary = {}
            for row in results or []:
                service = row['service']
                token_in = int(row['token_in'])
                token_out = int(row['token_out'])
                summary[service] = {
                    'token_in': token_in,
                    'token_out': token_out,
                    'total': token_in + token_out
                }

            return summary

        except Exception as e:
            logger.error(f"Failed to get daily token summary: {e}")
            return {}

    def get_agent_daily_usage(
        self,
        agent_id: str,
        record_date: Optional[date] = None
    ) -> Dict[str, Dict[str, int]]:
        """
        Get daily token usage for a specific agent

        Args:
            agent_id: AI agent ID
            record_date: Date to query (defaults to today)

        Returns:
            {service: {'token_in': int, 'token_out': int, 'total': int}, ...}
        """
        if record_date is None:
            record_date = get_et_today()

        try:
            query = """
                SELECT service, token_in, token_out
                FROM ai_token_records
                WHERE record_date = %s AND agent_id = %s
            """

            results = self.db.execute_query(query, (record_date, agent_id))

            summary = {}
            for row in results or []:
                service = row['service']
                token_in = int(row['token_in'])
                token_out = int(row['token_out'])
                summary[service] = {
                    'token_in': token_in,
                    'token_out': token_out,
                    'total': token_in + token_out
                }

            return summary

        except Exception as e:
            logger.error(f"Failed to get agent daily usage: {e}")
            return {}
