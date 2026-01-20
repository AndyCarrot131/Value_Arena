"""
AI Orchestrator Service
Orchestrate sequential calls to multiple AIs (Claude → GPT → Gemini)
"""

from typing import List, Dict, Any, Optional
import time
from core import DatabaseManager, AIClient, create_context_logger
from config import get_settings

logger = create_context_logger()


class AIOrchestrator:
    """AI orchestration service"""

    def __init__(self, db: DatabaseManager, ai_client: AIClient = None):
        """
        Initialize the orchestrator

        Args:
            db: database manager
            ai_client: AI client (deprecated, kept for backward compatibility)
        """
        self.db = db
        self.ai_client = ai_client  # Kept but no longer used
        self.settings = get_settings()
        self._client_cache = {}  # Cache AIClient instances: {(api_url, api_key): AIClient}

    def _get_client_for_agent(self, agent: Dict[str, Any]) -> AIClient:
        """
        Get or create an AIClient for the given agent

        Args:
            agent: agent config dict (includes api_url, api_key_env)

        Returns:
            AIClient instance
        """
        api_url = agent['api_url']
        api_key_env = agent['api_key_env']

        # Normalize API URL: remove possible /v1/chat/completions suffix
        # AIClient adds this path automatically to avoid duplicates
        api_url = api_url.rstrip('/')
        if api_url.endswith('/v1/chat/completions'):
            api_url = api_url[:-len('/v1/chat/completions')]
            logger.debug(f"Normalized API URL for {agent['agent_id']}: removed /v1/chat/completions suffix")

        # Get API key
        api_key = self.settings.get_api_key(api_key_env)

        # Check cache (uses normalized URL)
        cache_key = (api_url, api_key)
        if cache_key in self._client_cache:
            return self._client_cache[cache_key]

        # Create a new client and cache it
        client = AIClient(api_url=api_url, api_key=api_key)
        self._client_cache[cache_key] = client

        logger.info(f"Created new AIClient for {agent['agent_id']}: {api_url}")

        return client

    def get_enabled_agents(self) -> List[Dict[str, Any]]:
        """
        Get enabled AI agents
        
        Returns:
            List of AIs [{'agent_id': str, 'name': str, 'model': str, ...}, ...]
        """
        query = """
            SELECT 
                agent_id,
                name,
                model,
                api_url,
                api_key_env,
                strategy,
                enabled
            FROM ai_agents
            WHERE enabled = TRUE
            ORDER BY 
                CASE agent_id
                    WHEN 'claude' THEN 1
                    WHEN 'gpt' THEN 2
                    WHEN 'gemini' THEN 3
                    ELSE 4
                END
        """
        
        try:
            results = self.db.execute_query(query)
            logger.info(f"Found {len(results)} enabled AI agents")
            return results or []
        
        except Exception as e:
            logger.error(f"Failed to get enabled agents: {e}")
            return []
    
    def call_all_agents(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        delay_between_calls: int = 1
    ) -> Dict[str, Any]:
        """
        Call all AIs sequentially (Claude → GPT → Gemini)
        
        Args:
            messages: message list [{"role": "system", "content": "..."}, ...]
            temperature: temperature parameter
            delay_between_calls: delay between AI calls (seconds)
            
        Returns:
            {
                'claude': {'success': bool, 'response': str, 'error': str},
                'gpt': {...},
                'gemini': {...}
            }
        """
        agents = self.get_enabled_agents()
        
        if not agents:
            logger.error("No enabled AI agents found")
            return {}
        
        results = {}
        
        for agent in agents:
            agent_id = agent['agent_id']
            model = agent['model']

            logger.info(f"Calling AI: {agent_id} ({model})")

            try:
                # Get the dedicated client for this agent
                client = self._get_client_for_agent(agent)

                # Call the AI
                response = client.call(
                    model=model,
                    messages=messages,
                    temperature=temperature
                )

                # Extract content
                content = client.extract_content(response)
                
                results[agent_id] = {
                    'success': True,
                    'response': content,
                    'usage': response.get('usage', {}),
                    'error': None
                }
                
                logger.info(
                    f"AI call succeeded: {agent_id}",
                    extra={'details': {'response_length': len(content)}}
                )
            
            except Exception as e:
                logger.error(
                    f"AI call failed: {agent_id}",
                    extra={'details': {'error': str(e)}}
                )
                
                results[agent_id] = {
                    'success': False,
                    'response': None,
                    'error': str(e)
                }
            
            # Delay (avoid rate limiting)
            if agent != agents[-1]:  # The last one doesn't need a delay
                time.sleep(delay_between_calls)
        
        # Stats
        success_count = sum(1 for r in results.values() if r['success'])
        logger.info(
            f"AI orchestration completed: {success_count}/{len(agents)} succeeded",
            extra={'details': {'results': {k: v['success'] for k, v in results.items()}}}
        )
        
        return results
    
    def call_single_agent(
        self,
        agent_id: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        timeout_multiplier: float = 1.0
    ) -> Optional[Dict[str, Any]]:
        """
        Call a single AI (uses the agent-specific API key)

        Args:
            agent_id: AI ID
            messages: message list
            temperature: temperature parameter
            timeout_multiplier: multiplier for timeout (e.g., 2.0 for batch analysis)

        Returns:
            {'success': bool, 'response': str, 'error': str}
        """
        # Query AI config (includes api_url and api_key_env)
        query = """
            SELECT agent_id, name, model, api_url, api_key_env, enabled
            FROM ai_agents
            WHERE agent_id = %s
        """

        try:
            results = self.db.execute_query(query, (agent_id,))

            if not results:
                logger.error(f"AI agent not found: {agent_id}")
                return None

            agent = results[0]

            if not agent['enabled']:
                logger.error(f"AI agent is disabled: {agent_id}")
                return None

            model = agent['model']

            logger.info(f"Calling AI: {agent_id} ({model})")

            # Get the dedicated client for this agent
            client = self._get_client_for_agent(agent)

            # Calculate timeout (apply multiplier for batch analysis)
            timeout = int(client.timeout * timeout_multiplier) if timeout_multiplier != 1.0 else None

            # Call the AI
            response = client.call(
                model=model,
                messages=messages,
                temperature=temperature,
                timeout=timeout
            )

            # Extract content
            content = client.extract_content(response)

            result = {
                'success': True,
                'response': content,
                'usage': response.get('usage', {}),
                'error': None
            }

            logger.info(
                f"AI call succeeded: {agent_id}",
                extra={'details': {'response_length': len(content)}}
            )

            return result

        except Exception as e:
            logger.error(
                f"AI call failed: {agent_id}",
                extra={'details': {'error': str(e)}}
            )

            return {
                'success': False,
                'response': None,
                'error': str(e)
            }
    
    def parse_json_response(self, response: str) -> Optional[Dict[str, Any]]:
        """
        Parse JSON in an AI response (removing Markdown code fences)
        Uses json_repair as fallback for malformed JSON.

        Args:
            response: AI response text

        Returns:
            Parsed JSON object, or None on failure
        """
        import json
        import re

        # Strip Markdown code fences
        response = response.strip()

        # Try removing ```json ... ```
        match = re.search(r'```json\s*\n(.*?)\n```', response, re.DOTALL)
        if match:
            response = match.group(1)

        # Try removing ```...```
        match = re.search(r'```\s*\n(.*?)\n```', response, re.DOTALL)
        if match:
            response = match.group(1)

        try:
            return json.loads(response)

        except json.JSONDecodeError as e:
            # Fallback 1: try to extract first JSON object substring
            fallback_match = re.search(r'\{.*\}', response, re.DOTALL)
            if fallback_match:
                try:
                    return json.loads(fallback_match.group(0))
                except json.JSONDecodeError:
                    pass

            # Fallback 2: try json_repair
            try:
                from json_repair import repair_json
                logger.info("Attempting JSON repair...")
                repaired = repair_json(response, return_objects=True)
                if isinstance(repaired, dict):
                    logger.info("JSON repair succeeded")
                    return repaired
                elif isinstance(repaired, list) and len(repaired) > 0:
                    # If repaired to a list, try to get first dict
                    if isinstance(repaired[0], dict):
                        logger.info("JSON repair succeeded (extracted from list)")
                        return repaired[0]
            except Exception as repair_error:
                logger.warning(f"JSON repair failed: {repair_error}")

            logger.error(f"Failed to parse JSON response: {e}")
            logger.debug(f"Response content: {response[:500]}")
            return None
    
    def aggregate_responses(
        self,
        results: Dict[str, Any],
        aggregation_method: str = 'majority'
    ) -> Optional[str]:
        """
        Aggregate responses from multiple AIs (optional feature)
        
        Args:
            results: AI call results
            aggregation_method: aggregation method (majority/consensus/weighted)
            
        Returns:
            Aggregated response
        """
        successful_responses = [
            r['response'] for r in results.values()
            if r['success'] and r['response']
        ]
        
        if not successful_responses:
            return None
        
        if aggregation_method == 'majority':
            # Simple majority (placeholder, not fully implemented)
            return successful_responses[0]
        
        elif aggregation_method == 'consensus':
            # Consensus check (placeholder, not fully implemented)
            return successful_responses[0]
        
        elif aggregation_method == 'weighted':
            # Weighted average (placeholder, not fully implemented)
            return successful_responses[0]
        
        else:
            return successful_responses[0]
    
    def get_agent_statistics(self, agent_id: str) -> Dict[str, Any]:
        """
        Get AI statistics
        
        Args:
            agent_id: AI ID
            
        Returns:
            Statistics dictionary
        """
        # Query trade statistics
        query = """
            SELECT 
                COUNT(*) as total_trades,
                COUNT(CASE WHEN action = 'BUY' THEN 1 END) as buy_count,
                COUNT(CASE WHEN action = 'SELL' THEN 1 END) as sell_count,
                SUM(CASE WHEN action = 'BUY' THEN total_amount ELSE 0 END) as total_bought,
                SUM(CASE WHEN action = 'SELL' THEN total_amount ELSE 0 END) as total_sold
            FROM transactions
            WHERE agent_id = %s
        """
        
        try:
            results = self.db.execute_query(query, (agent_id,))
            
            if not results:
                return {}
            
            stats = results[0]
            
            # Query current assets
            wallet_query = """
                SELECT cash_balance FROM wallets WHERE agent_id = %s
            """
            wallet_results = self.db.execute_query(wallet_query, (agent_id,))
            
            if wallet_results:
                stats['cash_balance'] = float(wallet_results[0]['cash_balance'])
            
            return dict(stats)
        
        except Exception as e:
            logger.error(f"Failed to get agent statistics: {e}")
            return {}
