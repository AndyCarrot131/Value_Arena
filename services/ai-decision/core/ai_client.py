"""
AI Client (BaiCai API)
Unified BaiCai API client with retries and error handling
"""

import requests
import time
from typing import List, Dict, Any, Optional
from .logger import get_logger

logger = get_logger()


class AIClient:
    """BaiCai API client"""
    
    def __init__(
        self,
        api_url: str,
        api_key: str,
        timeout: int = 200,
        max_retries: int = 3,
        retry_delay: int = 1
    ):
        """
        Initialize the AI client
        
        Args:
            api_url: base API URL (https://www.baicai.chat)
            api_key: API Key
            timeout: request timeout (seconds)
            max_retries: max retries
            retry_delay: retry delay (seconds)
        """
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Request headers
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    def call(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Call the AI API (with retry logic)

        Args:
            model: model name (e.g., claude-sonnet-4-5)
            messages: messages [{"role": "system", "content": "..."}, ...]
            temperature: temperature (0-1)
            max_tokens: max token count
            timeout: request timeout override (seconds), uses self.timeout if None

        Returns:
            API response (includes choices, usage, etc.)

        Raises:
            RuntimeError: API call failed
        """
        endpoint = f"{self.api_url}/v1/chat/completions"
        request_timeout = timeout if timeout is not None else self.timeout
        
        payload = {
            'model': model,
            'messages': messages,
            'temperature': temperature
        }
        
        if max_tokens:
            payload['max_tokens'] = max_tokens
        
        # Retry logic
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(
                    f"AI API call attempt {attempt + 1}/{self.max_retries}",
                    extra={'details': {'model': model, 'endpoint': endpoint}}
                )
                
                response = requests.post(
                    endpoint,
                    headers=self.headers,
                    json=payload,
                    timeout=request_timeout
                )
                
                # Check HTTP status
                if response.status_code == 200:
                    result = response.json()
                    
                    logger.debug(
                        "AI API call succeeded",
                        extra={'details': {
                            'model': model,
                            'usage': result.get('usage', {})
                        }}
                    )
                    
                    return result
                
                elif response.status_code == 429:
                    # Rate limit, retry
                    logger.warning(
                        f"Rate limit hit, retrying in {self.retry_delay * (2 ** attempt)}s",
                        extra={'details': {'model': model, 'status_code': 429}}
                    )
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                        continue
                    else:
                        raise RuntimeError(f"Rate limit exceeded after {self.max_retries} retries")
                
                elif response.status_code >= 500:
                    # Server error, retry
                    logger.warning(
                        f"Server error {response.status_code}, retrying",
                        extra={'details': {'model': model, 'status_code': response.status_code}}
                    )
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))
                        continue
                    else:
                        raise RuntimeError(f"Server error after {self.max_retries} retries: {response.status_code}")
                
                else:
                    # Client error, do not retry
                    error_msg = response.text
                    raise RuntimeError(f"API call failed with status {response.status_code}: {error_msg}")
            
            except requests.exceptions.Timeout as e:
                last_error = e
                logger.warning(
                    f"Request timeout, retrying",
                    extra={'details': {'model': model, 'attempt': attempt + 1}}
                )
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                else:
                    raise RuntimeError(f"Request timeout after {self.max_retries} retries: {e}")
            
            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(
                    f"Request failed, retrying",
                    extra={'details': {'model': model, 'error': str(e)}}
                )
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                else:
                    raise RuntimeError(f"Request failed after {self.max_retries} retries: {e}")
        
        # All retries failed
        raise RuntimeError(f"AI API call failed after {self.max_retries} retries: {last_error}")
    
    def extract_content(self, response: Dict[str, Any]) -> str:
        """
        Extract content from API response
        
        Args:
            response: API response
            
        Returns:
            AI-generated text content
            
        Raises:
            ValueError: invalid response format
        """
        try:
            choices = response.get('choices', [])
            if not choices:
                raise ValueError("No choices in response")
            
            message = choices[0].get('message', {})
            content = message.get('content', '')
            
            if not content:
                raise ValueError("Empty content in response")
            
            return content
        
        except (KeyError, IndexError) as e:
            raise ValueError(f"Invalid response format: {e}")


# Global singleton (optional)
_ai_client_instance: Optional[AIClient] = None


def get_ai_client(api_url: str, api_key: str) -> AIClient:
    """
    Get the global AIClient singleton
    
    Args:
        api_url: base API URL
        api_key: API Key
        
    Returns:
        AIClient instance
    """
    global _ai_client_instance
    
    if _ai_client_instance is None:
        _ai_client_instance = AIClient(api_url=api_url, api_key=api_key)
    
    return _ai_client_instance
