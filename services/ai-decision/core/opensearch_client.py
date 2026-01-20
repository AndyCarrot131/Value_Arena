"""
OpenSearch Client
Write decision vectors to OpenSearch Serverless and support updating quality_weight
"""

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from .logger import get_logger

# US Eastern Time (ET) - Fixed offset UTC-04:00
ET_OFFSET = timezone(timedelta(hours=-4))

logger = get_logger()


class OpenSearchClient:
    """OpenSearch Serverless client"""
    
    def __init__(
        self,
        collection_endpoint: str,
        index_name: str,
        region: str = 'us-east-1'
    ):
        """
        Initialize the OpenSearch client
        
        Args:
            collection_endpoint: collection endpoint (https://xxx.us-east-1.aoss.amazonaws.com)
            index_name: index name (ai-investment-decisions)
            region: AWS region
        """
        self.collection_endpoint = collection_endpoint.replace('https://', '')
        self.index_name = index_name
        self.region = region
        
        # AWS SigV4 auth (for AOSS)
        credentials = boto3.Session().get_credentials()
        self.awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            'aoss',  # OpenSearch Serverless service name
            session_token=credentials.token
        )
        
        # Create OpenSearch client
        self.client = OpenSearch(
            hosts=[{'host': self.collection_endpoint, 'port': 443}],
            http_auth=self.awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )
    
    def index_decision(
        self,
        decision_id: str,
        agent_id: str,
        decision_embedding: List[float],
        reasoning: str,
        decision_type: str,
        symbol: str,
        quality_weight: float = 0.5,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Index a decision into OpenSearch
        
        Args:
            decision_id: decision ID (UUID)
            agent_id: AI ID
            decision_embedding: 1024-dim vector
            reasoning: decision reasoning text
            decision_type: decision type (BUY/SELL/HOLD)
            symbol: stock symbol
            quality_weight: decision quality weight (0-1, evaluated after 30 days)
            metadata: metadata (market environment, etc.)
            
        Returns:
            OpenSearch response
            
        Raises:
            RuntimeError: indexing failed
        """
        try:
            doc = {
                'decision_id': decision_id,
                'agent_id': agent_id,
                'decision_embedding': decision_embedding,
                'reasoning': reasoning,
                'decision_type': decision_type,
                'symbol': symbol,
                'quality_weight': quality_weight,
                'metadata': metadata or {},
                'created_at': datetime.now(ET_OFFSET).isoformat()
            }
            
            logger.debug(
                "Indexing decision to OpenSearch",
                extra={'details': {
                    'decision_id': decision_id,
                    'agent_id': agent_id,
                    'symbol': symbol,
                    'decision_type': decision_type
                }}
            )
            
            response = self.client.index(
                index=self.index_name,
                body=doc
            )
            
            logger.info(
                "Decision indexed successfully",
                extra={'details': {
                    'decision_id': decision_id,
                    'opensearch_id': response['_id']
                }}
            )
            
            return response
        
        except Exception as e:
            logger.error(
                "Failed to index decision",
                extra={'details': {'decision_id': decision_id, 'error': str(e)}}
            )
            raise RuntimeError(f"Failed to index decision: {e}")
    
    def update_quality_weight(
        self,
        decision_id: str,
        quality_weight: float
    ) -> Dict[str, Any]:
        """
        Update decision quality weight (evaluated after 30 days)
        
        Args:
            decision_id: decision ID
            quality_weight: new quality weight (0-1)
            
        Returns:
            OpenSearch response
            
        Raises:
            RuntimeError: update failed
        """
        try:
            logger.debug(
                "Updating quality weight",
                extra={'details': {'decision_id': decision_id, 'quality_weight': quality_weight}}
            )
            
            # Query for the document
            search_response = self.client.search(
                index=self.index_name,
                body={
                    'query': {
                        'term': {'decision_id': decision_id}
                    }
                }
            )
            
            hits = search_response['hits']['hits']
            if not hits:
                raise ValueError(f"Decision not found: {decision_id}")
            
            # Get document ID
            doc_id = hits[0]['_id']
            
            # Update document
            response = self.client.update(
                index=self.index_name,
                id=doc_id,
                body={
                    'doc': {
                        'quality_weight': quality_weight,
                        'evaluated_at': datetime.now(ET_OFFSET).isoformat()
                    }
                },
                refresh=True
            )
            
            logger.info(
                "Quality weight updated successfully",
                extra={'details': {'decision_id': decision_id, 'quality_weight': quality_weight}}
            )
            
            return response
        
        except Exception as e:
            logger.error(
                "Failed to update quality weight",
                extra={'details': {'decision_id': decision_id, 'error': str(e)}}
            )
            raise RuntimeError(f"Failed to update quality weight: {e}")
    
    def search_decisions(
        self,
        agent_id: Optional[str] = None,
        symbol: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search decisions (for testing or debugging)
        
        Args:
            agent_id: AI ID (optional)
            symbol: stock symbol (optional)
            limit: number of results to return
            
        Returns:
            List of decisions
        """
        try:
            # Build query
            query = {'bool': {'must': []}}
            
            if agent_id:
                query['bool']['must'].append({'term': {'agent_id': agent_id}})
            
            if symbol:
                query['bool']['must'].append({'term': {'symbol': symbol}})
            
            if not query['bool']['must']:
                query = {'match_all': {}}
            
            response = self.client.search(
                index=self.index_name,
                body={
                    'query': query,
                    'size': limit,
                    'sort': [{'created_at': {'order': 'desc'}}]
                }
            )
            
            results = []
            for hit in response['hits']['hits']:
                results.append(hit['_source'])
            
            return results
        
        except Exception as e:
            logger.error(
                "Failed to search decisions",
                extra={'details': {'error': str(e)}}
            )
            return []
    
    def ping(self) -> bool:
        """
        Test connection
        
        Returns:
            True if connection is healthy
        """
        try:
            return self.client.ping()
        except Exception:
            return False


# Global singleton (optional)
_opensearch_client_instance: Optional[OpenSearchClient] = None


def get_opensearch_client(
    collection_endpoint: str,
    index_name: str,
    region: str = 'us-east-1'
) -> OpenSearchClient:
    """
    Get global OpenSearchClient singleton
    
    Args:
        collection_endpoint: collection endpoint
        index_name: index name
        region: AWS region
        
    Returns:
        OpenSearchClient instance
    """
    global _opensearch_client_instance
    
    if _opensearch_client_instance is None:
        _opensearch_client_instance = OpenSearchClient(
            collection_endpoint=collection_endpoint,
            index_name=index_name,
            region=region
        )
    
    return _opensearch_client_instance
