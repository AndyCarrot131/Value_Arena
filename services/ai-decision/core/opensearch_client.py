"""
OpenSearch Client
Write decision vectors to OpenSearch Provisioned and support updating quality_weight
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
    """OpenSearch Provisioned client"""

    def __init__(
        self,
        collection_endpoint: str,
        index_name: str,
        region: str = 'us-east-1',
        service: str = 'es'
    ):
        """
        Initialize the OpenSearch client

        Args:
            collection_endpoint: domain endpoint (https://vpc-xxx.us-east-1.es.amazonaws.com)
            index_name: index name (ai-investment-decisions)
            region: AWS region
            service: AWS service name ('es' for Provisioned, 'aoss' for Serverless)
        """
        self.collection_endpoint = collection_endpoint.replace('https://', '')
        self.index_name = index_name
        self.region = region

        # AWS SigV4 auth
        credentials = boto3.Session().get_credentials()
        self.awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            service,  # 'es' for Provisioned, 'aoss' for Serverless
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
    
    def knn_search(
        self,
        query_vector: List[float],
        filter_conditions: Optional[Dict[str, Any]] = None,
        num_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        k-NN vector search for RAG retrieval

        Args:
            query_vector: 1024-dim query vector
            filter_conditions: OpenSearch filter (e.g., {'term': {'symbol': 'AAPL'}})
            num_results: number of results to return

        Returns:
            List of similar decisions with scores:
            [
                {
                    'content': str,  # reasoning text
                    'score': float,  # similarity score
                    'metadata': {...}  # all document fields
                },
                ...
            ]
        """
        try:
            logger.debug(
                "Performing k-NN search",
                extra={'details': {
                    'num_results': num_results,
                    'has_filter': filter_conditions is not None
                }}
            )

            # Build k-NN query body
            search_body = {
                "size": num_results,
                "query": {
                    "knn": {
                        "decision_embedding": {
                            "vector": query_vector,
                            "k": num_results
                        }
                    }
                }
            }

            # Add filter if provided
            if filter_conditions:
                search_body["query"]["knn"]["decision_embedding"]["filter"] = filter_conditions

            # Execute search
            response = self.client.search(
                index=self.index_name,
                body=search_body
            )

            # Parse results
            results = []
            for hit in response['hits']['hits']:
                source = hit['_source']
                results.append({
                    'content': source.get('reasoning', ''),
                    'score': hit['_score'],
                    'metadata': {
                        'decision_id': source.get('decision_id'),
                        'agent_id': source.get('agent_id'),
                        'symbol': source.get('symbol'),
                        'decision_type': source.get('decision_type'),
                        'type': source.get('metadata', {}).get('type', ''),
                        'date': source.get('metadata', {}).get('date', ''),
                        'created_at': source.get('created_at')
                    }
                })

            logger.info(
                f"k-NN search returned {len(results)} results",
                extra={'details': {
                    'num_results': len(results),
                    'avg_score': sum(r['score'] for r in results) / len(results) if results else 0
                }}
            )

            return results

        except Exception as e:
            logger.error(
                "k-NN search failed",
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
    region: str = 'us-east-1',
    service: str = 'es'
) -> OpenSearchClient:
    """
    Get global OpenSearchClient singleton

    Args:
        collection_endpoint: domain endpoint
        index_name: index name
        region: AWS region
        service: AWS service name ('es' for Provisioned, 'aoss' for Serverless)

    Returns:
        OpenSearchClient instance
    """
    global _opensearch_client_instance

    if _opensearch_client_instance is None:
        _opensearch_client_instance = OpenSearchClient(
            collection_endpoint=collection_endpoint,
            index_name=index_name,
            region=region,
            service=service
        )

    return _opensearch_client_instance
