"""
Bedrock Client
Call AWS Bedrock services: Titan V2 Embedding + Knowledge Base Retrieve
"""

import boto3
import json
from typing import List, Dict, Any, Optional
from .logger import get_logger

logger = get_logger()


class BedrockClient:
    """AWS Bedrock client"""
    
    def __init__(self, region: str = 'us-east-1', knowledge_base_id: Optional[str] = None):
        """
        Initialize the Bedrock client
        
        Args:
            region: AWS region
            knowledge_base_id: Knowledge Base ID (for RAG retrieval)
        """
        self.region = region
        self.knowledge_base_id = knowledge_base_id
        
        # Bedrock Runtime (for generating embeddings)
        self.runtime_client = boto3.client('bedrock-runtime', region_name=region)
        
        # Bedrock Agent Runtime (for KB retrieval)
        self.agent_client = boto3.client('bedrock-agent-runtime', region_name=region)
    
    def generate_embedding(self, text: str, dimensions: int = 1024) -> List[float]:
        """
        Generate text embeddings using Titan V2
        
        Args:
            text: input text
            dimensions: vector dimensions (1024)
            
        Returns:
            1024-dimensional vector
            
        Raises:
            RuntimeError: embedding generation failed
        """
        try:
            logger.debug(
                "Generating embedding",
                extra={'details': {'text_length': len(text), 'dimensions': dimensions}}
            )
            
            body = json.dumps({
                'inputText': text,
                'dimensions': dimensions,
                'normalize': True
            })
            
            response = self.runtime_client.invoke_model(
                modelId='amazon.titan-embed-text-v2:0',
                body=body
            )
            
            response_body = json.loads(response['body'].read())
            embedding = response_body['embedding']
            
            logger.debug(
                "Embedding generated successfully",
                extra={'details': {'vector_length': len(embedding)}}
            )
            
            return embedding
        
        except Exception as e:
            logger.error(
                "Failed to generate embedding",
                extra={'details': {'error': str(e)}}
            )
            raise RuntimeError(f"Failed to generate embedding: {e}")
    
    def retrieve_similar_cases(
        self,
        query_text: str,
        num_results: int = 10,
        filter_criteria: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve similar cases from the Knowledge Base
        
        Args:
            query_text: query text
            num_results: number of results
            filter_criteria: filter criteria (optional)
            
        Returns:
            List of similar cases [{'content': str, 'score': float, 'metadata': dict}, ...]
            
        Raises:
            RuntimeError: retrieval failed
            ValueError: Knowledge Base ID not configured
        """
        if not self.knowledge_base_id:
            raise ValueError("Knowledge Base ID not configured")
        
        try:
            logger.debug(
                "Retrieving from Knowledge Base",
                extra={'details': {
                    'query_length': len(query_text),
                    'num_results': num_results,
                    'kb_id': self.knowledge_base_id
                }}
            )
            
            # Build retrieval configuration
            retrieval_config = {
                'vectorSearchConfiguration': {
                    'numberOfResults': num_results
                }
            }
            
            # Add filter criteria (optional)
            if filter_criteria:
                retrieval_config['vectorSearchConfiguration']['filter'] = filter_criteria
            
            # Call Knowledge Base API
            response = self.agent_client.retrieve(
                knowledgeBaseId=self.knowledge_base_id,
                retrievalQuery={'text': query_text},
                retrievalConfiguration=retrieval_config
            )
            
            # Parse results
            results = []
            for item in response.get('retrievalResults', []):
                result = {
                    'content': item.get('content', {}).get('text', ''),
                    'score': item.get('score', 0.0),
                    'metadata': item.get('metadata', {})
                }
                results.append(result)
            
            logger.info(
                f"Retrieved {len(results)} similar cases",
                extra={'details': {
                    'num_results': len(results),
                    'avg_score': sum(r['score'] for r in results) / len(results) if results else 0
                }}
            )
            
            return results
        
        except self.agent_client.exceptions.ResourceNotFoundException:
            logger.error(
                "Knowledge Base not found",
                extra={'details': {'kb_id': self.knowledge_base_id}}
            )
            raise RuntimeError(f"Knowledge Base not found: {self.knowledge_base_id}")
        
        except Exception as e:
            logger.error(
                "Failed to retrieve from Knowledge Base",
                extra={'details': {'error': str(e)}}
            )
            raise RuntimeError(f"Failed to retrieve from Knowledge Base: {e}")
    
    def retrieve_with_filter(
        self,
        query_text: str,
        agent_id: Optional[str] = None,
        symbol: Optional[str] = None,
        num_results: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Retrieve with filters (helper)
        
        Args:
            query_text: query text
            agent_id: AI ID (optional; limit to specific AI decisions)
            symbol: stock symbol (optional; limit to specific symbol)
            num_results: number of results
            
        Returns:
            Similar cases list
        """
        filter_criteria = {}

        if agent_id:
            # Metadata fields are stored under metadata.*
            filter_criteria['equals'] = {'key': 'metadata.agent_id', 'value': agent_id}

        if symbol:
            if 'equals' in filter_criteria:
                # If filter already exists, use andAll
                filter_criteria = {
                    'andAll': [
                        filter_criteria,
                        {'equals': {'key': 'metadata.symbol', 'value': symbol}}
                    ]
                }
            else:
                filter_criteria['equals'] = {'key': 'metadata.symbol', 'value': symbol}

        if filter_criteria:
            return self.retrieve_similar_cases(query_text, num_results, filter_criteria)
        else:
            return self.retrieve_similar_cases(query_text, num_results)


# Global singleton (optional)
_bedrock_client_instance: Optional[BedrockClient] = None


def get_bedrock_client(region: str = 'us-east-1', knowledge_base_id: Optional[str] = None) -> BedrockClient:
    """
    Get the global BedrockClient singleton
    
    Args:
        region: AWS region
        knowledge_base_id: Knowledge Base ID
        
    Returns:
        BedrockClient instance
    """
    global _bedrock_client_instance
    
    if _bedrock_client_instance is None:
        _bedrock_client_instance = BedrockClient(region=region, knowledge_base_id=knowledge_base_id)
    
    return _bedrock_client_instance
