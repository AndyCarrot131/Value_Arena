#!/usr/bin/env python3
"""
AI Decision Service - Main Entry Point
Parse command line arguments and route to appropriate workflow
"""

import sys
import argparse

from config import get_settings
from utils import get_et_timestamp_iso
from core import (
    get_secrets_manager,
    get_database_manager,
    get_redis_client,
    get_ai_client,
    get_bedrock_client,
    get_opensearch_client,
    get_logger
)
from services import (
    DataCollector,
    MemoryManager,
    RAGRetriever,
    DecisionValidator,
    PortfolioExecutor,
    AIOrchestrator
)
from workflows import (
    HourlyNewsAnalysisWorkflow,
    DailySummaryWorkflow,
    TradingDecisionWorkflow,
    WeeklySummaryWorkflow,
    StockAnalysisWorkflow
)

# Initialize logger
logger = get_logger()


def initialize_services():
    """
    Initialize all service components
    
    Returns:
        Dictionary of initialized services
    """
    logger.info("Initializing services...")
    
    try:
        # Load configuration
        settings = get_settings()
        
        # Initialize core clients
        secrets_manager = get_secrets_manager(region=settings.region)
        
        db = get_database_manager(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=settings.db_user,
            password=settings.db_password
        )
        
        redis_client = get_redis_client(
            host=settings.redis_host,
            port=settings.redis_port,
            use_ssl=settings.redis_ssl
        )
        
        # NOTE: ai_client 已弃用，AIOrchestrator 现在为每个 agent 动态创建 client
        # 保留此代码用于向后兼容
        ai_client = get_ai_client(
            api_url=settings.baicai_api_url,
            api_key=settings.baicai_api_key
        )

        bedrock_client = get_bedrock_client(
            region=settings.region,
            knowledge_base_id=settings.knowledge_base_id
        )

        opensearch_client = get_opensearch_client(
            collection_endpoint=settings.opensearch_endpoint,
            index_name=settings.index_name,
            region=settings.region
        )

        # Initialize business services
        data_collector = DataCollector(db, redis_client)
        memory_manager = MemoryManager(db)
        rag_retriever = RAGRetriever(bedrock_client)
        decision_validator = DecisionValidator(db)
        portfolio_executor = PortfolioExecutor(db)
        # AIOrchestrator 不再使用全局 ai_client，每个 agent 使用独立的 API Key
        ai_orchestrator = AIOrchestrator(db, ai_client=None)
        
        logger.info("All services initialized successfully")
        
        return {
            'db': db,
            'redis': redis_client,
            'ai_client': ai_client,
            'bedrock': bedrock_client,
            'opensearch': opensearch_client,
            'data_collector': data_collector,
            'memory_manager': memory_manager,
            'rag_retriever': rag_retriever,
            'decision_validator': decision_validator,
            'portfolio_executor': portfolio_executor,
            'ai_orchestrator': ai_orchestrator
        }
    
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise


def run_hourly_news_analysis(services: dict, agent_id: str, test_mode: bool = False) -> bool:
    """
    Run hourly news analysis workflow

    Args:
        services: Initialized services
        agent_id: AI agent ID
        test_mode: If True, run without database writes

    Returns:
        True if successful
    """
    logger.info(f"Starting hourly news analysis for {agent_id} (test_mode={test_mode})")

    workflow = HourlyNewsAnalysisWorkflow(
        data_collector=services['data_collector'],
        memory_manager=services['memory_manager'],
        ai_orchestrator=services['ai_orchestrator'],
        db=services['db'],
        test_mode=test_mode
    )

    return workflow.run(agent_id)


def run_daily_summary(services: dict, agent_id: str, test_mode: bool = False) -> bool:
    """
    Run daily summary workflow

    Args:
        services: Initialized services
        agent_id: AI agent ID
        test_mode: If True, run without database/RAG writes

    Returns:
        True if successful
    """
    logger.info(f"Starting daily summary for {agent_id} (test_mode={test_mode})")

    workflow = DailySummaryWorkflow(
        data_collector=services['data_collector'],
        memory_manager=services['memory_manager'],
        rag_retriever=services['rag_retriever'],
        ai_orchestrator=services['ai_orchestrator'],
        bedrock_client=services['bedrock'],
        opensearch_client=services['opensearch'],
        db=services['db'],
        test_mode=test_mode
    )

    return workflow.run(agent_id)


def run_trading_decision(services: dict, agent_id: str, test_mode: bool = False) -> bool:
    """
    Run trading decision workflow

    Args:
        services: Initialized services
        agent_id: AI agent ID
        test_mode: If True, run without database writes and verbose logging

    Returns:
        True if successful
    """
    logger.info(f"Starting trading decision for {agent_id} (test_mode={test_mode})")

    workflow = TradingDecisionWorkflow(
        data_collector=services['data_collector'],
        memory_manager=services['memory_manager'],
        rag_retriever=services['rag_retriever'],
        decision_validator=services['decision_validator'],
        portfolio_executor=services['portfolio_executor'],
        ai_orchestrator=services['ai_orchestrator'],
        bedrock_client=services['bedrock'],
        opensearch_client=services['opensearch'],
        db=services['db'],
        test_mode=test_mode
    )

    return workflow.run(agent_id)


def run_weekly_summary(services: dict, agent_id: str, test_mode: bool = False) -> bool:
    """
    Run weekly stock summary workflow

    Args:
        services: Initialized services
        agent_id: AI agent ID
        test_mode: If True, run without database/RAG writes

    Returns:
        True if successful
    """
    logger.info(f"Starting weekly summary for {agent_id} (test_mode={test_mode})")

    workflow = WeeklySummaryWorkflow(
        data_collector=services['data_collector'],
        rag_retriever=services['rag_retriever'],
        ai_orchestrator=services['ai_orchestrator'],
        bedrock_client=services['bedrock'],
        opensearch_client=services['opensearch'],
        db=services['db'],
        test_mode=test_mode
    )

    return workflow.run(agent_id)


def run_stock_analysis(services: dict, agent_id: str, test_mode: bool = False, symbols: list = None) -> bool:
    """
    Run stock analysis workflow

    Args:
        services: Initialized services
        agent_id: AI agent ID
        test_mode: If True, run without database/RAG writes
        symbols: Optional list of symbols to analyze

    Returns:
        True if successful
    """
    logger.info(f"Starting stock analysis for {agent_id} (test_mode={test_mode}, symbols={symbols or 'ALL'})")

    workflow = StockAnalysisWorkflow(
        data_collector=services['data_collector'],
        rag_retriever=services['rag_retriever'],
        ai_orchestrator=services['ai_orchestrator'],
        bedrock_client=services['bedrock'],
        opensearch_client=services['opensearch'],
        test_mode=test_mode
    )

    results = workflow.run(agent_id, symbols=symbols)
    return len(results) > 0


def get_enabled_agents(services: dict) -> list:
    """
    Get list of enabled AI agents
    
    Args:
        services: Initialized services
        
    Returns:
        List of agent IDs
    """
    query = "SELECT agent_id FROM ai_agents WHERE enabled = TRUE ORDER BY agent_id"
    results = services['db'].execute_query(query)
    
    return [r['agent_id'] for r in results] if results else []


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='AI Decision Service - Execute investment workflows'
    )
    
    parser.add_argument(
        '--workflow',
        required=True,
        choices=[
            'hourly_news_analysis',
            'daily_summary',
            'trading_decision',
            'weekly_summary',
            'stock_analysis'
        ],
        help='Workflow to execute'
    )
    
    parser.add_argument(
        '--agent_id',
        required=True,
        help='AI agent ID (claude/gpt/gemini/all)'
    )
    
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level'
    )
    
    parser.add_argument(
        '--log-format',
        default='json',
        choices=['json', 'text'],
        help='Log output format'
    )

    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='Run in test mode (no database writes)'
    )

    parser.add_argument(
        '--symbols',
        type=str,
        default=None,
        help='Comma-separated list of symbols for stock_analysis workflow (e.g., AAPL,GOOGL,MSFT)'
    )

    args = parser.parse_args()
    
    # Configure logger
    global logger
    logger = get_logger(
        level=args.log_level,
        log_format=args.log_format
    )
    
    logger.info(
        f"Starting AI Decision Service",
        extra={'details': {
            'workflow': args.workflow,
            'agent_id': args.agent_id,
            'timestamp': get_et_timestamp_iso()
        }}
    )
    
    try:
        # Initialize services
        services = initialize_services()
        
        # Determine which agents to run
        if args.agent_id == 'all':
            agent_ids = get_enabled_agents(services)
            logger.info(f"Running workflow for all agents: {agent_ids}")
        else:
            agent_ids = [args.agent_id]
        
        if not agent_ids:
            logger.error("No agents found or specified")
            return 1
        
        # Route to appropriate workflow
        workflow_map = {
            'hourly_news_analysis': run_hourly_news_analysis,
            'daily_summary': run_daily_summary,
            'trading_decision': run_trading_decision,
            'weekly_summary': run_weekly_summary,
            'stock_analysis': run_stock_analysis
        }

        workflow_func = workflow_map[args.workflow]

        # Parse symbols for stock_analysis workflow
        symbols = None
        if args.symbols:
            symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]

        # Execute workflow for each agent
        success_count = 0
        for agent_id in agent_ids:
            logger.info(f"Executing {args.workflow} for {agent_id}")

            try:
                # Pass test_mode for workflows that support it
                if args.workflow == 'stock_analysis':
                    success = workflow_func(services, agent_id, test_mode=args.test_mode, symbols=symbols)
                elif args.workflow in ['hourly_news_analysis', 'daily_summary', 'trading_decision', 'weekly_summary']:
                    success = workflow_func(services, agent_id, test_mode=args.test_mode)
                else:
                    success = workflow_func(services, agent_id)

                if success:
                    success_count += 1
                    logger.info(f"Workflow completed successfully for {agent_id}")
                else:
                    logger.error(f"Workflow failed for {agent_id}")

            except Exception as e:
                logger.error(
                    f"Workflow execution failed for {agent_id}: {e}",
                    exc_info=True
                )
        
        # Summary
        logger.info(
            f"Workflow execution summary",
            extra={'details': {
                'total_agents': len(agent_ids),
                'successful': success_count,
                'failed': len(agent_ids) - success_count
            }}
        )
        
        # Return exit code
        if success_count == len(agent_ids):
            logger.info("All workflows completed successfully")
            return 0
        elif success_count > 0:
            logger.warning("Some workflows failed")
            return 1
        else:
            logger.error("All workflows failed")
            return 2
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 3
    
    finally:
        logger.info("AI Decision Service shutting down")


if __name__ == '__main__':
    sys.exit(main())
